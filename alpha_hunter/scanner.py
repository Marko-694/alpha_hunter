import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

from .binance_client import BinanceClient
from .binance_futures_client import BinanceFuturesClient
from .nansen_client import NansenClient
from .telegram_alerts import TelegramNotifier
from .logger import get_logger


class AlphaScanner:
    def __init__(
        self,
        binance_client: BinanceClient,
        notifier: TelegramNotifier,
        config: Dict[str, Any],
        logger: logging.Logger | None = None,
        futures_client: Optional[BinanceFuturesClient] = None,
        nansen_client: Optional[NansenClient] = None,
    ) -> None:
        self.binance_client = binance_client
        self.notifier = notifier
        self.config = config
        self.logger = logger or get_logger(self.__class__.__name__)
        self.futures_client = futures_client
        self.nansen_client = nansen_client

        scanner_cfg = config.get("scanner", {})
        signals_cfg = config.get("signals", {})

        self.interval_seconds = int(scanner_cfg.get("interval_seconds", 60))
        self.min_24h_volume_usdt = float(scanner_cfg.get("min_24h_volume_usdt", 1_000_000))
        self.max_24h_volume_usdt = float(scanner_cfg.get("max_24h_volume_usdt", 50_000_000))
        self.min_price_usdt = float(scanner_cfg.get("min_price_usdt", 0.01))
        self.max_price_usdt = float(scanner_cfg.get("max_price_usdt", 50))
        self.max_symbols_per_scan = int(scanner_cfg.get("max_symbols_per_scan", 80))
        self.cooldown_seconds = int(scanner_cfg.get("cooldown_seconds", 600))

        self.volume_multiplier_threshold = float(signals_cfg.get("volume_multiplier_threshold", 4.0))
        self.min_volume_1m_usdt = float(signals_cfg.get("min_volume_1m_usdt", 50_000))
        self.price_delta_percent_threshold = float(signals_cfg.get("price_delta_percent_threshold", 2.0))

        self.last_alert_times: Dict[str, float] = {}
        self._mm_clusters: Optional[Dict[str, Dict[str, Any]]] = None
        self._mm_clusters_loaded = False

    def _is_valid_symbol(self, symbol: str) -> bool:
        if not symbol.endswith("USDT"):
            return False
        bad_suffixes = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
        if symbol.endswith(bad_suffixes):
            return False
        return True

    def _load_clusters(self) -> None:
        if self._mm_clusters_loaded:
            return
        self._mm_clusters_loaded = True
        path = Path("data/alpha_profiler/clusters.json")
        if not path.exists():
            self.logger.warning("clusters.json not found at %s", path)
            self._mm_clusters = None
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                clusters: Dict[str, Dict[str, Any]] = {}
                for entry in data:
                    addr = str(entry.get("address") or "").lower()
                    if addr:
                        clusters[addr] = entry
                self._mm_clusters = clusters
            else:
                self.logger.warning("clusters.json has unexpected format")
                self._mm_clusters = None
        except Exception as exc:
            self.logger.warning("Failed to load clusters.json: %s", exc)
            self._mm_clusters = None

    def _build_mm_context_block(
        self,
        symbol: str,
        chain: Optional[str],
        contract: Optional[str],
        nansen_summary: Optional[Dict[str, Any]],
        volume_ratio: float,
        price_delta_percent: float,
    ) -> str:
        self._load_clusters()

        netflow_usd = None
        wallets_count = None
        wallets: List[str] = []
        if nansen_summary:
            netflow_usd = nansen_summary.get("netflow_usd")
            wallets_count = nansen_summary.get("wallets_count")
            wallets = [w.lower() for w in nansen_summary.get("wallets") or [] if isinstance(w, str)]

        cluster_matches_count = 0
        top_matches: List[Dict[str, Any]] = []
        if self._mm_clusters and wallets:
            for addr in wallets:
                if addr in self._mm_clusters:
                    cluster_matches_count += 1
                    top_matches.append(self._mm_clusters[addr])
            top_matches.sort(key=lambda x: float(x.get("total_net_usd") or 0.0), reverse=True)
            top_matches = top_matches[:3]

        score = 0.0
        score += max(0.0, min(40.0, (volume_ratio - 1.0) * 10.0))
        score += max(0.0, min(25.0, price_delta_percent * 2.0))
        if netflow_usd is not None:
            if netflow_usd > 0:
                score += max(0.0, min(20.0, netflow_usd / 50_000.0 * 20.0))
            else:
                score += max(-10.0, min(0.0, netflow_usd / 50_000.0 * 10.0))
        if cluster_matches_count > 0:
            score += max(5.0, min(15.0, cluster_matches_count * 5.0))
        score = max(0.0, min(100.0, score))
        score_int = int(round(score))

        lines = ["", "*MM / Smart Money контекст:*"]
        if netflow_usd is not None:
            lines.append(f"• Smart-money netflow (Nansen): `{netflow_usd:,.0f} USD`")
        else:
            lines.append("• Smart-money netflow (Nansen): `нет данных`")

        if wallets_count is not None:
            lines.append(f"• Активных smart-кошельков: `{wallets_count}`")

        if self._mm_clusters is None:
            lines.append("• Кластеры: `нет данных`")
        else:
            if cluster_matches_count > 0:
                lines.append(f"• Совпадений с нашими кластерами: `{cluster_matches_count}`")
                if top_matches:
                    short = ", ".join(
                        f"`{m.get('address')}` (pumps={m.get('pumps_participated')}, usd={m.get('total_net_usd')})"
                        for m in top_matches
                    )
                    lines.append(f"  Топ адреса: {short}")
            else:
                lines.append("• Совпадений с нашими кластерами: `нет`")

        lines.append(f"• Оценка вероятности пампа: `{score_int}%`")
        return "\n".join(lines)

    def scan_once(self) -> None:
        self.logger.info("Fetching 24h tickers from Binance")
        tickers = self.binance_client.get_24h_tickers()
        if not tickers:
            self.logger.warning("No tickers received from Binance")
            return

        total_tickers = len(tickers)
        passed_basic_filters = 0
        klines_requested = 0
        signals_triggered = 0

        scanner_symbols: List[Dict[str, Any]] = []

        for t in tickers:
            symbol = t.get("symbol")
            if not isinstance(symbol, str):
                continue
            if not self._is_valid_symbol(symbol):
                continue

            try:
                last_price = float(t.get("lastPrice", 0.0))
                quote_volume = float(t.get("quoteVolume", 0.0))
            except (TypeError, ValueError):
                continue

            if quote_volume < self.min_24h_volume_usdt or quote_volume > self.max_24h_volume_usdt:
                continue
            if last_price < self.min_price_usdt or last_price > self.max_price_usdt:
                continue

            scanner_symbols.append(
                {
                    "symbol": symbol,
                    "last_price": last_price,
                    "quote_volume_24h": quote_volume,
                }
            )
            passed_basic_filters += 1

        if not scanner_symbols:
            self.logger.info("No symbols passed basic 24h filters")
            return

        scanner_symbols.sort(key=lambda x: x.get("quote_volume_24h", 0.0), reverse=True)
        scanner_symbols = scanner_symbols[: self.max_symbols_per_scan]

        now_ts = time.time()

        for item in scanner_symbols:
            symbol = item["symbol"]
            klines = self.binance_client.get_recent_klines(symbol, interval="1m", limit=11)
            if len(klines) < 3:
                continue

            klines_requested += 1

            try:
                last_kline = klines[-1]
                prev_kline = klines[-2]

                last_quote_volume = float(last_kline[7])
                price_last = float(last_kline[4])
                price_prev = float(prev_kline[4])

                if price_prev <= 0:
                    continue

                prev_volumes = [float(k[7]) for k in klines[:-1]]
                avg_quote_volume = sum(prev_volumes) / max(len(prev_volumes), 1)
            except (TypeError, ValueError, IndexError) as exc:
                self.logger.debug("Error parsing klines for %s: %s", symbol, exc)
                continue

            if avg_quote_volume <= 0:
                continue

            volume_ratio = last_quote_volume / avg_quote_volume
            price_delta_percent = (price_last - price_prev) / price_prev * 100.0

            if last_quote_volume < self.min_volume_1m_usdt:
                continue
            if volume_ratio < self.volume_multiplier_threshold:
                continue

            if price_delta_percent < self.price_delta_percent_threshold:
                diff = self.price_delta_percent_threshold - price_delta_percent
                if diff <= 0.5:
                    self.logger.debug(
                        "Near-miss signal for %s: volume_ratio=%.2f, price_delta=%.2f%% (%.2f%% below threshold)",
                        symbol,
                        volume_ratio,
                        price_delta_percent,
                        diff,
                    )
                continue

            last_alert_ts = self.last_alert_times.get(symbol)
            if last_alert_ts is not None and (now_ts - last_alert_ts) < self.cooldown_seconds:
                continue

            base = symbol.replace("USDT", "")
            trade_url = f"https://www.binance.com/en/trade/{base}_USDT"

            futures_info_text = ""
            if self.futures_client is not None:
                fut_snapshot = self.futures_client.get_futures_snapshot(symbol)
                if fut_snapshot:
                    oi = fut_snapshot.get("open_interest", 0.0)
                    funding = fut_snapshot.get("funding_rate", 0.0)
                    mark_price = fut_snapshot.get("mark_price", 0.0)
                    futures_info_text = (
                        f"\n\n*Futures info:*\n"
                        f"Open interest: `{oi:,.0f}` контрактов\n"
                        f"Funding rate: `{funding:.4f}`\n"
                        f"Mark price: `{mark_price:.6f}` USDT"
                    )

            nansen_summary: Dict[str, Any] = {}
            nansen_cfg = self.config.get("nansen", {}) or {}
            if self.nansen_client is not None and nansen_cfg.get("enrich_binance_signals"):
                alpha_tokens = self.config.get("alpha_tokens") or []
                token_meta = next((t for t in alpha_tokens if str(t.get("symbol", "")).upper() == base), None)
                if token_meta:
                    chain = str(token_meta.get("chain") or "").lower()
                    contract = token_meta.get("contract") or ""
                    try:
                        payload = {"chain": chain, "token_address": contract, "window": "24h"}
                        netflow_resp = self.nansen_client.smart_money_netflows(payload)
                        dex_resp = self.nansen_client.smart_money_dex_trades(payload)
                        netflow_usd = None
                        wallets_count = None
                        largest_buy = None
                        wallets = []
                        if netflow_resp and isinstance(netflow_resp, dict):
                            netflow_usd = float(netflow_resp.get("netflow_usd", 0) or 0.0)
                            wallets_count = int(netflow_resp.get("smart_wallets", 0) or 0)
                            wlist = netflow_resp.get("wallets") or netflow_resp.get("addresses") or []
                            if isinstance(wlist, list):
                                wallets = [str(w) for w in wlist if isinstance(w, str)]
                        if dex_resp and isinstance(dex_resp, dict):
                            largest_buy = float(dex_resp.get("largest_buy_usd", 0) or 0.0)

                        nansen_summary = {
                            "netflow_usd": netflow_usd,
                            "wallets_count": wallets_count,
                            "largest_buy": largest_buy,
                            "wallets": wallets,
                            "chain": chain,
                            "contract": contract,
                        }
                    except Exception as exc:
                        self.logger.debug("Nansen enrichment failed for %s: %s", symbol, exc)

            mm_block = self._build_mm_context_block(
                symbol=symbol,
                chain=nansen_summary.get("chain") if nansen_summary else None,
                contract=nansen_summary.get("contract") if nansen_summary else None,
                nansen_summary=nansen_summary if nansen_summary else None,
                volume_ratio=volume_ratio,
                price_delta_percent=price_delta_percent,
            )

            msg = (
                f"🚨 *Alpha 1m сигнал*: `{symbol}`\n"
                f"Цена: `{price_last:.6f}` USDT\n"
                f"1m объём (quote): `{last_quote_volume:,.0f}` USDT\n"
                f"Средний 1m объём (предыдущие свечи): `{avg_quote_volume:,.0f}` USDT\n"
                f"Множитель объёма: `{volume_ratio:.2f}x`\n"
                f"Рост цены vs предыдущая 1m свеча: `{price_delta_percent:.2f}%`\n"
                f"[Открыть график на Binance]({trade_url})"
                f"{futures_info_text}"
                f"{mm_block}"
            )

            self.logger.info(
                "Alpha signal for %s: volume_ratio=%.2f price_delta=%.2f%%",
                symbol,
                volume_ratio,
                price_delta_percent,
            )

            self.notifier.send_message(msg)
            self.last_alert_times[symbol] = now_ts
            signals_triggered += 1

        self.logger.info(
            "Scan stats: total_tickers=%d, passed_basic_filters=%d, klines_requested=%d, signals_triggered=%d",
            total_tickers,
            passed_basic_filters,
            klines_requested,
            signals_triggered,
        )

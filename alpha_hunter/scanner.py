import logging
import os
import time
from typing import Any, Dict, List, Optional, Set

from .binance_client import BinanceClient
from .binance_futures_client import BinanceFuturesClient
from .birdeye_client import BirdEyeClient
from .hunter_state import (
    remember_alert,
    remember_event,
    seen_event,
    seen_message,
    set_actor_token_stage,
    too_soon,
    too_soon_alarm,
    too_soon_cluster_spree,
    too_soon_ping,
    record_cluster_spree,
    get_actor_token_stage,
)
from .logger import get_logger
from .nansen_client import NansenClient
from .telegram_alerts import TelegramNotifier


class AlphaScanner:
    def __init__(
        self,
        binance_client: BinanceClient,
        notifier: TelegramNotifier,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None,
        futures_client: Optional[BinanceFuturesClient] = None,
        nansen_client: Optional[NansenClient] = None,
        actor_features: Optional[Dict[str, Any]] = None,
        watchlist_addrs: Optional[List[str]] = None,
        state: Optional[Dict[str, Any]] = None,
        birdeye_client: Optional[BirdEyeClient] = None,
        initiator_index: Optional[Dict[str, Any]] = None,
        initiator_cfg: Optional[Dict[str, Any]] = None,
        nansen_cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.binance_client = binance_client
        self.notifier = notifier
        self.config = config
        self.logger = logger or get_logger(self.__class__.__name__)
        self.futures_client = futures_client
        self.nansen_client = nansen_client
        self.actor_features = actor_features or {}
        self.watchlist_addrs = {str(addr).lower() for addr in (watchlist_addrs or [])}
        self.state = state
        self.birdeye_client = birdeye_client
        self.initiator_data = initiator_index or {}
        self.initiator_cfg = initiator_cfg or {}
        self.reasons_line_enabled = bool(self.initiator_cfg.get("reasons_line"))
        self.nansen_cfg = (nansen_cfg or config.get("nansen") or {}) or {}

        scanner_cfg = config.get("scanner", {}) or {}
        signals_cfg = config.get("signals", {}) or {}
        thresholds_cfg = config.get("thresholds", {}) or {}
        self.interval_seconds = int(scanner_cfg.get("interval_seconds", 60))
        self.min_24h_volume_usdt = float(scanner_cfg.get("min_24h_volume_usdt", 1_000_000))
        self.max_24h_volume_usdt = float(scanner_cfg.get("max_24h_volume_usdt", 300_000_000))
        self.min_price_usdt = float(scanner_cfg.get("min_price_usdt", 0.02))
        self.max_price_usdt = float(scanner_cfg.get("max_price_usdt", 30))
        self.max_symbols_per_scan = int(scanner_cfg.get("max_symbols_per_scan", 80))
        self.cooldown_seconds = int(scanner_cfg.get("cooldown_seconds", 120))

        self.volume_multiplier_threshold = float(signals_cfg.get("volume_multiplier_threshold", 1.1))
        self.min_volume_1m_usdt = float(signals_cfg.get("min_volume_1m_usdt", 10_000))
        self.price_delta_percent_threshold = float(signals_cfg.get("price_delta_percent_threshold", 0.2))
        self.trust_min = float(thresholds_cfg.get("trust_min", 0.25))
        self.trust_weight = float(thresholds_cfg.get("trust_weight", 0.9))
        self.trust_cooldown_min = int(thresholds_cfg.get("trust_cooldown_min_seconds", 60))
        self.trust_cooldown_max = int(thresholds_cfg.get("trust_cooldown_max_seconds", 240))
        self.trust_override_score = float(thresholds_cfg.get("trust_override_score", 1.2))
        self.actor_trust_map: Dict[str, Dict[str, Any]] = {}

        alerts_cfg = config.get("alerts", {}) or {}
        self.alerts_enabled = bool(alerts_cfg.get("enabled", True))
        self.ping_prefix = str(alerts_cfg.get("ping_prefix", "👀"))
        self.alarm_prefix = str(alerts_cfg.get("alarm_prefix", "🚨"))
        self.min_ping_cooldown = int(alerts_cfg.get("min_seconds_between_pings_per_symbol", 900))
        self.min_alarm_cooldown = int(alerts_cfg.get("min_seconds_between_alarms_per_symbol", 1800))
        self.cluster_spree_window = 3600
        self.cluster_spree_cooldown = 1800

        self.threshold_ping = float(self.initiator_cfg.get("threshold_ping", 0.4))
        self.threshold_alarm = float(self.initiator_cfg.get("threshold_alarm", 0.75))
        self.lookback_seconds = int(self.initiator_cfg.get("lookback_seconds", 600))
        self.max_initiator_checks = int(self.initiator_cfg.get("max_tokens_per_cycle", 10))

        self.fusion_cfg = config.get("fusion", {}) or {}
        self.fusion_enabled = bool(self.fusion_cfg.get("enabled", True))
        self.hot_tokens: Dict[str, Dict[str, Any]] = {}
        self.hot_tokens_ttl = int(self.fusion_cfg.get("hot_tokens_ttl_seconds", 3600))
        self.max_hot_tokens = int(self.fusion_cfg.get("max_hot_tokens", 200))
        self.prefer_hot_tokens = bool(self.fusion_cfg.get("prefer_hot_tokens_in_scan", True))
        self.relax_hot_thresholds = bool(self.fusion_cfg.get("relax_thresholds_for_hot_tokens", True))
        self.relax_volume_delta = float(self.fusion_cfg.get("relax_volume_multiplier_delta", 0.0))
        self.relax_price_delta = float(self.fusion_cfg.get("relax_price_delta_percent_delta", 0.0))
        self.symbol_contract_map = self._build_symbol_contract_map(config.get("alpha_tokens") or [])
        self.dump_cfg = config.get("dump_alerts", {}) or {}
        self.market_dump_enabled = bool(self.dump_cfg.get("enabled", True))
        self.market_dump_vol = float(self.dump_cfg.get("market_volume_multiplier", 1.5))
        self.market_dump_price = float(self.dump_cfg.get("market_price_drop_percent", 0.4))

        self.last_alert_times: Dict[str, float] = {}
        self._initiator_tokens_seen: Set[str] = set()
        self._initiator_cycle_checks = 0
        self._cluster_spree_sent_cycle = False
        self.last_cycle_stats: Dict[str, Any] = {}
        self._nansen_cache: Dict[str, Dict[str, Any]] = {}
        self._nansen_cycle_calls = 0
        self.max_nansen_calls = max(0, int(self.nansen_cfg.get("max_calls_per_cycle", 1) or 0))
        self.nansen_cache_ttl = int(self.nansen_cfg.get("cache_seconds", 600) or 600)
        self.nansen_mode = str(self.nansen_cfg.get("mode", "auto") or "auto").lower()
        self.exit_signatures: Dict[str, Dict[str, Any]] = {}

    def update_artifacts(self, snapshot: Dict[str, Any]) -> None:
        if not snapshot:
            return
        if "features" in snapshot:
            self.actor_features = snapshot.get("features") or {}
        trust_map = snapshot.get("trust_map")
        if trust_map is not None:
            self.actor_trust_map = {
                str(addr).lower(): data for addr, data in (trust_map or {}).items()
            }
        addrs = snapshot.get("watchlist_addrs")
        if addrs is not None:
            self.watchlist_addrs = {str(addr).lower() for addr in addrs}
        if snapshot.get("initiators") is not None:
            self.initiator_data = snapshot.get("initiators") or {}
        if snapshot.get("exit_signatures") is not None:
            self.exit_signatures = snapshot.get("exit_signatures") or {}

    def scan_once(self) -> None:
        self._initiator_tokens_seen = set()
        self._initiator_cycle_checks = 0
        self._cluster_spree_sent_cycle = False
        self._nansen_cycle_calls = 0

        self.logger.info("Fetching 24h tickers from Binance")
        tickers = self.binance_client.get_24h_tickers()
        self.last_cycle_stats = {
            "total_tickers": len(tickers or []),
            "candidates": 0,
            "klines_requested": 0,
            "alerts_sent": 0,
            "pings_sent": 0,
            "alarms_sent": 0,
        }
        if not tickers:
            self.logger.warning("No tickers received from Binance")
            return

        scanner_symbols: List[Dict[str, Any]] = []
        for t in tickers:
            symbol = t.get("symbol")
            if not isinstance(symbol, str) or not self._is_valid_symbol(symbol):
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

        if not scanner_symbols:
            self.logger.info("No symbols passed 24h filters")
            return

        scanner_symbols.sort(key=lambda item: item.get("quote_volume_24h", 0.0), reverse=True)
        scanner_symbols = scanner_symbols[: self.max_symbols_per_scan]
        self.last_cycle_stats["candidates"] = len(scanner_symbols)

        now_ts = time.time()
        self._prune_hot_tokens(now_ts)
        if self.hot_tokens:
            self.logger.info("Active hot tokens: %d", len(self.hot_tokens))
        if self.fusion_enabled and self.prefer_hot_tokens and self.hot_tokens:
            hot_list: List[Dict[str, Any]] = []
            rest: List[Dict[str, Any]] = []
            for item in scanner_symbols:
                token_key_hint = self._token_key_from_symbol(item["symbol"])
                if token_key_hint and self._is_hot_token(token_key_hint, now_ts):
                    item["token_key_hint"] = token_key_hint
                    hot_list.append(item)
                else:
                    rest.append(item)
            scanner_symbols = hot_list + rest
        klines_requested = 0
        alerts_sent = 0
        pings_sent = 0
        alarms_sent = 0

        for item in scanner_symbols:
            symbol = item["symbol"]
            token_key_hint = item.get("token_key_hint") or self._token_key_from_symbol(symbol)
            hot_meta_hint = self._get_hot_meta(token_key_hint, now_ts)
            volume_threshold = self.volume_multiplier_threshold
            price_threshold = self.price_delta_percent_threshold
            if self.fusion_enabled and self.relax_hot_thresholds and hot_meta_hint:
                volume_threshold = max(0.1, volume_threshold + self.relax_volume_delta)
                price_threshold = max(0.0, price_threshold + self.relax_price_delta)
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
                prev_volumes = [float(k[7]) for k in klines[:-1]]
                avg_quote_volume = sum(prev_volumes) / max(len(prev_volumes), 1)
            except (TypeError, ValueError, IndexError):
                continue

            if price_prev <= 0 or avg_quote_volume <= 0:
                continue

            volume_ratio = last_quote_volume / avg_quote_volume
            price_delta_percent = (price_last - price_prev) / price_prev * 100.0
            hot_meta_hint = self._get_hot_meta(token_key_hint, now_ts)
            dump_pre_candidate = (
                hot_meta_hint is not None
                and price_delta_percent <= -abs(self.market_dump_price)
                and volume_ratio >= self.market_dump_vol
            )
            if last_quote_volume < self.min_volume_1m_usdt and not dump_pre_candidate:
                continue
            if volume_ratio < volume_threshold and not dump_pre_candidate:
                continue
            if price_delta_percent < price_threshold and not dump_pre_candidate:
                continue

            last_alert_ts = self.last_alert_times.get(symbol)

            trade_url = f"https://www.binance.com/en/trade/{symbol.replace('USDT','')}_USDT"
            futures_block = self._compose_futures_block(symbol)
            nansen_summary = self._maybe_fetch_nansen_summary(symbol, require_metrics=False)

            token_key = self._derive_token_key(symbol, nansen_summary) or token_key_hint
            hot_meta = self._get_hot_meta(token_key, now_ts) or hot_meta_hint

            activation = None
            if (
                self.initiator_cfg.get("enabled")
                and nansen_summary.get("chain")
                and nansen_summary.get("contract")
            ):
                activation = self._maybe_check_initiators(
                    symbol=symbol,
                    chain=str(nansen_summary.get("chain")),
                    contract=str(nansen_summary.get("contract")),
                    now_ts=now_ts,
                )

            dump_risk = self._assess_market_dump(
                klines=klines,
                volume_ratio=volume_ratio,
                price_delta_percent=price_delta_percent,
                activation=activation,
                hot_meta=hot_meta,
                token_key=token_key,
            )

            trust_payload = self._actor_trust_for_activation(activation)
            trust_value = float(trust_payload.get("trust") or 0.0)
            if activation is not None:
                activation["trust_info"] = trust_payload

            confidence = self._compute_confidence(
                activation=activation,
                volume_ratio=volume_ratio,
                price_delta_percent=price_delta_percent,
                symbol=symbol,
                now_ts=now_ts,
                nansen_summary=nansen_summary,
            )
            base_score = confidence["value"]
            final_multiplier = 1.0 + self.trust_weight * trust_value
            final_score = self._clamp(base_score * final_multiplier)
            confidence["base_value"] = base_score
            confidence["value"] = final_score
            confidence["percent"] = int(round(final_score * 100))
            confidence["label"] = "HIGH" if final_score >= 0.75 else "MED" if final_score >= 0.4 else "LOW"
            if trust_value < self.trust_min and base_score < self.trust_override_score:
                self.logger.debug(
                    "Skipping %s due to low trust base=%.2f trust=%.2f",
                    symbol,
                    base_score,
                    trust_value,
                )
                continue
            trust_cooldown = self._cooldown_for_trust(trust_value)
            effective_symbol_cooldown = (
                min(self.cooldown_seconds, trust_cooldown) if self.cooldown_seconds else trust_cooldown
            )
            if last_alert_ts is not None and (now_ts - last_alert_ts) < effective_symbol_cooldown:
                continue
            mode = self._select_alert_mode(confidence["value"])
            if not mode:
                if dump_risk.get("triggered"):
                    mode = "ping"
                else:
                    continue

            if self.state:
                if mode == "ping" and too_soon_ping(
                    self.state, symbol=symbol, cooldown_sec=self.min_ping_cooldown, now_ts=now_ts
                ):
                    self.logger.debug("Ping cooldown active for %s", symbol)
                    continue
                if mode == "alarm" and too_soon_alarm(
                    self.state, symbol=symbol, cooldown_sec=self.min_alarm_cooldown, now_ts=now_ts
                ):
                    self.logger.debug("Alarm cooldown active for %s", symbol)
                    continue
                if too_soon(self.state, symbol=symbol, cooldown_sec=effective_symbol_cooldown, now_ts=now_ts):
                    continue

            if dump_risk.get("escalate") and mode == "ping":
                mode = "alarm"

            metrics_summary = self._maybe_fetch_nansen_summary(symbol, require_metrics=True)
            if metrics_summary:
                nansen_summary.update(metrics_summary)

            message = self._compose_message(
                symbol=symbol,
                price_last=price_last,
                last_quote_volume=last_quote_volume,
                avg_quote_volume=avg_quote_volume,
                volume_ratio=volume_ratio,
                price_delta_percent=price_delta_percent,
                trade_url=trade_url,
                futures_block=futures_block,
                activation=activation,
                confidence=confidence,
                hot_meta=hot_meta,
                now_ts=now_ts,
                mode=mode,
                nansen_summary=nansen_summary,
                dump_risk=dump_risk,
            )

            if self.state and seen_message(self.state, message):
                self.logger.debug("Duplicate alert skipped for %s", symbol)
                continue

            if not self.alerts_enabled:
                self.logger.info("Alerts disabled; skipping send for %s", symbol)
                continue

            try:
                self.notifier.send_message(
                    message,
                    alert_kind=mode,
                    address=symbol,
                )
            except Exception as exc:  # pragma: no cover
                self.logger.error("Failed to send alert for %s: %s", symbol, exc)
                continue

            self.logger.info(
                "Alert trust context: symbol=%s base=%.2f trust=%.2f final=%.2f cooldown=%ss addr=%s",
                symbol,
                base_score,
                trust_value,
                confidence["value"],
                effective_symbol_cooldown,
                (trust_payload.get("address") if trust_payload else None),
            )
            if self.state and activation:
                self._mark_pump_stage(activation, token_key, now_ts, symbol)

            self.last_alert_times[symbol] = now_ts
            alerts_sent += 1
            if mode == "ping":
                pings_sent += 1
            else:
                alarms_sent += 1

            actor_addrs = activation.get("hit_addresses", []) if activation else []
            cluster_ids = activation.get("hit_clusters", []) if activation else []
            if self.state is not None:
                dedupe_cfg = (
                    self.config.get("hunter_enrichment", {}).get("dedupe", {})
                    if isinstance(self.config, dict)
                    else {}
                )
                remember_alert(
                    self.state,
                    symbol=symbol,
                    actor_addrs=actor_addrs,
                    message_text=message,
                    now_ts=int(now_ts),
                    max_hashes=int(dedupe_cfg.get("last_hashes", 300)),
                    alert_kind=mode,
                    cluster_ids=cluster_ids,
                )
                self._maybe_emit_cluster_spree(cluster_ids, now_ts)

        self.last_cycle_stats.update(
            {
                "klines_requested": klines_requested,
                "alerts_sent": alerts_sent,
                "pings_sent": pings_sent,
                "alarms_sent": alarms_sent,
            }
        )

        self.logger.info(
            "Scan stats: symbols=%d klines=%d alerts=%d (pings=%d alarms=%d)",
            len(scanner_symbols),
            klines_requested,
            alerts_sent,
            pings_sent,
            alarms_sent,
        )

    def _compose_futures_block(self, symbol: str) -> str:
        if not self.futures_client:
            return ""
        snapshot = self.futures_client.get_futures_snapshot(symbol)
        if not snapshot:
            return ""
        lines = [
            "*Futures info:*",
            f"Open interest: `{snapshot.get('open_interest', 0.0):,.0f}`",
            f"Funding rate: `{snapshot.get('funding_rate', 0.0):.4f}`",
            f"Mark price: `{snapshot.get('mark_price', 0.0):.6f}` USDT",
        ]
        return "\n".join(lines)

    def _maybe_fetch_nansen_summary(self, symbol: str, require_metrics: bool = True) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        token_key_hint = self._token_key_from_symbol(symbol)
        chain = ''
        contract = ''
        if token_key_hint and ':' in token_key_hint and not token_key_hint.startswith('bsc_symbol'):
            chain, contract = token_key_hint.split(':', 1)
        else:
            alpha_tokens = self.config.get('alpha_tokens') or []
            base = symbol.replace('USDT', '').upper()
            token_meta = next((t for t in alpha_tokens if str(t.get('symbol', '')).upper() == base), None)
            if token_meta:
                chain = str(token_meta.get('chain') or 'bsc').lower()
                contract = str(token_meta.get('contract') or '').lower()
        if chain and contract:
            result['chain'] = chain
            result['contract'] = contract
            result['token_key'] = f"{chain}:{contract}"
        else:
            return result
        if not require_metrics:
            return result
        if (
            not self.nansen_client
            or not self.nansen_cfg.get('enabled')
            or not self.nansen_cfg.get('enrich_binance_signals')
        ):
            return result
        cache_key = f"{chain}:{contract}"
        now_ts = time.time()
        cached = self._nansen_cache.get(cache_key)
        if cached and cached.get('expires_ts', 0) > now_ts:
            data = cached.get('data') or {}
            result.update(data)
            result['cached'] = True
            return result
        if self.max_nansen_calls and self._nansen_cycle_calls >= self.max_nansen_calls:
            return result
        payload = {'chain': chain, 'token_address': contract, 'window': '24h'}
        mode = self.nansen_mode
        endpoint = 'netflows' if mode in ('', 'auto', 'netflows') else 'dex_trades'
        try:
            if endpoint == 'dex_trades':
                response = self.nansen_client.smart_money_dex_trades(payload)
            else:
                response = self.nansen_client.smart_money_netflows(payload)
        except Exception as exc:  # pragma: no cover
            self.logger.debug('Nansen enrichment failed for %s: %s', symbol, exc)
            return result
        if not isinstance(response, dict):
            return result
        self._nansen_cycle_calls += 1
        data: Dict[str, Any] = {}
        if endpoint == 'dex_trades':
            data['largest_buy'] = response.get('largest_buy_usd')
            data['trades_count'] = response.get('trades') or response.get('count')
        else:
            data['netflow_usd'] = response.get('netflow_usd') or response.get('netflow')
            data['wallets_count'] = response.get('smart_wallets') or response.get('wallets_count')
            if 'largest_buy_usd' in response:
                data['largest_buy'] = response.get('largest_buy_usd')
        data['source'] = endpoint
        self._nansen_cache[cache_key] = {'expires_ts': now_ts + self.nansen_cache_ttl, 'data': data}
        result.update(data)
        result['cached'] = False
        return result

    def _assess_market_dump(
        self,
        *,
        klines: List[List[Any]],
        volume_ratio: float,
        price_delta_percent: float,
        activation: Optional[Dict[str, Any]],
        hot_meta: Optional[Dict[str, Any]],
        token_key: Optional[str],
    ) -> Dict[str, Any]:
        info = {
            'triggered': False,
            'escalate': False,
            'hot_token': bool(hot_meta),
            'initiator_hits': False,
            'red_candles': 0,
            'volume_ratio': volume_ratio,
            'price_delta_percent': price_delta_percent,
            'reasons': [],
        }
        if not self.market_dump_enabled or len(klines) < 3:
            return info
        red = 0
        for candle in klines[-3:]:
            try:
                open_price = float(candle[1])
                close_price = float(candle[4])
            except (TypeError, ValueError):
                continue
            if close_price < open_price:
                red += 1
        info['red_candles'] = red
        cond_red = red >= 2
        cond_price = price_delta_percent <= -abs(self.market_dump_price)
        cond_volume = volume_ratio >= self.market_dump_vol
        reasons: List[str] = []
        if cond_volume:
            reasons.append('volume')
        if cond_red:
            reasons.append('red')
        if cond_price:
            reasons.append('price_drop')
        triggered = cond_volume and (cond_red or cond_price)
        info['triggered'] = triggered
        if not triggered:
            return info
        initiator_hits = bool((activation or {}).get('hit_addresses') or (activation or {}).get('hit_clusters'))
        info['initiator_hits'] = initiator_hits
        info['reasons'] = reasons
        baseline = self._baseline_for_activation(activation)
        if baseline:
            info['baseline'] = baseline
        exit_state = self._exit_state_for_activation(activation, token_key)
        if exit_state:
            info['exit_state'] = exit_state
            reasons.append('exit_state')
        info['escalate'] = initiator_hits
        return info

    def _maybe_check_initiators(
        self,
        *,
        symbol: str,
        chain: str,
        contract: str,
        now_ts: float,
    ) -> Optional[Dict[str, Any]]:
        if not self.initiator_cfg.get("enabled") or not self.birdeye_client:
            return None
        initiator_addrs = self.initiator_data.get("initiator_addrs") or set()
        if isinstance(initiator_addrs, list):
            initiator_addrs = {str(addr).lower() for addr in initiator_addrs}
        else:
            initiator_addrs = {str(addr).lower() for addr in initiator_addrs}
        if not initiator_addrs:
            return None

        token_key = f"{chain}:{contract.lower()}"
        if self._initiator_cycle_checks >= self.max_initiator_checks or token_key in self._initiator_tokens_seen:
            return None
        self._initiator_tokens_seen.add(token_key)
        self._initiator_cycle_checks += 1

        start_ts = max(0, int(now_ts - self.lookback_seconds))
        try:
            items = self.birdeye_client.fetch_for_window(
                token_address=contract,
                start_ts=start_ts,
                end_ts=int(now_ts),
                label=f"initiator_{symbol}",
                chain=chain,
            )
        except Exception as exc:  # pragma: no cover
            self.logger.debug("BirdEye fetch failed for %s: %s", symbol, exc)
            return None
        if not items:
            return None

        addr_cluster = self.initiator_data.get("addr_cluster_id") or self.initiator_data.get("addr_cluster") or {}
        addr_score = self.initiator_data.get("addr_score") or {}
        addr_campaigns = self.initiator_data.get("addr_campaigns") or {}
        addr_tokens = self.initiator_data.get("addr_tokens") or {}
        addr_details = self.initiator_data.get("addr_details") or {}
        hit_addrs: Dict[str, int] = {}
        hit_clusters: Set[str] = set()
        for tx in items:
            if not isinstance(tx, dict):
                continue
            wallet = str(
                tx.get("owner")
                or tx.get("from")
                or tx.get("from_address")
                or tx.get("maker")
                or ""
            ).lower()
            if not wallet or wallet not in initiator_addrs:
                continue
            hit_addrs[wallet] = hit_addrs.get(wallet, 0) + 1
            cluster_id = addr_cluster.get(wallet)
            if cluster_id:
                hit_clusters.add(str(cluster_id))
        if not hit_addrs:
            return None
        required_clusters = int(self.initiator_cfg.get("require_cluster_hits", 0))
        if required_clusters > 0 and len(hit_clusters) < required_clusters:
            return None

        top_hits = sorted(hit_addrs, key=lambda addr: addr_score.get(addr, 0.0), reverse=True)
        top_addr = top_hits[0]
        top_entry = addr_details.get(top_addr) or {
            "address": top_addr,
            "initiator_score": addr_score.get(top_addr, 0.0),
            "campaigns_count": addr_campaigns.get(top_addr, 0),
            "tokens": addr_tokens.get(top_addr, []),
        }
        top_entry.setdefault("tokens", addr_tokens.get(top_addr, []))
        recent_notes = self._recent_activity_notes(top_hits[:3], symbol, now_ts)
        return {
            "hit_addresses": list(hit_addrs.keys()),
            "hit_clusters": list(hit_clusters),
            "top_hits": top_hits[:3],
            "top_entry": top_entry,
            "top_score": float(top_entry.get("initiator_score") or addr_score.get(top_addr, 0.0) or 0.0),
            "recent_notes": recent_notes,
            "tokens": addr_tokens.get(top_addr, []),
            "token_key": token_key,
        }

    def _compose_message(
        self,
        *,
        symbol: str,
        price_last: float,
        last_quote_volume: float,
        avg_quote_volume: float,
        volume_ratio: float,
        price_delta_percent: float,
        trade_url: str,
        futures_block: str,
        activation: Optional[Dict[str, Any]],
        confidence: Dict[str, Any],
        hot_meta: Optional[Dict[str, Any]],
        now_ts: float,
        mode: str,
        nansen_summary: Optional[Dict[str, Any]],
        dump_risk: Optional[Dict[str, Any]],
    ) -> str:
        prefix = self.alarm_prefix if mode == "alarm" else self.ping_prefix
        title = "Alpha 1m сигнал" if mode == "alarm" else "Alpha 1m наблюдение"
        parts = [
            f"{prefix} {title}: `{symbol}`",
            f"Цена: `{price_last:.6f}` USDT",
            f"1m объём: `{last_quote_volume:,.0f}` USDT",
            f"Средний 1m объём: `{avg_quote_volume:,.0f}` USDT",
            f"Усиление объёма: `{volume_ratio:.2f}x`",
            f"Δ цены к предыдущей свече: `{price_delta_percent:.2f}%`",
            f"[Открыть Binance]({trade_url})",
        ]
        if futures_block:
            parts.append(futures_block)
        context_block = self._build_initiator_context_block(activation, confidence, hot_meta, now_ts)
        if context_block:
            parts.append(context_block)
        nansen_block = self._build_nansen_block(nansen_summary)
        if nansen_block:
            parts.append(nansen_block)
        dump_block = self._build_dump_block(dump_risk)
        if dump_block:
            parts.append(dump_block)
        parts.append(f"Режим: {mode.upper()}")
        return "\n\n".join(parts)

    def _build_initiator_context_block(
        self,
        activation: Optional[Dict[str, Any]],
        confidence: Dict[str, Any],
        hot_meta: Optional[Dict[str, Any]],
        now_ts: float,
    ) -> str:
        lines = ["🧠 Initiator context:"]
        lookback_minutes = max(1, self.lookback_seconds // 60)
        if not activation:
            lines.append(f"• Совпадений с initiators нет (окно {lookback_minutes} мин)")
        else:
            hits = activation.get("hit_addresses") or []
            clusters = activation.get("hit_clusters") or []
            lines.append(f"• Совпадения: {len(hits)} кошельков (кластеры: {len(clusters)})")
            top_entry = activation.get("top_entry") or {}
            addr = str(top_entry.get("address") or "")
            addr_disp = f"{addr[:6]}..{addr[-4:]}" if addr else "n/a"
            score = float(top_entry.get("initiator_score") or 0.0)
            campaigns = int(top_entry.get("campaigns_count") or 0)
            pre_buy = top_entry.get("avg_pre_buy_usd")
            exit_sec = top_entry.get("median_exit_speed_seconds")
            exit_minutes = f"{exit_sec / 60:.1f}m" if exit_sec else "n/a"
            line = f"• Топ: {addr_disp} | score={score:.2f} | кампаний={campaigns}"
            if pre_buy:
                line += f" | pre_buy≈${pre_buy:,.0f}"
            if exit_sec:
                line += f" | exit≈{exit_minutes}"
            lines.append(line)
            tokens = top_entry.get("tokens") or activation.get("tokens") or []
            if tokens:
                lines.append("• Tokens: " + ", ".join(tokens[:3]))
            recent_notes = activation.get("recent_notes") or []
            if recent_notes:
                lines.append("• Последние активности: " + "; ".join(recent_notes))
        if hot_meta:
            ttl = max(0, int(hot_meta.get("expires_ts", now_ts) - now_ts))
            ttl_minutes = ttl // 60
            lines.append(
                f"• Hot token: YES (ttl {ttl_minutes}m, w={hot_meta.get('weight', 0.0):.2f})"
            )
        trust_info = (activation or {}).get("trust_info")
        trust_line = self._format_trust_line(trust_info)
        if trust_line:
            lines.append(trust_line)
        lines.append(f"• Оценка вероятности пампа: {confidence['percent']}% ({confidence['label']})")
        reasons = confidence.get("reasons_line")
        if reasons:
            lines.append(reasons)
        return "\n".join(lines)

    def _actor_trust_for_activation(self, activation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not activation or not self.actor_trust_map:
            return {"trust": 0.0}
        candidates = (activation.get("top_hits") or activation.get("hit_addresses") or [])[:3]
        for addr in candidates:
            norm = str(addr or "").lower()
            if not norm:
                continue
            entry = self.actor_trust_map.get(norm)
            if entry:
                payload = dict(entry)
                payload["address"] = norm
                return payload
        return {"trust": 0.0}

    def _cooldown_for_trust(self, trust_value: float) -> int:
        try:
            trust = float(trust_value)
        except (TypeError, ValueError):
            trust = 0.0
        trust = max(0.0, min(1.0, trust))
        span = max(self.trust_cooldown_max - self.trust_cooldown_min, 0)
        cooldown = self.trust_cooldown_max - int(span * trust)
        cooldown = max(self.trust_cooldown_min, min(self.trust_cooldown_max, cooldown))
        return cooldown

    def _format_trust_line(self, trust_info: Optional[Dict[str, Any]]) -> str:
        if not trust_info:
            return ""
        trust_val = trust_info.get("trust")
        if trust_val is None:
            return ""
        last_summary = trust_info.get("last_summary") or {}
        realized = last_summary.get("realized_pnl_usd")
        pressure = last_summary.get("buy_pressure_usd")
        reason = last_summary.get("trust_reason") or trust_info.get("reason") or "n/a"
        realized_text = f"{realized:+,.0f}$" if isinstance(realized, (int, float)) else "n/a"
        pressure_text = f"${pressure:,.0f}" if isinstance(pressure, (int, float)) else "n/a"
        return f"• Trust: {float(trust_val):.2f} | last pump: {realized_text} | pressure: {pressure_text} | reason: {reason}"

    def _build_nansen_block(self, summary: Optional[Dict[str, Any]]) -> str:
        if not summary:
            return ""
        netflow = summary.get("netflow_usd")
        wallets = summary.get("wallets_count")
        largest_buy = summary.get("largest_buy")
        trades = summary.get("trades_count")
        has_data = False
        for value in (netflow, wallets, largest_buy, trades):
            if isinstance(value, (int, float)) and value:
                has_data = True
                break
        if not has_data:
            return ""
        lines = ["🧩 Nansen:"]
        if isinstance(wallets, (int, float)) and wallets >= 0:
            lines.append(f"• Smart wallets: {int(wallets)}")
        if isinstance(netflow, (int, float)):
            sign = "+" if netflow >= 0 else ""
            lines.append(f"• Netflow 24h: {sign}${netflow:,.0f}")
        if isinstance(largest_buy, (int, float)) and largest_buy > 0:
            lines.append(f"• Largest buy: ≈${largest_buy:,.0f}")
        if isinstance(trades, (int, float)) and trades > 0:
            lines.append(f"• Trades tracked: {int(trades)}")
        return "\n".join(lines)

    def _build_dump_block(self, dump_risk: Optional[Dict[str, Any]]) -> str:
        if not dump_risk or not dump_risk.get("triggered"):
            return ""
        lines = ["? Dump risk:"]
        red = int(dump_risk.get("red_candles", 0) or 0)
        lines.append(f"? Red candles (last 3): {red}/3")
        vol = dump_risk.get("volume_ratio", 0.0) or 0.0
        delta = dump_risk.get("price_delta_percent", 0.0) or 0.0
        lines.append(f"? Volume: {vol:.2f}x | ?={delta:.2f}%")
        if dump_risk.get("initiator_hits"):
            lines.append("? Initiators present in flow")
        if dump_risk.get("hot_token"):
            lines.append("? Token flagged as HOT")
        reasons = dump_risk.get("reasons")
        if reasons:
            lines.append(f"? Reasons: {', '.join(reasons)}")
        baseline = dump_risk.get("baseline")
        if isinstance(baseline, dict):
            exit_sec = baseline.get("exit_speed_p50_seconds")
            exit_minutes = f"{exit_sec/60:.1f}m" if isinstance(exit_sec, (int, float)) and exit_sec > 0 else "n/a"
            dump_ratio = baseline.get("dump_ratio_p50")
            ratio_text = f"{dump_ratio:.2f}x" if isinstance(dump_ratio, (int, float)) else "n/a"
            lines.append(f"? Exit baseline: p50?{exit_minutes} | dump?{ratio_text}")
        exit_state = dump_risk.get("exit_state")
        if isinstance(exit_state, dict):
            score = exit_state.get("exit_wave_score")
            if isinstance(score, (int, float)):
                lines.append(f"? Current exit wave score: {score:.2f}")
        return "\n".join(lines)

    def _mark_pump_stage(
        self,
        activation: Optional[Dict[str, Any]],
        token_key: Optional[str],
        now_ts: float,
        symbol: str,
    ) -> None:
        if not self.state or not activation:
            return
        stage_token = activation.get("token_key") or token_key
        if not stage_token:
            return
        for addr in activation.get("hit_addresses", []):
            payload = {"symbol": symbol, "source": "scanner", "updated_at": now_ts}
            set_actor_token_stage(
                self.state,
                addr=addr,
                token_key=stage_token,
                stage="PUMP",
                now_ts=int(now_ts),
                last=payload,
            )

    def _baseline_for_activation(self, activation: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not activation or not self.exit_signatures:
            return None
        top_entry = activation.get("top_entry") or {}
        addr = str(top_entry.get("address") or "").lower()
        if not addr:
            return None
        if addr in self.exit_signatures:
            return self.exit_signatures[addr]
        chain_key = f"bsc:{addr}"
        return self.exit_signatures.get(chain_key)

    def _exit_state_for_activation(
        self,
        activation: Optional[Dict[str, Any]],
        token_key: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not self.state or not activation:
            return None
        token = activation.get("token_key") or token_key
        if not token:
            return None
        top_entry = activation.get("top_entry") or {}
        addr = top_entry.get("address")
        if not addr:
            return None
        entry = get_actor_token_stage(self.state, addr, token)
        if not entry or entry.get("stage") != "EXIT":
            return None
        return entry.get("last") or {}

    def _compute_confidence(
        self,
        *,
        activation: Optional[Dict[str, Any]],
        volume_ratio: float,
        price_delta_percent: float,
        symbol: str,
        now_ts: float,
        nansen_summary: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        hit_addrs = len((activation or {}).get("hit_addresses") or [])
        hit_clusters = len((activation or {}).get("hit_clusters") or [])
        top_score = float((activation or {}).get("top_score") or 0.0)
        market_strength = self._market_strength(volume_ratio, price_delta_percent)
        initiator_strength = self._initiator_strength(hit_addrs, hit_clusters, top_score)
        seen_minutes = self._symbol_seen_minutes(symbol, activation, now_ts)
        novelty = self._novelty_penalty(seen_minutes)
        nansen_strength = self._nansen_strength(nansen_summary)
        value = self._clamp(
            0.4 * market_strength + 0.4 * initiator_strength + 0.1 * novelty + 0.1 * nansen_strength
        )
        percent = int(round(value * 100))
        label = "HIGH" if value >= 0.75 else "MED" if value >= 0.4 else "LOW"
        reasons_line = None
        if self.reasons_line_enabled:
            seen_text = "?" if seen_minutes is None else f"{int(seen_minutes)}m"
            wallets = int((nansen_summary or {}).get("wallets_count") or 0)
            netflow_val = (nansen_summary or {}).get("netflow_usd")
            netflow_text = "n/a"
            if isinstance(netflow_val, (int, float)):
                netflow_text = f"{netflow_val/1000:.1f}k"
            reasons_line = (
                f"???????: clusters={hit_clusters} addrs={hit_addrs} top={top_score:.2f} "
                f"vol={volume_ratio:.2f}x ?={price_delta_percent:.2f}% "
                f"seen={seen_text} nansen={wallets}/{netflow_text}"
            )
        return {
            "value": value,
            "percent": percent,
            "label": label,
            "reasons_line": reasons_line,
        }

    def _select_alert_mode(self, confidence_value: float) -> Optional[str]:
        if confidence_value >= self.threshold_alarm:
            return "alarm"
        if confidence_value >= self.threshold_ping:
            return "ping"
        return None

    def _symbol_seen_minutes(
        self,
        symbol: str,
        activation: Optional[Dict[str, Any]],
        now_ts: float,
    ) -> Optional[float]:
        if not self.state or not activation:
            return None
        symbol_upper = symbol.upper()
        actor_map = (self.state.get("symbol_recent_actors") or {}).get(symbol_upper) or {}
        cluster_map = (self.state.get("symbol_recent_clusters") or {}).get(symbol_upper) or {}
        deltas: List[float] = []
        for addr in activation.get("hit_addresses", []):
            ts = actor_map.get(addr.lower())
            if ts:
                deltas.append(max(0.0, (now_ts - ts) / 60.0))
        for cluster_id in activation.get("hit_clusters", []):
            ts = cluster_map.get(cluster_id)
            if ts:
                deltas.append(max(0.0, (now_ts - ts) / 60.0))
        if not deltas:
            return None
        return min(deltas)

    def _recent_activity_notes(self, addrs: List[str], symbol: str, now_ts: float) -> List[str]:
        notes: List[str] = []
        if not self.state:
            return notes
        actor_map = self.state.get("actor_recent_symbols") or {}
        for addr in addrs:
            sym_map = actor_map.get(addr.lower()) or {}
            ts = sym_map.get(symbol.upper())
            if not ts:
                continue
            minutes = max(1, int((now_ts - ts) / 60))
            notes.append(f"{addr[:6]}..{addr[-4:]} видел {symbol} {minutes}м назад")
        return notes

    def update_hot_tokens(self, hot_entries: List[Dict[str, Any]], now_ts: float) -> None:
        if not self.fusion_enabled or not hot_entries:
            return
        for entry in hot_entries:
            token_key = entry.get("token_key")
            if not token_key:
                continue
            weight = max(0.0, min(1.0, float(entry.get("weight") or 0.0)))
            meta = {
                "expires_ts": now_ts + self.hot_tokens_ttl,
                "weight": weight,
                "last_seen_ts": now_ts,
                "addr": entry.get("addr"),
                "initiator_score": entry.get("initiator_score"),
                "new_token": bool(entry.get("new_token")),
            }
            self.hot_tokens[token_key] = meta
        self._prune_hot_tokens(now_ts)

    def _build_symbol_contract_map(self, entries: List[Dict[str, Any]]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for entry in entries or []:
            if not isinstance(entry, dict):
                continue
            symbol = str(entry.get("symbol") or "").upper()
            contract = entry.get("contract")
            if not symbol or not contract:
                continue
            chain = str(entry.get("chain") or "bsc").lower()
            mapping[symbol] = f"{chain}:{str(contract).lower()}"
        return mapping

    def _token_key_from_symbol(self, symbol: str) -> Optional[str]:
        if not symbol:
            return None
        base = symbol.replace("USDT", "").upper()
        if base in self.symbol_contract_map:
            return self.symbol_contract_map[base]
        if base:
            return f"bsc_symbol:{base}"
        return None

    def _derive_token_key(self, symbol: str, nansen_summary: Optional[Dict[str, Any]]) -> Optional[str]:
        if nansen_summary and nansen_summary.get("contract"):
            chain = str(nansen_summary.get("chain") or "bsc").lower()
            contract = str(nansen_summary.get("contract")).lower()
            return f"{chain}:{contract}"
        return self._token_key_from_symbol(symbol)

    def _get_hot_meta(self, token_key: Optional[str], now_ts: float) -> Optional[Dict[str, Any]]:
        if not token_key:
            return None
        meta = self.hot_tokens.get(token_key)
        if not meta:
            return None
        if meta.get("expires_ts", 0) < now_ts:
            self.hot_tokens.pop(token_key, None)
            return None
        return meta

    def _is_hot_token(self, token_key: str, now_ts: float) -> bool:
        return self._get_hot_meta(token_key, now_ts) is not None

    def _prune_hot_tokens(self, now_ts: float) -> None:
        expired = [token for token, meta in self.hot_tokens.items() if meta.get("expires_ts", 0) < now_ts]
        for token in expired:
            self.hot_tokens.pop(token, None)
        if len(self.hot_tokens) <= self.max_hot_tokens:
            return
        for token, _ in sorted(self.hot_tokens.items(), key=lambda kv: kv[1].get("last_seen_ts", 0))[
            : len(self.hot_tokens) - self.max_hot_tokens
        ]:
            self.hot_tokens.pop(token, None)

    @staticmethod
    def _market_strength(volume_ratio: float, price_delta_percent: float) -> float:
        vol_norm = AlphaScanner._norm01(volume_ratio, 1.5, 6.0)
        price_norm = AlphaScanner._norm01(price_delta_percent, 0.2, 5.0)
        return AlphaScanner._clamp(0.6 * vol_norm + 0.4 * price_norm)

    @staticmethod
    def _initiator_strength(hit_addrs: int, hit_clusters: int, top_score: float) -> float:
        cluster_norm = AlphaScanner._norm01(hit_clusters, 0, 3)
        addr_norm = AlphaScanner._norm01(hit_addrs, 0, 5)
        score_norm = AlphaScanner._norm01(top_score, 0.3, 1.0)
        return AlphaScanner._clamp(0.3 * cluster_norm + 0.2 * addr_norm + 0.5 * score_norm)

    @staticmethod
    def _nansen_strength(summary: Optional[Dict[str, Any]]) -> float:
        if not summary:
            return 0.0
        wallets = float(summary.get("wallets_count") or 0.0)
        netflow = float(summary.get("netflow_usd") or 0.0)
        wallets_norm = AlphaScanner._norm01(wallets, 2, 15)
        netflow_norm = AlphaScanner._norm01(max(0.0, netflow), 25000, 250000)
        return AlphaScanner._clamp(0.6 * wallets_norm + 0.4 * netflow_norm)

    @staticmethod
    def _novelty_penalty(seen_minutes: Optional[float]) -> float:
        if seen_minutes is None:
            return 0.1
        if seen_minutes < 15:
            return -0.3
        if seen_minutes < 30:
            return -0.15
        if seen_minutes > 180:
            return 0.2
        if seen_minutes > 90:
            return 0.1
        return 0.0

    @staticmethod
    def _norm01(value: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return max(0.0, min(1.0, (value - lo) / (hi - lo)))

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, value))

    def _maybe_emit_cluster_spree(self, cluster_ids: List[str], now_ts: float) -> None:
        if not self.state or not cluster_ids or self._cluster_spree_sent_cycle:
            return
        cluster_map = self.state.get("cluster_recent_symbols") or {}
        for cluster_id in cluster_ids:
            bucket = cluster_map.get(cluster_id) or {}
            recent_symbols = [
                sym for sym, ts in bucket.items() if now_ts - ts <= self.cluster_spree_window
            ]
            unique_symbols = sorted(set(recent_symbols))
            if len(unique_symbols) < 3:
                continue
            event_key = f"spree:{cluster_id}:{int(now_ts) // 1800}"
            if (
                too_soon_cluster_spree(
                    self.state, cluster_id=cluster_id, cooldown_sec=self.cluster_spree_cooldown, now_ts=now_ts
                )
                or seen_event(self.state, event_key)
            ):
                continue
            msg = (
                f"{self.ping_prefix} Cluster spree: cluster={cluster_id} symbols="
                f"{', '.join(unique_symbols[-5:])} (last {self.cluster_spree_window // 60}m)"
            )
            try:
                self.notifier.send_message(
                    msg,
                    alert_kind="cluster_spree",
                    address=cluster_id,
                )
                record_cluster_spree(self.state, cluster_id, int(now_ts))
                remember_event(self.state, event_key, int(now_ts))
                self._cluster_spree_sent_cycle = True
                break
            except Exception as exc:  # pragma: no cover
                self.logger.error("Failed to send cluster spree alert: %s", exc)

    def _is_valid_symbol(self, symbol: str) -> bool:
        if not symbol.endswith("USDT"):
            return False
        suffixes = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
        return not symbol.endswith(suffixes)

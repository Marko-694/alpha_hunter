import time
from typing import Any, Dict, List, Optional

from .logger import get_logger
from .nansen_client import NansenClient
from .telegram_alerts import TelegramNotifier


class NansenSmartMoneyMonitor:
    def __init__(
        self,
        nansen_client: NansenClient,
        notifier: TelegramNotifier,
        config: Dict[str, Any],
        logger=None,
    ) -> None:
        self.nansen_client = nansen_client
        self.notifier = notifier
        self.config = config
        self.logger = logger or get_logger("nansen_monitor")

        cfg = config.get("nansen", {}) or {}
        self.enabled = bool(cfg.get("enabled", False))
        self.interval_sec = int(cfg.get("token_screener_interval_sec", 300))
        self.smart_window_min = int(cfg.get("smart_money_window_minutes", 60))
        self.max_credits_per_day = int(cfg.get("max_credits_per_day", 80))
        self.min_netflow_usd = float(cfg.get("min_token_netflow_usd_24h", 10_000))
        self.min_wallets = int(cfg.get("min_smart_wallets_count", 3))

        self.last_run_ts: float = 0.0
        self.credits_used: int = 0
        self.last_credits_day = time.strftime("%Y-%m-%d", time.gmtime())

    def _within_limits(self) -> bool:
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today != self.last_credits_day:
            self.last_credits_day = today
            self.credits_used = 0
        return self.credits_used < self.max_credits_per_day

    def run_once(self, alpha_tokens: List[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        now_ts = time.time()
        if self.last_run_ts and (now_ts - self.last_run_ts) < self.interval_sec:
            return
        if not self._within_limits():
            self.logger.warning("Nansen credit limit reached for the day, skipping run")
            return

        tokens_payload = []
        for t in alpha_tokens:
            chain = str(t.get("chain") or "").lower()
            contract = t.get("contract") or ""
            symbol = str(t.get("symbol") or "").upper()
            if chain and contract and symbol:
                tokens_payload.append({"chain": chain, "token_address": contract, "symbol": symbol})

        if not tokens_payload:
            self.last_run_ts = now_ts
            return

        payload = {
            "tokens": [{"chain": t["chain"], "token_address": t["token_address"]} for t in tokens_payload],
            "window": f"{self.smart_window_min}m",
        }

        resp = self.nansen_client.token_screener(payload)
        if resp is None:
            self.last_run_ts = now_ts
            return

        self.credits_used += 1
        tokens_data = resp.get("tokens") or resp.get("data") or []
        if not isinstance(tokens_data, list):
            self.last_run_ts = now_ts
            return

        for entry in tokens_data:
            try:
                contract = entry.get("token_address") or entry.get("address") or ""
                chain = entry.get("chain") or ""
                netflow = float(entry.get("smart_money_net_inflow_usd", 0) or 0)
                wallet_count = int(entry.get("smart_wallets", 0) or 0)
            except Exception:
                continue

            if netflow < self.min_netflow_usd or wallet_count < self.min_wallets:
                continue

            symbol_match = next((t for t in tokens_payload if t["token_address"] == contract and t["chain"] == chain), None)
            if not symbol_match:
                continue

            symbol = symbol_match["symbol"]
            msg = (
                f"🧠 Nansen Smart Money сигнал: `{symbol}`\n\n"
                f"Chain: `{chain}`\n"
                f"Smart Money net inflow: `{netflow:,.0f} USD`\n"
                f"Активных смарт-кошельков: `{wallet_count}`\n"
                f"[Открыть график на Binance](https://www.binance.com/ru/trade/{symbol})"
            )
            self.notifier.send_message(msg)

        self.last_run_ts = now_ts

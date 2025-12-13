import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from .logger import get_logger
from .telegram_alerts import TelegramNotifier
from .explorer_client import ExplorerClient


class StablecoinInflowMonitor:
    """
    Монитор стейбл-инфлоу на биржевые адреса.
    Использует токен-транзакции из Etherscan-совместимых API.
    """

    def __init__(
        self,
        notifier: TelegramNotifier,
        config: Dict[str, Any],
        logger=None,
        explorer_client: Optional[ExplorerClient] = None,
    ) -> None:
        self.logger = logger or get_logger("stablecoin_inflow")
        self.notifier = notifier
        self.config = config

        inflow_cfg = config.get("stablecoin_inflow", {}) or {}

        self.enabled: bool = bool(inflow_cfg.get("enabled", False))
        self.poll_interval_seconds: int = int(inflow_cfg.get("poll_interval_seconds", 300))
        self.min_usd_value: float = float(inflow_cfg.get("min_usd_value", 200_000))
        self.lookback_seconds: int = int(inflow_cfg.get("lookback_seconds", 3600))
        self.state_file: str = str(inflow_cfg.get("state_file", "data/stablecoin_inflow_state.json"))

        stable_symbols_cfg = inflow_cfg.get("stable_symbols") or []
        self.stable_symbols: List[str] = [str(s).upper() for s in stable_symbols_cfg]

        exchange_addresses_cfg = inflow_cfg.get("exchange_addresses", {}) or {}
        self.exchange_addresses: List[Dict[str, str]] = []
        for chain, addr_list in exchange_addresses_cfg.items():
            for addr in (addr_list or []):
                addr_str = str(addr).strip()
                if addr_str:
                    self.exchange_addresses.append({"chain": str(chain), "address": addr_str})

        self.explorer_client = explorer_client
        self.last_run_ts: float = 0.0

        self.state: Dict[str, Dict[str, int]] = self._load_state()

        self.logger.info(
            "StablecoinInflowMonitor initialized: enabled=%s, poll_interval=%ss, addresses=%d, stable_symbols=%s",
            self.enabled,
            self.poll_interval_seconds,
            len(self.exchange_addresses),
            ",".join(self.stable_symbols),
        )

    def _load_state(self) -> Dict[str, Dict[str, int]]:
        if not self.state_file:
            return {}
        if not os.path.exists(self.state_file):
            return {}
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception as exc:
            self.logger.error("Failed to load stablecoin inflow state from %s: %s", self.state_file, exc)
        return {}

    def _save_state(self) -> None:
        if not self.state_file:
            return
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f)
        except Exception as exc:
            self.logger.error("Failed to save stablecoin inflow state to %s: %s", self.state_file, exc)

    def _get_last_block(self, chain: str, address: str) -> Optional[int]:
        chain_state = self.state.get(chain) or {}
        last_block = chain_state.get(address)
        if last_block is None:
            return None
        try:
            return int(last_block)
        except (TypeError, ValueError):
            return None

    def _set_last_block(self, chain: str, address: str, block_number: int) -> None:
        chain_state = self.state.setdefault(chain, {})
        chain_state[address] = int(block_number)

    def _normalize_pair_key(self, chain: str, address: str) -> Tuple[str, str]:
        return chain, address.lower()

    def run_once(self) -> None:
        if not self.enabled:
            return
        if not self.exchange_addresses:
            return
        if self.explorer_client is None:
            self.logger.debug("Explorer client not configured, skipping stablecoin inflow monitor")
            return

        now_ts = time.time()
        if self.last_run_ts and (now_ts - self.last_run_ts) < self.poll_interval_seconds:
            return

        since_ts = now_ts - float(self.lookback_seconds)

        total_alerts = 0
        for item in self.exchange_addresses:
            chain = str(item.get("chain"))
            address = str(item.get("address"))
            if not chain or not address:
                continue
            if not self.explorer_client.has_api_for_chain(chain):
                continue

            last_block = self._get_last_block(chain, address)
            transfers = self.explorer_client.get_recent_token_transfers(
                chain=chain,
                address=address,
                start_block=last_block + 1 if last_block is not None else None,
                sort="asc",
            )
            if not transfers:
                continue

            address_lc = address.lower()
            max_block_for_pair: Optional[int] = last_block

            for tx in transfers:
                try:
                    to_addr = str(tx.get("to") or "").lower()
                    from_addr = str(tx.get("from") or "")
                    token_symbol = str(tx.get("tokenSymbol") or "").upper()
                    token_decimals = int(tx.get("tokenDecimal") or 18)
                    value_raw = int(tx.get("value") or 0)
                    block_number = int(tx.get("blockNumber") or 0)
                    ts_int = int(tx.get("timeStamp") or 0)
                    tx_hash = str(tx.get("hash") or "")
                except Exception:
                    continue

                if to_addr != address_lc:
                    continue

                if self.stable_symbols and token_symbol not in self.stable_symbols:
                    continue

                if ts_int and ts_int < int(since_ts):
                    continue

                amount = value_raw / (10**token_decimals) if token_decimals >= 0 else float(value_raw)
                usd_value = amount

                if usd_value < self.min_usd_value:
                    continue

                tx_url = self.explorer_client.get_tx_url(chain, tx_hash) or ""

                msg = (
                    "🐳 *Stablecoin inflow detected*\n"
                    f"Сеть: `{chain}`\n"
                    f"Токен: `{token_symbol}`\n"
                    f"Объём: `${usd_value:,.0f}`\n"
                    f"От: `{from_addr}`\n"
                    f"К: `{address}`\n"
                )
                if tx_url:
                    msg += f"[Tx link]({tx_url})"
                else:
                    msg += f"Tx hash: `{tx_hash}`"

                self.logger.info(
                    "Stablecoin inflow: chain=%s symbol=%s usd=%.0f to=%s",
                    chain,
                    token_symbol,
                    usd_value,
                    address,
                )
                self.notifier.send_message(msg)
                total_alerts += 1

                if max_block_for_pair is None or block_number > max_block_for_pair:
                    max_block_for_pair = block_number

            if max_block_for_pair is not None:
                self._set_last_block(chain, address, max_block_for_pair)

        if total_alerts:
            self._save_state()

        self.last_run_ts = now_ts

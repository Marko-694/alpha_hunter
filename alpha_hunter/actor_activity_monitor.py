import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .covalent_client import CovalentClient
from .hunter_state import (
    get_actor_token_stage,
    is_new_token_for_actor,
    prune_actor_token_stage,
    remember_actor_token,
    record_actor_activity_alert,
    remember_event,
    seen_event,
    set_actor_token_stage,
    too_soon_actor_activity,
)
from .logger import get_logger


def _norm_addr(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).strip().lower()


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _token_key(chain: str, contract: Optional[str], symbol: Optional[str]) -> Optional[str]:
    chain_key = (chain or "bsc").lower()
    if contract:
        return f"{chain_key}:{contract.lower()}"
    if symbol:
        return f"{chain_key}_symbol:{symbol.upper()}"
    return None


def _short_addr(addr: str) -> str:
    if len(addr) <= 10:
        return addr
    return f"{addr[:6]}..{addr[-4:]}"


class ActorActivityMonitor:
    """
    Samples watchlist/initiator wallets, records their token usage,
    produces structured alerts, and surfaces hot tokens for fusion logic.
    """

    def __init__(
        self,
        *,
        covalent_client: Optional[CovalentClient],
        notifier,
        config: Dict[str, Any],
        state: Optional[Dict[str, Any]],
        logger: Optional[logging.Logger] = None,
        state_path: str = "data/actor_activity_state.json",
        warehouse_path: str = "data/alpha_profiler/warehouse/actor_activity.json",
        history_path: str = "data/alpha_profiler/warehouse/actor_activity_history.jsonl",
    ) -> None:
        self.logger = logger or get_logger(self.__class__.__name__)
        self.cfg = config.get("actor_activity", {}) or {}
        self.alerts_cfg = config.get("alerts", {}) or {}
        self.new_token_cfg = config.get("new_token", {}) or {}
        self.fusion_cfg = config.get("fusion", {}) or {}
        self.dump_cfg = config.get("dump_alerts", {}) or {}
        self.enabled = bool(self.cfg.get("enabled"))
        self.covalent_client = covalent_client
        self.notifier = notifier
        self.state = state
        self.state_path = Path(state_path)
        self.warehouse_path = Path(warehouse_path)
        self.history_path = Path(history_path)

        self.interval_seconds = int(self.cfg.get("interval_seconds", 600))
        legacy_window = int(self.cfg.get("lookback_minutes", 0) or 0)
        default_window = legacy_window or int(self.dump_cfg.get("window_minutes", 0) or 15)
        self.window_minutes = int(self.cfg.get("window_minutes", default_window))
        self.lookback_minutes = self.window_minutes
        self.max_addresses_per_tick = int(self.cfg.get("max_addresses_per_tick", 25))
        self.max_addrs_per_run = int(self.cfg.get("max_addrs_per_run", self.max_addresses_per_tick))
        self.shuffle_each_cycle = bool(self.cfg.get("shuffle_each_cycle", False))
        self.min_total_usd_volume = float(self.cfg.get("min_total_usd_volume", 2000))
        self.min_tx_count = int(self.cfg.get("min_tx_count", 8))
        self.alert_top_tokens = int(self.cfg.get("alert_top_tokens", 3))
        self.cooldown_actor_activity = int(
            self.alerts_cfg.get("min_seconds_between_actor_activity_alerts", 900)
        )
        self.ping_prefix = str(self.alerts_cfg.get("ping_prefix", "👀"))
        self.dump_enabled = bool(self.dump_cfg.get("enabled", True))
        self.dump_min_tx = int(
            self.dump_cfg.get("min_tx")
            or self.dump_cfg.get("min_tx_count", self.min_tx_count)
        )
        self.dump_min_total = float(self.dump_cfg.get("min_total_usd_volume", self.min_total_usd_volume))
        self.dump_min_sell_usd = float(self.dump_cfg.get("min_sell_usd", 2000))
        self.dump_min_sell_ratio = float(self.dump_cfg.get("min_sell_ratio", 0.65))
        self.dump_min_score = float(self.dump_cfg.get("min_initiator_score", 0.0))
        self.dump_require_initiator = bool(self.dump_cfg.get("require_initiator", False))
        self.dump_cooldown = int(self.dump_cfg.get("cooldown_seconds", 1800))
        self.dump_bucket_seconds = max(60, self.dump_cooldown or 1800)
        self.dump_window_minutes = int(self.dump_cfg.get("window_minutes", self.window_minutes))

        self.exit_wave_cfg = self.dump_cfg.get("exit_wave", {}) or {}
        self.exit_wave_enabled = bool(self.exit_wave_cfg.get("enabled", True))
        self.exit_wave_accel_threshold = float(self.exit_wave_cfg.get("accel_threshold", 0.2))
        self.exit_wave_score_threshold = float(self.exit_wave_cfg.get("score_threshold", 0.7))
        self.setup_sell_ratio_threshold = float(
            self.dump_cfg.get("setup_sell_ratio_threshold", self.dump_min_sell_ratio)
        )

        self.max_token_history = int(self.new_token_cfg.get("max_tokens_per_actor_history", 2000))
        self.history_days = int(self.new_token_cfg.get("history_days", 14))
        self.min_first_seen_minutes = int(self.new_token_cfg.get("min_first_seen_minutes_ago", 3))

        self.event_bucket_seconds = 1800
        thresholds_cfg = config.get("thresholds", {}) or {}
        self.trust_min = float(thresholds_cfg.get("trust_min", 0.25))
        self.trust_weight = float(thresholds_cfg.get("trust_weight", 0.9))
        self.trust_override_score = float(thresholds_cfg.get("trust_override_score", 1.2))
        self.trust_cooldown_min = int(thresholds_cfg.get("trust_cooldown_min_seconds", 60))
        self.trust_cooldown_max = int(thresholds_cfg.get("trust_cooldown_max_seconds", 240))
        self.trust_map: Dict[str, Dict[str, Any]] = {}

        self._internal_state = self._load_state()
        self._state_cursor_key = "actor_activity_cursor"
        self._state_cycle_key = "actor_activity_last_cycle_id"
        self._tracked: List[Dict[str, Any]] = []
        self._cycle_marker = self._load_cycle_marker()
        self._cursor = self._load_cursor()
        self.exit_signatures: Dict[str, Dict[str, Any]] = {}

    def update_sources(
        self,
        *,
        watchlist_records: Optional[List[Dict[str, Any]]] = None,
        initiator_snapshot: Optional[Dict[str, Any]] = None,
        exit_signatures: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.enabled:
            return
        if exit_signatures is not None:
            self.exit_signatures = {
                str(addr).lower(): data for addr, data in (exit_signatures or {}).items()
            }
        combined: Dict[str, Dict[str, Any]] = {}
        for record in watchlist_records or []:
            if not isinstance(record, dict):
                continue
            addr = _norm_addr(record.get("address"))
            if not addr:
                continue
            combined.setdefault(addr, {"address": addr})["watchlist"] = record
        if initiator_snapshot:
            initiator_addrs = initiator_snapshot.get("initiator_addrs") or []
            addr_details = initiator_snapshot.get("addr_details") or {}
            addr_scores = initiator_snapshot.get("addr_score") or {}
            addr_campaigns = initiator_snapshot.get("addr_campaigns") or {}
            for addr in initiator_addrs:
                norm = _norm_addr(addr)
                if not norm:
                    continue
                entry = combined.setdefault(norm, {"address": norm})
                entry["initiator"] = addr_details.get(norm) or {}
                entry["initiator_score"] = float(addr_scores.get(norm, 0.0))
                entry["campaigns_count"] = int(addr_campaigns.get(norm, 0))
        tracked: List[Dict[str, Any]] = []
        for addr, payload in combined.items():
            chain = "bsc"
            actor_score = None
            source_tags: List[str] = []
            if payload.get("watchlist"):
                wl = payload["watchlist"]
                chain = wl.get("chain") or chain
                actor_score = wl.get("actor_score")
                source_tags.append("watchlist")
            initiator_info = payload.get("initiator")
            if initiator_info:
                chain = initiator_info.get("chain") or chain
                source_tags.append("initiator")
            tracked.append(
                {
                    "address": addr,
                    "chain": str(chain or "bsc").lower(),
                    "actor_score": float(actor_score) if actor_score is not None else None,
                    "initiator_details": initiator_info or {},
                    "initiator_score": float(payload.get("initiator_score") or 0.0),
                    "campaigns_count": int(payload.get("campaigns_count") or 0),
                    "source_tags": source_tags,
                }
            )
        tracked.sort(key=lambda item: (0 if "initiator" in item["source_tags"] else 1, item["address"]))
        previous_len = len(self._tracked)
        self._tracked = tracked
        if not tracked:
            self._cursor = 0
        elif previous_len != len(tracked):
            self._cursor = self._cursor % len(tracked)
        self._persist_cursor()
        self._save_state()

    def run_once(self, now_ts: Optional[int] = None) -> Dict[str, Any]:
        now_ts = int(now_ts or time.time())
        stats: Dict[str, Any] = {
            "candidates_total": len(self._tracked),
            "processed": 0,
            "processed_addrs": 0,
            "skipped": 0,
            "errors": 0,
            "alerts_sent": 0,
            "dump_alerts": 0,
            "hot_tokens": 0,
            "new_tokens": 0,
            "cache_hits": 0,
            "api_calls": 0,
            "cursor_before": self._cursor,
            "cursor_after": self._cursor,
        }
        result: Dict[str, Any] = {
            "ran": False,
            "reason": None,
            "alerts": [],
            "dump_events": [],
            "hot_tokens": [],
            "stats": stats,
        }
        if not self.enabled:
            result["reason"] = "disabled"
            return result
        if not self.covalent_client or not self.notifier:
            result["reason"] = "not_configured"
            return result
        if not self._tracked:
            result["reason"] = "no_candidates"
            return result
        last_run = int(self._internal_state.get("last_run_ts") or 0)
        if now_ts - last_run < self.interval_seconds:
            result["reason"] = "interval_not_elapsed"
            return result

        sample = self._pick_addresses(now_ts)
        stats["processed"] = len(sample)
        stats["processed_addrs"] = len(sample)
        stats["cursor_after"] = self._cursor
        result["ran"] = True
        result["reason"] = None

        address_payloads: List[Dict[str, Any]] = []
        hot_tokens: List[Dict[str, Any]] = []
        new_tokens_per_actor: Dict[str, int] = {}

        for meta in sample:
            try:
                agg, diag = self._scan_address(meta, now_ts)
            except Exception as exc:  # pragma: no cover
                self.logger.error("Address scan failed for %s: %s", meta.get("address"), exc)
                stats["errors"] += 1
                stats["skipped"] += 1
                continue
            diag = diag or {}
            stats["cache_hits"] += diag.get("cache_hits", 0)
            stats["api_calls"] += diag.get("api_calls", 0)
            stats["errors"] += diag.get("errors", 0)
            if not agg:
                stats["skipped"] += 1
                continue
            address_payloads.append(agg)
            new_tokens_per_actor[meta["address"]] = agg.get("new_tokens_count", 0)
            stats["new_tokens"] += agg.get("new_tokens_count", 0)
            for entry in agg.get("hot_token_entries", []):
                data = {
                    "token_key": entry["token_key"],
                    "weight": entry["weight"],
                    "addr": meta["address"],
                    "initiator_score": meta.get("initiator_score") or 0.0,
                    "new_token": entry["new_token"],
                    "tx_count": entry["tx_count"],
                    "usd_volume": entry["usd_volume"],
                    "symbol": entry["symbol"],
                }
                hot_tokens.append(data)

            dump_entry = agg.get("dump_candidate")
            if dump_entry and self._handle_dump_alert(meta, agg, dump_entry, now_ts, result):
                stats["dump_alerts"] += 1

            trust_value = float(agg.get("trust") or 0.0)
            trust_info = agg.get("trust_info") or {}
            agg["trust_info"] = trust_info
            effective_cd = min(self.cooldown_actor_activity, self._cooldown_for_trust(trust_value))

            if not agg.get("meets_alert_threshold"):
                continue

            event_token_key = agg.get("primary_token_key") or ""
            event_key = f"act:{meta['address']}:{event_token_key}:{self._event_bucket(now_ts)}"
            if self.state:
                if event_token_key:
                    setup_key = f"setup:{meta['address'].lower()}:{event_token_key}:{self._event_bucket(now_ts)}"
                    if seen_event(self.state, setup_key):
                        continue
                if too_soon_actor_activity(
                    self.state,
                    address=meta["address"],
                    cooldown_sec=effective_cd,
                    now_ts=now_ts,
                ):
                    continue
                if seen_event(self.state, event_key):
                    continue

            message = self._format_alert_message(agg)
            try:
                self.notifier.send_message(
                    message,
                    alert_kind="actor_activity",
                    address=meta["address"],
                )
            except Exception as exc:  # pragma: no cover
                self.logger.error("Failed to send actor activity alert for %s: %s", meta["address"], exc)
                continue

            self.logger.info(
                "Actor activity trust: addr=%s base=%.2f trust=%.2f final=%.2f cooldown=%ss",
                meta["address"],
                agg.get("base_score", 0.0),
                trust_value,
                agg.get("final_score", agg.get("base_score", 0.0)),
                effective_cd,
            )
            stats["alerts_sent"] += 1
            result["alerts"].append(
                {
                    "address": meta["address"],
                    "token_key": event_token_key,
                    "tx_count": agg["tx_count"],
                    "usd_volume": agg["usd_volume"],
                }
            )
            if self.state is not None:
                record_actor_activity_alert(self.state, meta["address"], now_ts)
                remember_event(self.state, event_key, now_ts)

        stats["hot_tokens"] = len(hot_tokens)
        self._write_activity_artifact(address_payloads, now_ts)
        self._append_history_line(address_payloads, now_ts)

        self._internal_state["last_run_ts"] = now_ts
        self._save_state()

        result["hot_tokens"] = hot_tokens
        result["stats"] = stats
        result["new_tokens_per_actor"] = new_tokens_per_actor
        return result

    def _pick_addresses(self, now_ts: int) -> List[Dict[str, Any]]:
        total = len(self._tracked)
        if total == 0:
            return []
        count = self.max_addrs_per_run or self.max_addresses_per_tick or 1
        count = max(1, min(count, total))
        out: List[Dict[str, Any]] = []
        for _ in range(count):
            out.append(self._tracked[self._cursor])
            self._cursor = (self._cursor + 1) % total
            if self._cursor == 0:
                self._maybe_shuffle_tracked(now_ts)
        self._persist_cursor()
        return out

    def update_trust_map(self, trust_map: Optional[Dict[str, Any]]) -> None:
        self.trust_map = {str(addr).lower(): data for addr, data in (trust_map or {}).items()}

    def _scan_address(self, meta: Dict[str, Any], now_ts: int) -> Tuple[Optional[Dict[str, Any]], Dict[str, int]]:
        chain = meta.get("chain") or "bsc"
        lookback_seconds = self.window_minutes * 60
        start_ts = max(0, now_ts - lookback_seconds)
        diag: Dict[str, int] = {}
        try:
            txs, diag = self.covalent_client.iter_transactions_window(
                chain=chain,
                address=meta["address"],
                start_ts=start_ts,
                end_ts=now_ts,
            )
        except Exception as exc:
            self.logger.error("Covalent window fetch failed for %s (%s): %s", meta["address"], chain, exc)
            error_diag = diag or {}
            error_diag["errors"] = error_diag.get("errors", 0) + 1
            return None, error_diag
        if not txs:
            return None, diag or {}
        agg = self._aggregate_transactions(meta, txs, now_ts)
        return agg, diag or {}

    def _aggregate_transactions(
        self,
        meta: Dict[str, Any],
        txs: List[Dict[str, Any]],
        seen_ts: int,
    ) -> Dict[str, Any]:
        tx_count = len(txs)
        token_stats: Dict[str, Dict[str, Any]] = {}
        total_volume = 0.0
        total_buy = 0.0
        total_sell = 0.0
        actor_addr = meta["address"]
        for tx in txs:
            contract = tx.get("token_address") or tx.get("contract_address")
            symbol = (tx.get("token_symbol") or tx.get("contract_ticker_symbol") or "") or ""
            token_key = _token_key(meta.get("chain", "bsc"), contract, symbol)
            symbol_display = symbol.upper() if symbol else (contract or "UNKNOWN")
            key = token_key or symbol_display
            bucket = token_stats.setdefault(
                key,
                {
                    "token_key": token_key,
                    "symbol": symbol_display,
                    "contract": (contract or "").lower() or None,
                    "tx_count": 0,
                    "usd_volume": 0.0,
                    "buy_usd": 0.0,
                    "sell_usd": 0.0,
                },
            )
            bucket["tx_count"] += 1
            usd = float(tx.get("usd_value") or tx.get("value_quote") or 0.0)
            if usd < 0:
                usd = abs(usd)
            bucket["usd_volume"] += usd
            total_volume += usd
            direction = self._tx_direction(tx, actor_addr)
            if direction == "sell":
                bucket["sell_usd"] += usd
                total_sell += usd
            elif direction == "buy":
                bucket["buy_usd"] += usd
                total_buy += usd

        hot_entries: List[Dict[str, Any]] = []
        new_tokens_count = 0
        for stats in token_stats.values():
            token_key = stats["token_key"] or _token_key(meta.get("chain", "bsc"), stats.get("contract"), stats["symbol"])
            if not token_key:
                continue
            is_new = is_new_token_for_actor(
                self.state or {},
                address=actor_addr,
                token_key=token_key,
                now_ts=seen_ts,
                history_days=self.history_days,
                min_first_seen_minutes_ago=self.min_first_seen_minutes,
            )
            if is_new:
                new_tokens_count += 1
            if self.state is not None:
                remember_actor_token(
                    self.state,
                    address=actor_addr,
                    token_key=token_key,
                    ts=seen_ts,
                    max_tokens_per_actor_history=self.max_token_history,
                )
            tx_share = stats["tx_count"] / max(1, tx_count)
            vol_share = stats["usd_volume"] / max(1.0, total_volume)
            weight = max(0.0, min(1.0, 0.4 * tx_share + 0.6 * vol_share))
            stats["token_key"] = token_key
            stats["new_token"] = is_new
            stats["weight"] = weight
            token_flow = stats["buy_usd"] + stats["sell_usd"]
            stats["sell_ratio"] = stats["sell_usd"] / max(1.0, token_flow)
            prev_ratio = self._get_previous_sell_ratio(actor_addr, token_key)
            stats["prev_sell_ratio"] = prev_ratio
            stats["sell_ratio_delta"] = (
                stats["sell_ratio"] - prev_ratio if isinstance(prev_ratio, (int, float)) else stats["sell_ratio"]
            )
            stats["exit_wave_score"] = 0.0
            hot_entries.append(stats)

        hot_entries.sort(key=lambda entry: entry["usd_volume"], reverse=True)
        summary_tokens = [
            {"symbol": entry["symbol"], "usd_volume": entry["usd_volume"], "tx_count": entry["tx_count"]}
            for entry in hot_entries[: self.alert_top_tokens]
        ]
        top_symbol_names = [token["symbol"] for token in summary_tokens]
        for entry in hot_entries:
            entry["top_symbols"] = top_symbol_names

        meets_threshold_base = total_volume >= self.min_total_usd_volume or tx_count >= self.min_tx_count
        base_score = max(
            total_volume / max(1.0, self.min_total_usd_volume),
            tx_count / max(1, self.min_tx_count),
        )
        trust_entry = self.trust_map.get(actor_addr.lower()) or {}
        trust_value = float(trust_entry.get("trust") or 0.0)
        final_score = base_score * (1.0 + self.trust_weight * trust_value)
        trust_gate = trust_value < self.trust_min and base_score < self.trust_override_score
        meets_threshold = meets_threshold_base and not trust_gate
        trust_info = dict(trust_entry)
        trust_info.setdefault("trust", trust_value)
        primary_token_key = hot_entries[0]["token_key"] if hot_entries else None
        dump_candidate = self._select_dump_candidate(meta, hot_entries, total_volume, tx_count, seen_ts)
        status = "EXIT" if dump_candidate else "ACTIVE"
        self._record_token_observation(actor_addr, hot_entries, seen_ts)

        return {
            "address": actor_addr,
            "chain": meta.get("chain") or "bsc",
            "tx_count": tx_count,
            "usd_volume": total_volume,
            "buy_usd": total_buy,
            "sell_usd": total_sell,
            "sell_ratio": total_sell / max(1.0, total_buy + total_sell),
            "top_tokens": summary_tokens,
            "window_minutes": self.window_minutes,
            "seen_at": seen_ts,
            "source_tags": meta.get("source_tags") or [],
            "actor_score": meta.get("actor_score"),
            "initiator_score": meta.get("initiator_score"),
            "campaigns_count": meta.get("campaigns_count"),
            "status": status,
            "kind": status,
            "stage": status,
            "meets_alert_threshold": meets_threshold,
            "hot_token_entries": hot_entries,
            "primary_token_key": primary_token_key,
            "new_tokens_count": new_tokens_count,
            "dump_candidate": dump_candidate,
            "trust": trust_value,
            "base_score": base_score,
            "final_score": final_score,
            "trust_info": trust_info,
        }

    def _get_previous_sell_ratio(self, addr: str, token_key: Optional[str]) -> Optional[float]:
        if not self.state or not addr or not token_key:
            return None
        entry = get_actor_token_stage(self.state, addr, token_key)
        if not entry:
            return None
        last = entry.get("last") or {}
        value = last.get("sell_ratio")
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def _record_token_observation(self, addr: str, entries: List[Dict[str, Any]], now_ts: int) -> None:
        if not self.state or not entries:
            return
        updated = False
        for entry in entries:
            token_key = entry.get("token_key")
            if not token_key:
                continue
            stage_info = get_actor_token_stage(self.state, addr, token_key) or {}
            stage_name = stage_info.get("stage")
            sell_ratio = float(entry.get("sell_ratio") or 0.0)
            if entry.get("new_token") and sell_ratio <= self.setup_sell_ratio_threshold:
                stage_name = "SETUP"
            payload = {
                "sell_ratio": sell_ratio,
                "sell_ratio_prev": entry.get("prev_sell_ratio"),
                "sell_ratio_delta": entry.get("sell_ratio_delta"),
                "exit_wave_score": entry.get("exit_wave_score"),
                "window_minutes": self.window_minutes,
                "tx_count": entry.get("tx_count"),
                "usd_volume": entry.get("usd_volume"),
                "updated_at": now_ts,
            }
            set_actor_token_stage(
                self.state,
                addr=addr,
                token_key=token_key,
                stage=stage_name,
                now_ts=now_ts,
                last=payload,
            )
            updated = True
        if updated:
            prune_actor_token_stage(self.state)
    
    def _mark_exit_stage(self, addr: str, token_key: Optional[str], dump_entry: Dict[str, Any], now_ts: int) -> None:
        if not self.state or not token_key or not addr:
            return
        payload = {
            "sell_ratio": dump_entry.get("sell_ratio"),
            "sell_ratio_delta": dump_entry.get("sell_ratio_delta"),
            "exit_wave_score": dump_entry.get("exit_wave_score"),
            "window_minutes": self.window_minutes,
            "tx_count": dump_entry.get("tx_count"),
            "usd_volume": dump_entry.get("usd_volume"),
            "kind": "dumpwave",
        }
        set_actor_token_stage(
            self.state,
            addr=addr,
            token_key=token_key,
            stage="EXIT",
            now_ts=now_ts,
            last=payload,
        )
        prune_actor_token_stage(self.state)

    def _lookup_exit_signature(self, addr: Optional[str]) -> Optional[Dict[str, Any]]:
        if not addr:
            return None
        key = str(addr).lower()
        if key in self.exit_signatures:
            return self.exit_signatures[key]
        chain_key = f"bsc:{key}"
        return self.exit_signatures.get(chain_key)

    def _select_dump_candidate(
        self,
        meta: Dict[str, Any],
        hot_entries: List[Dict[str, Any]],
        total_volume: float,
        tx_count: int,
        now_ts: int,
    ) -> Optional[Dict[str, Any]]:
        if (
            not self.dump_enabled
            or (total_volume < self.dump_min_total and tx_count < self.dump_min_tx)
        ):
            return None
        best: Optional[Dict[str, Any]] = None
        initiator_score = float(meta.get("initiator_score") or 0.0)
        actor_score = float(meta.get("actor_score") or 0.0)
        score_for_check = initiator_score if initiator_score > 1 else actor_score
        for entry in hot_entries:
            sell_ratio = entry.get("sell_ratio", 0.0)
            prev_ratio = entry.get("prev_sell_ratio")
            delta = sell_ratio - (prev_ratio if isinstance(prev_ratio, (int, float)) else 0.0)
            entry["sell_ratio_delta"] = delta
            entry["exit_wave_score"] = self._exit_wave_score(
                sell_ratio=sell_ratio,
                sell_usd=entry.get("sell_usd", 0.0),
                tx_count=entry.get("tx_count", 0),
                sell_ratio_delta=delta,
            )
            if entry.get("new_token"):
                continue
            if entry.get("tx_count", 0) < self.dump_min_tx:
                continue
            if entry.get("usd_volume", 0.0) < self.dump_min_total:
                continue
            if entry.get("sell_usd", 0.0) < self.dump_min_sell_usd:
                continue
            if sell_ratio < self.dump_min_sell_ratio:
                continue
            if self.dump_require_initiator and score_for_check < self.dump_min_score:
                continue
            if self.exit_wave_enabled and entry["exit_wave_score"] < self.exit_wave_score_threshold:
                continue
            if not best or entry["sell_ratio"] > best.get("sell_ratio", 0.0):
                best = entry
        if not best:
            return None
        return {
            "token_key": best.get("token_key"),
            "symbol": best.get("symbol"),
            "sell_usd": best.get("sell_usd", 0.0),
            "buy_usd": best.get("buy_usd", 0.0),
            "sell_ratio": best.get("sell_ratio", 0.0),
            "tx_count": best.get("tx_count", 0),
            "usd_volume": best.get("usd_volume", 0.0),
            "new_token": best.get("new_token"),
            "top_symbols": best.get("top_symbols"),
            "sell_ratio_delta": best.get("sell_ratio_delta"),
            "exit_wave_score": best.get("exit_wave_score"),
            "stage": "EXIT",
            "seen_at": now_ts,
        }

    def _exit_wave_score(
        self,
        *,
        sell_ratio: float,
        sell_usd: float,
        tx_count: int,
        sell_ratio_delta: float,
    ) -> float:
        base = self._clamp(
            (sell_ratio - self.dump_min_sell_ratio) / max(1e-3, 1.0 - self.dump_min_sell_ratio)
        )
        vol = self._clamp(sell_usd / max(1.0, self.dump_min_sell_usd))
        tx_norm = self._clamp(tx_count / max(1, self.dump_min_tx))
        accel = self._clamp(
            sell_ratio_delta / max(self.exit_wave_accel_threshold or 0.01, 0.01)
        )
        return self._clamp(0.35 * base + 0.25 * vol + 0.20 * tx_norm + 0.20 * accel)

    def _write_activity_artifact(self, items: List[Dict[str, Any]], now_ts: int) -> None:
        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
            "window_minutes": self.window_minutes,
            "items": items,
        }
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(self.warehouse_path, json.dumps(payload, ensure_ascii=False, indent=2))

    def _append_history_line(self, items: List[Dict[str, Any]], now_ts: int) -> None:
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
                "window_minutes": self.window_minutes,
                "items": items,
            },
            ensure_ascii=False,
        )
        with self.history_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _handle_dump_alert(
        self,
        meta: Dict[str, Any],
        summary: Dict[str, Any],
        dump_entry: Dict[str, Any],
        now_ts: int,
        result: Dict[str, Any],
    ) -> bool:
        if not self.dump_enabled:
            return False
        token_key = dump_entry.get('token_key') or dump_entry.get('symbol')
        if not token_key:
            return False
        bucket = self.dump_bucket_seconds or 1800
        event_key = f"dumpwave:{meta['address'].lower()}:{token_key}:{int(now_ts) // bucket}"
        if self.state and seen_event(self.state, event_key):
            return False
        baseline = self._lookup_exit_signature(meta["address"])
        if baseline:
            dump_entry["baseline"] = baseline
            summary["baseline"] = baseline
        summary["status"] = "EXIT"
        summary["kind"] = "dumpwave"
        summary["exit_wave_score"] = dump_entry.get("exit_wave_score")
        summary["sell_ratio_delta"] = dump_entry.get("sell_ratio_delta")
        message = self._format_dump_alert_message(summary, dump_entry)
        try:
            self.notifier.send_message(
                message,
                alert_kind="actor_dump",
                address=meta["address"],
            )
        except Exception as exc:  # pragma: no cover
            self.logger.error('Failed to send dump alert for %s: %s', meta['address'], exc)
            return False
        if self.state is not None:
            remember_event(self.state, event_key, now_ts)
            self._mark_exit_stage(
                meta["address"],
                token_key,
                dump_entry,
                now_ts,
            )
        result.setdefault('dump_events', []).append(
            {
                'address': meta['address'],
                'token_key': token_key,
                'sell_usd': dump_entry.get('sell_usd', 0.0),
                'sell_ratio': dump_entry.get('sell_ratio', 0.0),
                'exit_wave_score': dump_entry.get('exit_wave_score'),
                'stage': 'EXIT',
            }
        )
        return True

    def _format_alert_message(self, alert: Dict[str, Any]) -> str:
        addr = alert['address']
        short_addr = _short_addr(addr)
        volume = alert.get('usd_volume') or 0.0
        tx_count = alert.get('tx_count') or 0
        tokens = alert.get('top_tokens') or []
        if tokens:
            top_tokens_line = ', '.join(
                f"{token.get('symbol','?')} (~${token.get('usd_volume', 0):,.0f})" for token in tokens
            )
        else:
            top_tokens_line = 'n/a'
        initiator_score = alert.get('initiator_score')
        score_text = f"{float(initiator_score):.2f}" if isinstance(initiator_score, (int, float)) else 'n/a'
        campaigns = alert.get('campaigns_count')
        campaigns_text = str(int(campaigns)) if isinstance(campaigns, (int, float)) else 'n/a'
        prefix = self.ping_prefix or '👀'
        trust_line = self._format_trust_line(alert.get("trust_info"))
        lines = [
            f"{prefix} Actor activity spike ({self.window_minutes}m)",
            f"Address: {short_addr} | chain={alert.get('chain','?')} | init_score={score_text} | campaigns={campaigns_text}",
            f"Tx: {tx_count} | Volume≈${volume:,.0f}",
            f"Top tokens: {top_tokens_line}",
            f"Status: {alert.get('status','ACTIVE')}",
        ]
        if trust_line:
            lines.append(trust_line)
        return "\n".join(lines)

    def _format_dump_alert_message(self, summary: Dict[str, Any], dump_entry: Dict[str, Any]) -> str:
        addr = summary['address']
        short_addr = _short_addr(addr)
        initiator_score = summary.get('initiator_score')
        score_text = f"{float(initiator_score):.2f}" if isinstance(initiator_score, (int, float)) else 'n/a'
        campaigns = summary.get('campaigns_count')
        campaigns_text = str(int(campaigns)) if isinstance(campaigns, (int, float)) else 'n/a'
        tokens = summary.get('top_tokens') or []
        if tokens:
            top_tokens_line = ', '.join(
                f"{t.get('symbol','?')} (~${t.get('usd_volume',0):,.0f})" for t in tokens
            )
        else:
            top_tokens_line = 'n/a'
        sell_ratio = float(dump_entry.get('sell_ratio') or 0.0)
        sell_usd = dump_entry.get('sell_usd') or 0.0
        buy_usd = dump_entry.get('buy_usd') or 0.0
        symbol = dump_entry.get('symbol') or '?'
        token_key = dump_entry.get('token_key') or symbol
        volume = dump_entry.get('usd_volume') or 0.0
        tx_count = dump_entry.get('tx_count') or summary.get('tx_count') or 0
        stage = dump_entry.get("stage") or summary.get("status") or "EXIT"
        score = dump_entry.get("exit_wave_score")
        delta = dump_entry.get("sell_ratio_delta")
        baseline = dump_entry.get("baseline") or summary.get("baseline")
        lines = [
            '👎 Initiator dump (exit wave)',
            f"Address: {short_addr} | chain={summary.get('chain','?')} | init_score={score_text} | campaigns={campaigns_text}",
            f"Token: {symbol} ({token_key})",
            f"Sell?${sell_usd:,.0f} | Buy?${buy_usd:,.0f} | ratio={sell_ratio*100:.0f}%",
            f"Tx: {tx_count} | Volume?${volume:,.0f}",
            f"Top tokens: {top_tokens_line}",
            f"Stage: {stage}",
        ]
        if isinstance(score, (int, float)):
            lines.append(f"Exit wave score: {score:.2f}")
        if isinstance(delta, (int, float)):
            lines.append(f"Selling acceleration Δ={delta*100:.0f}pp")
        if isinstance(baseline, dict):
            exit_sec = baseline.get("exit_speed_p50_seconds")
            exit_minutes = f"{exit_sec/60:.1f}m" if isinstance(exit_sec, (int, float)) and exit_sec > 0 else "n/a"
            dump_ratio = baseline.get("dump_ratio_p50")
            ratio_text = f"{dump_ratio:.2f}x" if isinstance(dump_ratio, (int, float)) else "n/a"
            lines.append(
                f"Baseline exit: p50≈{exit_minutes} | dump≈{ratio_text} | campaigns={baseline.get('campaigns','?')}"
            )
        trust_line = self._format_trust_line(summary.get("trust_info"))
        if trust_line:
            lines.append(trust_line)
        lines.append('Режим: PING')
        return "\n".join(lines)

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
        return f"Trust: {float(trust_val):.2f} | last pump: {realized_text} | pressure: {pressure_text} | reason: {reason}"

    def _event_bucket(self, ts: int) -> int:
        return int(ts // self.event_bucket_seconds)

    @staticmethod
    def _tx_direction(tx: Dict[str, Any], wallet: str) -> Optional[str]:
        direction = str(tx.get("direction") or tx.get("tx_direction") or "").lower()
        if direction in ("in", "buy", "incoming"):
            return "buy"
        if direction in ("out", "sell", "outgoing"):
            return "sell"
        wallet_norm = (wallet or "").lower()
        from_addr = str(tx.get("from_address") or tx.get("from") or "").lower()
        to_addr = str(tx.get("to_address") or tx.get("to") or "").lower()
        if wallet_norm and from_addr == wallet_norm:
            return "sell"
        if wallet_norm and to_addr == wallet_norm:
            return "buy"
        return None

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, value))

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

    def _maybe_shuffle_tracked(self, now_ts: int) -> None:
        if not self.shuffle_each_cycle or not self._tracked:
            return
        cycle_id = time.strftime("%Y%m%d", time.gmtime(now_ts))
        if self._cycle_marker == cycle_id:
            return
        rng = random.Random(cycle_id)
        rng.shuffle(self._tracked)
        self._cursor = 0
        self._set_cycle_marker(cycle_id)

    def _load_cursor(self) -> int:
        if self.state is not None:
            value = self.state.get(self._state_cursor_key)
            if isinstance(value, int):
                return max(0, value)
            try:
                return max(0, int(value))
            except Exception:
                pass
        value = self._internal_state.get("cursor")
        if isinstance(value, int):
            return max(0, value)
        try:
            return max(0, int(value))
        except Exception:
            return 0

    def _persist_cursor(self) -> None:
        cursor_value = max(0, int(self._cursor))
        self._internal_state["cursor"] = cursor_value
        if self.state is not None:
            self.state[self._state_cursor_key] = cursor_value
            self.state[self._state_cycle_key] = self._cycle_marker

    def _load_cycle_marker(self) -> str:
        marker = ""
        if self.state is not None:
            raw = self.state.get(self._state_cycle_key)
            if isinstance(raw, (int, str)):
                marker = str(raw)
        if not marker:
            raw = self._internal_state.get("last_cycle_id")
            if isinstance(raw, (int, str)):
                marker = str(raw)
        return marker or ""

    def _set_cycle_marker(self, marker: str) -> None:
        marker_str = str(marker or "")
        self._cycle_marker = marker_str
        self._internal_state["last_cycle_id"] = marker_str
        if self.state is not None:
            self.state[self._state_cycle_key] = marker_str

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"last_run_ts": 0, "cursor": 0}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {"last_run_ts": 0, "cursor": 0}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(self.state_path, json.dumps(self._internal_state, ensure_ascii=False, indent=2))

import json
import logging
import os
import hashlib
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_STATE = {
    "last_symbol_alert_ts": {},
    "last_actor_alert_ts": {},
    "last_sent_hashes": [],
    "actor_recent_symbols": {},
    "symbol_recent_actors": {},
    "last_symbol_ping_ts": {},
    "last_symbol_alarm_ts": {},
    "last_actor_activity_alert_ts": {},
    "cluster_recent_symbols": {},
    "symbol_recent_clusters": {},
    "last_cluster_spree_ts": {},
    "actor_recent_tokens": {},
    "token_recent_actors": {},
    "last_setup_alert_ts": {},
    "recent_events": {},
    "alerts_per_hour": {},
    "actor_activity_cursor": 0,
    "actor_activity_last_cycle_id": "",
    "actor_token_stage": {},
}


def _atomic_write(path: str, content: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


def load_state(path: str = "data/hunter_state.json", logger: logging.Logger | None = None) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {**DEFAULT_STATE}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state is not dict")
        # merge defaults
        merged = {**DEFAULT_STATE, **data}
        for k in DEFAULT_STATE:
            merged.setdefault(k, DEFAULT_STATE[k])
        _migrate_recent_maps(merged)
        return merged
    except Exception as exc:  # pragma: no cover
        if logger:
            logger.warning("State load failed, starting fresh: %s", exc)
        return {**DEFAULT_STATE}


def save_state(state: Dict[str, Any], path: str = "data/hunter_state.json") -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(path, json.dumps(state, ensure_ascii=False, indent=2))


def _hash_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def seen_message(state: Dict[str, Any], message_text: str) -> bool:
    hashes: List[str] = state.get("last_sent_hashes") or []
    h = _hash_text(message_text)
    return h in hashes


def remember_alert(
    state: Dict[str, Any],
    *,
    symbol: str,
    actor_addrs: list[str],
    message_text: str,
    now_ts: int,
    max_hashes: int = 300,
    alert_kind: str = "alarm",
    cluster_ids: Optional[List[str]] = None,
) -> None:
    sym = symbol.upper()
    state.setdefault("last_symbol_alert_ts", {})[sym] = now_ts
    if alert_kind == "ping":
        state.setdefault("last_symbol_ping_ts", {})[sym] = now_ts
    elif alert_kind == "alarm":
        state.setdefault("last_symbol_alarm_ts", {})[sym] = now_ts
    hashes: List[str] = state.get("last_sent_hashes") or []
    h = _hash_text(message_text)
    hashes.append(h)
    if len(hashes) > max_hashes:
        hashes = hashes[-max_hashes:]
    state["last_sent_hashes"] = hashes
    las = state.setdefault("last_actor_alert_ts", {})
    ars = state.setdefault("actor_recent_symbols", {})
    sra = state.setdefault("symbol_recent_actors", {})
    for addr in actor_addrs:
        if not addr:
            continue
        la = addr.lower()
        las[la] = now_ts
        sym_map = ars.get(la)
        if isinstance(sym_map, list):
            sym_map = {s: now_ts for s in sym_map}
        if not isinstance(sym_map, dict):
            sym_map = {}
        sym_map[sym] = now_ts
        if len(sym_map) > 50:
            _trim_old_entries(sym_map, 50)
        ars[la] = sym_map

        symbol_map = sra.get(sym)
        if not isinstance(symbol_map, dict):
            symbol_map = {}
        symbol_map[la] = now_ts
        if len(symbol_map) > 50:
            _trim_old_entries(symbol_map, 50)
        sra[sym] = symbol_map
    crs = state.setdefault("cluster_recent_symbols", {})
    src = state.setdefault("symbol_recent_clusters", {})
    for cid in cluster_ids or []:
        if not cid:
            continue
        cluster = str(cid)
        sym_map = crs.get(cluster)
        if not isinstance(sym_map, dict):
            sym_map = {}
        sym_map[sym] = now_ts
        if len(sym_map) > 50:
            _trim_old_entries(sym_map, 50)
        crs[cluster] = sym_map

        symbol_map = src.get(sym)
        if not isinstance(symbol_map, dict):
            symbol_map = {}
        symbol_map[cluster] = now_ts
        if len(symbol_map) > 50:
            _trim_old_entries(symbol_map, 50)
        src[sym] = symbol_map


def remember_actor_token(
    state: Dict[str, Any],
    *,
    address: str,
    token_key: str,
    ts: int,
    max_tokens_per_actor_history: int,
) -> None:
    if not address or not token_key:
        return
    actor = address.lower()
    art = state.setdefault("actor_recent_tokens", {})
    bucket = art.get(actor)
    if not isinstance(bucket, dict):
        bucket = {}
    bucket[token_key] = ts
    _trim_old_entries(bucket, max_tokens_per_actor_history)
    art[actor] = bucket

    tra = state.setdefault("token_recent_actors", {})
    token_bucket = tra.get(token_key)
    if not isinstance(token_bucket, dict):
        token_bucket = {}
    token_bucket[actor] = ts
    _trim_old_entries(token_bucket, 200)
    tra[token_key] = token_bucket


def is_new_token_for_actor(
    state: Dict[str, Any],
    *,
    address: str,
    token_key: str,
    now_ts: int,
    history_days: int,
    min_first_seen_minutes_ago: int,
) -> bool:
    if not address or not token_key:
        return False
    history = state.get("actor_recent_tokens") or {}
    bucket = history.get(address.lower()) or {}
    last_ts = bucket.get(token_key)
    if last_ts is None:
        return True
    if now_ts - last_ts >= max(1, history_days) * 86400:
        return True
    if now_ts - last_ts <= max(0, min_first_seen_minutes_ago) * 60:
        return False
    return False


def too_soon(state: Dict[str, Any], *, symbol: str, cooldown_sec: int, now_ts: int) -> bool:
    last_ts = (state.get("last_symbol_alert_ts") or {}).get(symbol.upper())
    if last_ts is None:
        return False
    return (now_ts - last_ts) < cooldown_sec


def too_soon_ping(state: Dict[str, Any], *, symbol: str, cooldown_sec: int, now_ts: int) -> bool:
    last_ts = (state.get("last_symbol_ping_ts") or {}).get(symbol.upper())
    if last_ts is None:
        return False
    return (now_ts - last_ts) < cooldown_sec


def too_soon_alarm(state: Dict[str, Any], *, symbol: str, cooldown_sec: int, now_ts: int) -> bool:
    last_ts = (state.get("last_symbol_alarm_ts") or {}).get(symbol.upper())
    if last_ts is None:
        return False
    return (now_ts - last_ts) < cooldown_sec


def too_soon_actor_activity(state: Dict[str, Any], *, address: str, cooldown_sec: int, now_ts: int) -> bool:
    last_ts = (state.get("last_actor_activity_alert_ts") or {}).get(address.lower())
    if last_ts is None:
        return False
    return (now_ts - last_ts) < cooldown_sec


def record_actor_activity_alert(state: Dict[str, Any], addr: str, now_ts: int) -> None:
    state.setdefault("last_actor_activity_alert_ts", {})[addr.lower()] = now_ts


def too_soon_cluster_spree(state: Dict[str, Any], *, cluster_id: str, cooldown_sec: int, now_ts: int) -> bool:
    last_ts = (state.get("last_cluster_spree_ts") or {}).get(cluster_id)
    if last_ts is None:
        return False
    return (now_ts - last_ts) < cooldown_sec


def record_cluster_spree(state: Dict[str, Any], cluster_id: str, now_ts: int) -> None:
    state.setdefault("last_cluster_spree_ts", {})[cluster_id] = now_ts


def can_send_alert(state: Dict[str, Any], now_ts: float, max_per_hour: int) -> bool:
    if max_per_hour <= 0:
        return True
    bucket = str(int(now_ts) // 3600)
    counters = state.setdefault("alerts_per_hour", {})
    count = counters.get(bucket, 0)
    return count < max_per_hour


def record_alert_sent(state: Dict[str, Any], now_ts: float, max_buckets: int = 48) -> None:
    bucket = str(int(now_ts) // 3600)
    counters = state.setdefault("alerts_per_hour", {})
    counters[bucket] = counters.get(bucket, 0) + 1
    if len(counters) > max_buckets:
        for key, _ in sorted(counters.items(), key=lambda kv: int(kv[0]))[: len(counters) - max_buckets]:
            counters.pop(key, None)


def seen_event(state: Dict[str, Any], key: str) -> bool:
    return key in (state.get("recent_events") or {})


def remember_event(state: Dict[str, Any], key: str, ts: int, max_events: int = 2000) -> None:
    events = state.setdefault("recent_events", {})
    events[key] = ts
    if len(events) > max_events:
        overflow = len(events) - max_events
        for ev_key, _ in sorted(events.items(), key=lambda kv: kv[1])[:overflow]:
            events.pop(ev_key, None)


def _stage_token_key(token_key: Optional[str]) -> Optional[str]:
    if not token_key:
        return None
    token_key = str(token_key).strip()
    if not token_key:
        return None
    if ":" not in token_key:
        return f"symbol:{token_key.upper()}"
    prefix, value = token_key.split(":", 1)
    prefix = prefix.lower()
    if prefix.endswith("_symbol"):
        prefix = "symbol"
    if prefix == "symbol":
        return f"symbol:{value.upper()}"
    return f"{prefix}:{value.lower()}"


def set_actor_token_stage(
    state: Dict[str, Any],
    addr: str,
    token_key: Optional[str],
    stage: Optional[str],
    now_ts: int,
    last: Optional[Dict[str, Any]] = None,
    max_per_actor: int = 50,
) -> None:
    if not state or not addr or not token_key:
        return
    norm_key = _stage_token_key(token_key)
    if not norm_key:
        return
    bucket = state.setdefault("actor_token_stage", {})
    actor_key = addr.lower()
    actor_bucket = bucket.get(actor_key)
    if not isinstance(actor_bucket, dict):
        actor_bucket = {}
    entry = actor_bucket.get(norm_key) or {}
    if stage:
        entry["stage"] = stage
    entry["ts"] = now_ts
    if last:
        entry_last = dict(entry.get("last") or {})
        entry_last.update(last)
        entry["last"] = entry_last
    actor_bucket[norm_key] = entry
    bucket[actor_key] = actor_bucket
    prune_actor_token_stage(state, max_per_actor=max_per_actor)


def get_actor_token_stage(
    state: Dict[str, Any],
    addr: str,
    token_key: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not state or not addr or not token_key:
        return None
    norm_key = _stage_token_key(token_key)
    if not norm_key:
        return None
    bucket = state.get("actor_token_stage") or {}
    actor_bucket = bucket.get(addr.lower())
    if not isinstance(actor_bucket, dict):
        return None
    entry = actor_bucket.get(norm_key)
    if isinstance(entry, dict):
        return entry
    return None


def prune_actor_token_stage(state: Dict[str, Any], max_per_actor: int = 50) -> None:
    if not state:
        return
    bucket = state.get("actor_token_stage")
    if not isinstance(bucket, dict):
        state["actor_token_stage"] = {}
        return
    to_delete = []
    for actor, tokens in bucket.items():
        if not isinstance(tokens, dict):
            to_delete.append(actor)
            continue
        if len(tokens) <= max_per_actor:
            continue
        entries = sorted(tokens.items(), key=lambda kv: kv[1].get("ts", 0))
        for key, _ in entries[: len(tokens) - max_per_actor]:
            tokens.pop(key, None)
    for actor in to_delete:
        bucket.pop(actor, None)


def _trim_old_entries(bucket: Dict[str, int], max_len: int) -> None:
    if len(bucket) <= max_len:
        return
    # remove oldest entries
    for key, _ in sorted(bucket.items(), key=lambda kv: kv[1])[: len(bucket) - max_len]:
        bucket.pop(key, None)


def _migrate_recent_maps(state: Dict[str, Any]) -> None:
    now_ts = int(time.time())
    ars = state.get("actor_recent_symbols") or {}
    if isinstance(ars, list):
        ars = {}
    if isinstance(ars, dict):
        new_map: Dict[str, Dict[str, int]] = {}
        for addr, value in ars.items():
            if isinstance(value, dict):
                cleaned = {str(sym).upper(): int(ts) for sym, ts in value.items() if isinstance(ts, int)}
                new_map[addr] = cleaned
            elif isinstance(value, list):
                cleaned = {str(sym).upper(): now_ts for sym in value}
                new_map[addr] = cleaned
        for addr in new_map:
            if len(new_map[addr]) > 50:
                _trim_old_entries(new_map[addr], 50)
        state["actor_recent_symbols"] = new_map
    else:
        state["actor_recent_symbols"] = {}

    sra = state.get("symbol_recent_actors")
    if not isinstance(sra, dict):
        sra = {}
    # rebuild symbol index from actor map to avoid stale structures
    rebuild: Dict[str, Dict[str, int]] = {}
    for addr, sym_map in state["actor_recent_symbols"].items():
        if not isinstance(sym_map, dict):
            continue
        for sym, ts in sym_map.items():
            bucket = rebuild.setdefault(sym, {})
            bucket[addr] = int(ts)
    for sym in rebuild:
        if len(rebuild[sym]) > 50:
            _trim_old_entries(rebuild[sym], 50)
    state["symbol_recent_actors"] = rebuild

    art = state.get("actor_recent_tokens")
    if not isinstance(art, dict):
        art = {}
    cleaned_art: Dict[str, Dict[str, int]] = {}
    for addr, bucket in art.items():
        if not isinstance(bucket, dict):
            continue
        cleaned_bucket: Dict[str, int] = {}
        for token_key, ts in bucket.items():
            if isinstance(ts, int):
                cleaned_bucket[str(token_key)] = ts
        cleaned_art[addr.lower()] = cleaned_bucket
    state["actor_recent_tokens"] = cleaned_art

    tra = state.get("token_recent_actors")
    if not isinstance(tra, dict):
        tra = {}
    cleaned_tra: Dict[str, Dict[str, int]] = {}
    for token_key, bucket in tra.items():
        if not isinstance(bucket, dict):
            continue
        cleaned_bucket = {}
        for addr, ts in bucket.items():
            if isinstance(ts, int):
                cleaned_bucket[str(addr).lower()] = ts
        cleaned_tra[str(token_key)] = cleaned_bucket
    state["token_recent_actors"] = cleaned_tra

    if not isinstance(state.get("last_setup_alert_ts"), dict):
        state["last_setup_alert_ts"] = {}
    if not isinstance(state.get("recent_events"), dict):
        state["recent_events"] = {}
    if not isinstance(state.get("alerts_per_hour"), dict):
        state["alerts_per_hour"] = {}
    if not isinstance(state.get("actor_activity_cursor"), int):
        try:
            state["actor_activity_cursor"] = int(state.get("actor_activity_cursor") or 0)
        except Exception:
            state["actor_activity_cursor"] = 0
    if not isinstance(state.get("actor_activity_last_cycle_id"), (str, int)):
        state["actor_activity_last_cycle_id"] = ""
    ats = state.get("actor_token_stage")
    if not isinstance(ats, dict):
        state["actor_token_stage"] = {}

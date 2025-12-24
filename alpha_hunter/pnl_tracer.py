import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from .actors_builder import _load_json  # reuse loader
from .covalent_client import CovalentClient
from .birdeye_client import BirdEyeClient

SYSTEM_DIR = os.path.join("data", "alpha_profiler", "system")
PROGRESS_PATH = os.path.join(SYSTEM_DIR, "pnl_progress.json")
WAREHOUSE_DIR = "data/alpha_profiler/warehouse"
CACHE_TX_DIR = "data/alpha_profiler/cache_tx"
RAW_CACHE_COV = "data/raw_cache/covalent"
RAW_CACHE_BE = "data/raw_cache/birdeye"
CACHE_VERSION = "2.0"
CACHE_HEALTH_PATH = "data/alpha_profiler/warehouse/cache_health.json"
TX_INDEX_DIR = os.path.join(WAREHOUSE_DIR, "tx_index")
TX_INDEX_RETENTION_DAYS = 7
TRUST_K_USD = 200.0
TRUST_K_TX = 30.0
TRUST_EWMA_DECAY = 0.85
TRUST_MAX_PUMPS = 10
ACCUMULATION_PRESSURE_USD = 1500.0
DISTRIBUTION_RATIO = 1.10


def _write_json_atomic(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)



def _tx_index_path(chain: str, address: str) -> str:
    return os.path.join(TX_INDEX_DIR, chain.lower(), f"{address.lower()}.jsonl")


def _tx_index_key(entry: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        entry.get("tx_hash"),
        entry.get("log_index"),
        entry.get("token_address"),
        entry.get("from"),
        entry.get("to"),
    )


def _compute_direction(wallet: str, from_addr: str, to_addr: str) -> str:
    wallet = (wallet or "").lower()
    if wallet and from_addr == wallet and to_addr != wallet:
        return "out"
    if wallet and to_addr == wallet and from_addr != wallet:
        return "in"
    return "unknown"


def _normalize_covalent_tx(tx: Dict[str, Any], wallet: str, chain: str) -> Dict[str, Any]:
    ts_iso = tx.get("block_signed_at") or tx.get("timestamp") or tx.get("timeStamp")
    if ts_iso:
        try:
            ts_dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
            ts_int = int(ts_dt.replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            try:
                ts_int = int(ts_iso)
            except Exception:
                ts_int = 0
    else:
        ts_int = int(tx.get("timestamp") or 0)
    token_address = str(
        tx.get("token_address")
        or tx.get("contract_address")
        or tx.get("token")
        or ""
    ).lower()
    token_symbol = tx.get("token_symbol") or tx.get("contract_ticker_symbol")
    from_addr = str(
        tx.get("from_address") or tx.get("from") or tx.get("from_addr") or ""
    ).lower()
    to_addr = str(
        tx.get("to_address") or tx.get("to") or tx.get("to_addr") or ""
    ).lower()
    direction = tx.get("direction")
    if direction not in {"in", "out"}:
        direction = _compute_direction(wallet, from_addr, to_addr)
    amount = tx.get("amount")
    if amount is None:
        raw_delta = tx.get("delta") or tx.get("value") or "0"
        decimals = int(tx.get("token_decimals") or tx.get("contract_decimals") or 0)
        try:
            amount = float(raw_delta) / (10**decimals) if decimals >= 0 else float(raw_delta)
        except Exception:
            amount = 0.0
    else:
        try:
            amount = float(amount)
        except Exception:
            amount = 0.0
    usd_value = tx.get("usd_value")
    if usd_value is None:
        usd_value = tx.get("value_quote")
    fees_paid = tx.get("fees_paid") or tx.get("gas_spent_quote")
    log_index = tx.get("log_index") or tx.get("logIndex")
    pricing_missing = bool(tx.get("pricing_missing"))
    return {
        "tx_hash": tx.get("tx_hash") or tx.get("hash"),
        "log_index": log_index,
        "timestamp": ts_int,
        "from": from_addr,
        "to": to_addr,
        "token_address": token_address or "",
        "token_symbol": token_symbol,
        "amount": amount,
        "usd_value": usd_value,
        "fees_paid": fees_paid,
        "direction": direction,
        "chain": chain,
        "wallet": wallet,
        "pricing_missing": pricing_missing or usd_value is None,
    }


def _load_tx_index_records(
    chain: str,
    address: str,
    retention_days: int = TX_INDEX_RETENTION_DAYS,
) -> Tuple[List[Dict[str, Any]], Set[Tuple[Any, ...]]]:
    path = _tx_index_path(chain, address)
    records: List[Dict[str, Any]] = []
    seen: Set[Tuple[Any, ...]] = set()
    cutoff = time.time() - retention_days * 86400
    if not os.path.exists(path):
        return records, seen
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                ts = int(entry.get("timestamp") or 0)
                if ts and ts < cutoff:
                    continue
                if not entry.get("direction"):
                    entry["direction"] = _compute_direction(
                        entry.get("wallet", address.lower()),
                        str(entry.get("from") or "").lower(),
                        str(entry.get("to") or "").lower(),
                    )
                key = _tx_index_key(entry)
                seen.add(key)
                records.append(entry)
    except Exception:
        return [], set()
    records.sort(key=lambda x: x.get("timestamp", 0))
    return records, seen


def _persist_tx_index_records(
    chain: str,
    address: str,
    records: List[Dict[str, Any]],
) -> None:
    if not records:
        return
    path = _tx_index_path(chain, address)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cutoff = time.time() - TX_INDEX_RETENTION_DAYS * 86400
    filtered: List[Dict[str, Any]] = []
    for entry in records:
        ts = int(entry.get("timestamp") or 0)
        if ts and ts < cutoff:
            continue
        filtered.append(entry)
    records = filtered
    with open(path, "w", encoding="utf-8") as f:
        for entry in sorted(records, key=lambda x: x.get("timestamp", 0)):
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _clamp01(value: Optional[float]) -> float:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, val))


def _sat(value: Optional[float], k: float) -> float:
    if not isinstance(k, (int, float)) or k <= 0:
        return 0.0
    try:
        val = max(0.0, float(value) if value is not None else 0.0)
    except (TypeError, ValueError):
        return 0.0
    if val <= 0:
        return 0.0
    return 1.0 - math.exp(-val / k)


def _compute_pump_trust(pump: Dict[str, Any]) -> Dict[str, Any]:
    realized = float(pump.get("realized_pnl_usd") or 0.0)
    cashed_out = float(pump.get("cashed_out_usd") or 0.0)
    buy_pressure = float(pump.get("buy_pressure_usd") or pump.get("invested_usd") or 0.0)
    buy_ratio = float(pump.get("buy_ratio") or 0.0)
    tx_count = float(pump.get("tx_count") or 0.0)
    roi_reason = str(pump.get("roi_reason") or "ok")

    profit_score = _sat(realized if realized > 0 else 0.0, TRUST_K_USD)
    exit_score = _sat(cashed_out if cashed_out > 0 else 0.0, TRUST_K_USD)
    pressure_score = _sat(buy_pressure, TRUST_K_USD) * _clamp01((buy_ratio - 0.55) / 0.45)
    activity_score = _sat(tx_count, TRUST_K_TX)
    base = (
        0.55 * profit_score
        + 0.20 * exit_score
        + 0.20 * pressure_score
        + 0.05 * activity_score
    )
    trust_reason = roi_reason or "ok"
    if roi_reason == "ok":
        pass
    elif roi_reason == "no_sells":
        if pressure_score >= 0.6:
            base *= 0.95
            trust_reason = "accumulation"
        else:
            base *= 0.65
            trust_reason = "no_sells"
    elif roi_reason in ("no_activity", "no_buys"):
        base *= 0.30
        trust_reason = roi_reason
    elif roi_reason == "mixed":
        base *= 0.80
        trust_reason = "mixed"
    else:
        trust_reason = roi_reason or "ok"
    trust_value = _clamp01(base)
    return {
        "trust": trust_value,
        "reason": trust_reason,
        "components": {
            "profit_score": profit_score,
            "exit_score": exit_score,
            "pressure_score": pressure_score,
            "activity_score": activity_score,
        },
    }


def _compute_actor_trust(pumps: List[Dict[str, Any]]) -> Tuple[float, Dict[str, Any], Dict[str, Any]]:
    if not pumps:
        return 0.0, {"n_pumps_used": 0, "stability_bonus_applied": False, "ewma_decay": TRUST_EWMA_DECAY}, {}
    sorted_pumps = sorted(
        [p for p in pumps if p.get("pump_start_ts")],
        key=lambda p: p.get("pump_start_ts", 0),
        reverse=True,
    )
    if not sorted_pumps:
        sorted_pumps = pumps[:]
    total = 0.0
    weight_sum = 0.0
    stability_hits = 0
    for idx, pump in enumerate(sorted_pumps[:TRUST_MAX_PUMPS]):
        trust_val = float(pump.get("trust_pump") or 0.0)
        weight = TRUST_EWMA_DECAY ** idx
        total += trust_val * weight
        weight_sum += weight
        components = pump.get("trust_components") or {}
        if float(components.get("profit_score") or 0.0) >= 0.6:
            stability_hits += 1
    actor_trust = _clamp01(total / weight_sum if weight_sum else 0.0)
    stability_bonus_applied = stability_hits >= 2
    if stability_bonus_applied:
        actor_trust = _clamp01(actor_trust + 0.05)
    last_pump = sorted_pumps[0] if sorted_pumps else {}
    meta = {
        "n_pumps_used": min(len(sorted_pumps), TRUST_MAX_PUMPS),
        "stability_bonus_applied": stability_bonus_applied,
        "ewma_decay": TRUST_EWMA_DECAY,
    }
    return actor_trust, meta, last_pump


def _parse_iso_ts(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None

def normalize_window(start_ts: int, end_ts: int, resolution_minutes: int = 30) -> tuple[int, int]:
    """Normalize window to reduce cache fragmentation."""
    res = max(1, resolution_minutes) * 60
    start_norm = start_ts - (start_ts % res)
    end_norm = end_ts + (res - (end_ts % res)) if end_ts % res else end_ts
    return start_norm, end_norm


def parse_dt_unsafe(value: str) -> datetime:
    value = (value or "").strip()
    fmts = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y_%m_%d %H:%M",
        "%Y-%m-%d %H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def load_pump_profiles(base_dir: str = "data/alpha_profiler") -> List[Dict]:
    profiles: List[Dict] = []
    if not os.path.isdir(base_dir):
        return profiles
    for fname in os.listdir(base_dir):
        if not fname.endswith("_profile.json"):
            continue
        path = os.path.join(base_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                profiles.append(data)
        except Exception:
            continue
    return profiles


def _load_progress(path: str = PROGRESS_PATH) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_progress(data: Dict[str, Any], path: str = PROGRESS_PATH) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _build_timeline(
    normalized_txs: List[Dict[str, Any]], bucket_minutes: int = 1
) -> Dict[str, Any]:
    bucket_seconds = bucket_minutes * 60
    timelines: Dict[str, Dict[str, Any]] = {}
    for tx in normalized_txs:
        addr = tx.get("address")
        chain = tx.get("chain")
        symbol = tx.get("symbol") or "UNKNOWN"
        pump_key = f"{symbol}_{tx.get('pump_start_ts')}"
        actor_key = f"{chain}:{addr}"
        ts = int(tx.get("timestamp") or 0)
        bucket = ts - (ts % bucket_seconds)
        net = tx.get("usd_value") or 0.0
        bucket_entry = timelines.setdefault(actor_key, {}).setdefault(pump_key, {})
        bucket_entry.setdefault("buckets", {})
        bucket_entry["buckets"].setdefault(bucket, 0.0)
        bucket_entry["buckets"][bucket] += net
    # format output
    output: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bucket_minutes": bucket_minutes,
        "actors": {},
    }
    for actor_key, pumps in timelines.items():
        output["actors"].setdefault(actor_key, {"pumps": {}})
        for pump_key, data in pumps.items():
            buckets = sorted(data["buckets"].items())
            output["actors"][actor_key]["pumps"][pump_key] = {
                "t": [
                    datetime.fromtimestamp(b, tz=timezone.utc).isoformat()
                    for b, _ in buckets
                ],
                "net_usd": [v for _, v in buckets],
            }
    return output


def _build_holdings_overlap(actors_out: List[Dict[str, Any]]) -> Dict[str, Any]:
    tokens: List[str] = []
    actor_keys: List[str] = []
    matrix: List[List[float]] = []
    token_index: Dict[str, int] = {}

    # gather tokens
    for a in actors_out:
        actor_key = f"{a.get('chain', '')}:{a.get('address', '')}"
        actor_keys.append(actor_key)
        holdings = a.get("current_holdings") or []
        row: List[float] = []
        for h in holdings:
            tok = h.get("symbol") or h.get("contract_address")
            if tok is None:
                continue
            if tok not in token_index:
                token_index[tok] = len(tokens)
                tokens.append(tok)
                # extend existing rows
                for r in matrix:
                    r.append(0.0)
            # ensure row length
        # build row
        row = [0.0] * len(tokens)
        for h in holdings:
            tok = h.get("symbol") or h.get("contract_address")
            if tok is None:
                continue
            idx = token_index.get(tok)
            if idx is None:
                continue
            row[idx] += float(h.get("usd_value") or 0.0)
        matrix.append(row)

    # rarity score: normalized inverse frequency
    rarity: Dict[str, float] = {}
    if tokens:
        freq = [0.0] * len(tokens)
        for row in matrix:
            for i, val in enumerate(row):
                if val > 0:
                    freq[i] += 1
        max_freq = max(freq) if freq else 1.0
        for i, tok in enumerate(tokens):
            rarity[tok] = 1.0 - (freq[i] / max_freq if max_freq else 0.0)

    overlap = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tokens": tokens,
        "actors": actor_keys,
        "matrix_usd": matrix,
        "token_rarity": rarity,
    }
    return overlap


def recompute_watchlist_pnl(
    config: Dict[str, Any],
    logger: logging.Logger,
    profiles_dir: str = "data/alpha_profiler",
    output_path: str = os.path.join(SYSTEM_DIR, "pnl_watchlist.json"),
    pre_days: int = 3,
    post_hours: int = 2,
    *,
    targets: Optional[List[Dict[str, Any]]] = None,
    label: str = "watchlist",
    timeline_output: Optional[str] = None,
    overlap_output: Optional[str] = None,
    network_output: Optional[str] = None,
    features_output: Optional[str] = None,
    cache_health_path: Optional[str] = None,
    tx_csv_path: Optional[str] = None,
    summary_wallets_path: Optional[str] = None,
    summary_tokens_path: Optional[str] = None,
    summary_tokens_by_actor_path: Optional[str] = None,
    force_refresh: bool = False,
    cache_ttl_seconds: int = 7200,
) -> str:
    prev_out = _load_json(output_path) or {}
    prev_cache_meta = prev_out.get("cache_meta", {}) if isinstance(prev_out, dict) else {}
    cache_meta = {
        "first_seen": prev_cache_meta.get("first_seen") or datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "cache_hits": 0,
        "cache_misses": 0,
        "api_calls": 0,
        "covalent_api_calls": 0,
        "birdeye_api_calls": 0,
        "buckets_cached": 0,
        "buckets_missing": 0,
        "pages_cached": 0,
        "pages_missing": 0,
        "pages_total": 0,
        "buckets_total": 0,
        "birdeye_pages_cached": 0,
        "birdeye_pages_missing": 0,
        "birdeye_pages_total": 0,
        "covalent_buckets_cached": 0,
        "covalent_buckets_missing": 0,
        "covalent_buckets_total": 0,
    }
    cache_meta["schema_version"] = 2
    system_dir = os.path.join(profiles_dir, "system")
    if targets is None:
        watchlist = _load_json(os.path.join(system_dir, "watchlist.json")) or []
    else:
        watchlist = targets
    timeline_output = timeline_output or (
        os.path.join(system_dir, "timeline_watchlist.json")
        if label == "watchlist"
        else os.path.join(system_dir, f"timeline_{label}.json")
    )
    overlap_output = overlap_output or (
        os.path.join(system_dir, "holdings_overlap.json")
        if label == "watchlist"
        else os.path.join(system_dir, f"holdings_overlap_{label}.json")
    )
    network_output = network_output or (
        os.path.join(system_dir, "network_watchlist.json")
        if label == "watchlist"
        else os.path.join(system_dir, f"network_{label}.json")
    )
    features_output = features_output or (
        os.path.join(WAREHOUSE_DIR, "features.json")
        if label == "watchlist"
        else os.path.join(WAREHOUSE_DIR, f"features_{label}.json")
    )
    cache_health_path = cache_health_path or (
        CACHE_HEALTH_PATH if label == "watchlist" else os.path.join(WAREHOUSE_DIR, f"cache_health_{label}.json")
    )
    tx_csv_path = tx_csv_path or (
        os.path.join(WAREHOUSE_DIR, "tx_normalized.csv")
        if label == "watchlist"
        else os.path.join(WAREHOUSE_DIR, f"tx_normalized_{label}.csv")
    )
    summary_wallets_path = summary_wallets_path or (
        os.path.join(WAREHOUSE_DIR, "summary_wallets.json")
        if label == "watchlist"
        else os.path.join(WAREHOUSE_DIR, f"summary_wallets_{label}.json")
    )
    summary_tokens_path = summary_tokens_path or (
        os.path.join(WAREHOUSE_DIR, "summary_tokens.json")
        if label == "watchlist"
        else os.path.join(WAREHOUSE_DIR, f"summary_tokens_{label}.json")
    )
    summary_tokens_by_actor_path = summary_tokens_by_actor_path or (
        os.path.join(WAREHOUSE_DIR, "summary_tokens_by_actor.json")
        if label == "watchlist"
        else os.path.join(WAREHOUSE_DIR, f"summary_tokens_by_actor_{label}.json")
    )
    cov_cfg = config.get("covalent", {}) or {}
    cache_cfg = config.get("cache_tx", {}) or {}
    cache_normalize = bool(cache_cfg.get("normalize", True))
    cache_res_min = int(cache_cfg.get("resolution_minutes", 30) or 30)
    cache_max_days = int(cache_cfg.get("max_window_days", 14) or 14)
    cov_client = CovalentClient(
        base_url=cov_cfg.get("base_url", "https://api.covalenthq.com"),
        api_key=cov_cfg.get("api_key", ""),
        logger=logger,
        max_calls_per_minute=int(cov_cfg.get("max_calls_per_minute") or 0) or None,
    )
    cov_enabled = bool(cov_cfg.get("enabled")) and bool(cov_client.api_key)
    now_dt = datetime.now(timezone.utc)
    snapshot_last_ts = _parse_iso_ts(prev_cache_meta.get("last_updated"))
    snapshot_stale = snapshot_last_ts is None or (now_dt.timestamp() - snapshot_last_ts) > cache_ttl_seconds

    be_client = BirdEyeClient(config=config, logger=logger)

    actors = _load_json(os.path.join(system_dir, "actors.json")) or []
    prev_actors_map = {}
    if isinstance(prev_out, dict):
        for a in prev_out.get("actors", []) or []:
            key = f"{str(a.get('address','')).lower()}:{str(a.get('chain','')).lower()}"
            prev_actors_map[key] = a
    profiles = load_pump_profiles(base_dir=profiles_dir)
    progress = _load_progress()

    actor_index = {
        f"{a.get('address', '').lower()}:{a.get('chain', '').lower()}": a
        for a in actors
    }

    actors_out: List[Dict[str, Any]] = []
    normalized_txs: List[Dict[str, Any]] = []
    summary_tokens: Dict[str, Dict[str, Any]] = {}
    summary_tokens_by_actor: Dict[str, Dict[str, Any]] = {}
    features: List[Dict[str, Any]] = []
    cache_hits_global = 0
    cache_miss_global = 0
    api_calls_global = 0
    buckets_needed_global = 0
    http_429_global = 0
    sleep_seconds_global = 0.0
    failed_buckets_global = 0

    limit = os.getenv("PWL_LIMIT")
    if limit:
        try:
            limit = int(limit)
        except Exception:
            limit = None
    diagnostic_mode = bool(os.getenv("PWL_DIAGNOSTIC"))

    success_cnt = 0
    fail_cnt = 0
    fail_reasons: Dict[str, int] = {}

    for idx, actor in enumerate(watchlist, start=1):
        if limit and idx > limit:
            logger.info("PWL_LIMIT=%s reached, stopping early", limit)
            break
        logger.info("Actor %d/%d: %s", idx, len(watchlist), actor.get("address"))
        addr = str(actor.get("address") or "").lower()
        chain = str(actor.get("chain") or "").lower()
        key = f"{addr}:{chain}"
        profile_actor = actor_index.get(key, {})
        profile_total_pnl = profile_actor.get("total_realized_pnl_usd", 0.0)
        profile_avg_roi = profile_actor.get("avg_roi", 0.0)
        best_level = profile_actor.get("best_pamper_level", "")
        actor_score = profile_actor.get("actor_score", 0.0)
        pumps_count = profile_actor.get("pumps_count", actor.get("pumps_count", 0))
        prev_actor = prev_actors_map.get(f"{addr}:{chain}") or {}
        need_force_refresh = bool(force_refresh) or snapshot_stale or not prev_actor

        tx_index_records, tx_index_seen = _load_tx_index_records(chain, addr)
        tx_index_dirty = False

        cov_success = cov_enabled
        fail_reason: Optional[str] = None
        portfolio_value = 0.0
        holdings: List[Dict[str, Any]] = []
        pumps_out: List[Dict[str, Any]] = []
        total_before = total_during = total_after = 0.0
        total_tx_items = 0
        total_errors = 0
        total_buckets = 0
        cache_hits = 0
        cache_misses = 0
        api_calls = 0
        buckets_needed = 0
        http_429_count = 0
        sleep_seconds_total = 0.0
        linked_map: Dict[str, Dict[str, Any]] = {}
        fees_total = 0.0
        timeline_buckets: Dict[int, Dict[str, Any]] = {}
        has_sell_tx = False
        price_missing = False
        last_seen_ts = 0

        # Текущие холдинги (один раз)
        if cov_enabled:
            try:
                balances = cov_client.get_token_balances(chain=chain, address=addr)
                holdings = [
                    {
                        "contract_address": b.get("token_address"),
                        "symbol": b.get("token_symbol"),
                        "balance": b.get("balance"),
                        "usd_value": b.get("usd_value"),
                    }
                    for b in balances[:25]
                ]
                portfolio_value = sum((b.get("usd_value") or 0.0) for b in balances)
            except Exception as exc:
                cov_success = False
                fail_reason = fail_reason or "balances_failed"
                logger.error("Covalent balances failed for %s: %s", addr, exc)

        # Профили, где участвовал адрес
        relevant_profiles = []
        for prof in profiles:
            wallets = prof.get("wallets") or []
            for w in wallets:
                if str(w.get("address") or "").lower() == addr:
                    relevant_profiles.append(prof)
                    break

        for prof in relevant_profiles:
            has_sell_tx = False
            price_missing = False
            token_flows: Dict[str, Dict[str, Any]] = {}
            invested_during = 0.0
            cashed_during = 0.0
            cashed_after = 0.0
            buy_pressure_usd = 0.0
            buy_events = 0
            sell_events = 0
            transfer_events_total = 0
            pricing_missing_count = 0
            meta = prof.get("meta") or {}
            pump_start_ts = int(meta.get("start_ts") or meta.get("pump_start_ts") or 0)
            pump_end_ts = int(meta.get("end_ts") or meta.get("pump_end_ts") or 0)
            if pump_start_ts <= 0:
                pump_start_ts = int(parse_dt_unsafe(meta.get("pump_start", "")).timestamp())
            if pump_end_ts <= 0:
                pump_end_ts = int(parse_dt_unsafe(meta.get("pump_end", "")).timestamp())
            last_seen_ts = max(last_seen_ts, pump_end_ts)

            window_start = pump_start_ts - pre_days * 86400
            window_end = pump_end_ts + post_hours * 3600
            if cache_normalize:
                window_start, window_end = normalize_window(window_start, window_end, cache_res_min)
            # guardrail
            if (window_end - window_start) > cache_max_days * 86400:
                window_end = window_start + cache_max_days * 86400
            # resume via progress
            progress_key = f"{chain}:{addr}:{window_start}:{window_end}"
            progress.setdefault(progress_key, {})
            progress[progress_key]["started_bucket"] = window_start // 900
            progress[progress_key].setdefault("started_at", datetime.now(timezone.utc).isoformat())
            resume_bucket = progress.get(progress_key, {}).get("committed_bucket") or progress.get(progress_key, {}).get("last_bucket")

            pnl_before = pnl_during = pnl_after = 0.0
            tx_count = 0
            txs: List[Dict[str, Any]] = []
            diag: Dict[str, Any] = {}
            has_sell_tx = False
            price_missing = False
            last_seen_tx_ts = 0
            if cov_enabled:
                cache_path = os.path.join(
                    CACHE_TX_DIR,
                    f"{chain}_{addr}_{window_start}_{window_end}.json",
                )
                window_txs = [
                    rec
                    for rec in tx_index_records
                    if window_start <= int(rec.get("timestamp") or 0) <= window_end
                ]
                txs.extend(window_txs)
                if not txs and os.path.exists(cache_path):
                    try:
                        with open(cache_path, "r", encoding="utf-8") as f:
                            cached_raw = json.load(f) or []
                        normalized_cached = [
                            _normalize_covalent_tx(raw_tx, addr, chain) for raw_tx in cached_raw or []
                        ]
                        for norm in normalized_cached:
                            key = _tx_index_key(norm)
                            if key not in tx_index_seen:
                                tx_index_seen.add(key)
                                tx_index_records.append(norm)
                                tx_index_dirty = True
                        if normalized_cached:
                            tx_index_records.sort(key=lambda x: x.get("timestamp", 0))
                        txs = [
                            rec
                            for rec in tx_index_records
                            if window_start <= int(rec.get("timestamp") or 0) <= window_end
                        ]
                        cache_hits += 1
                        cache_hits_global += 1
                    except Exception:
                        txs = []

                need_http = need_force_refresh
                if not need_http:
                    if not txs:
                        need_http = True
                    else:
                        latest_ts = max(int(t.get("timestamp") or 0) for t in txs)
                        if latest_ts < window_end - 60:
                            need_http = True

                if need_http:
                    txs_raw, diag = cov_client.iter_transactions_window(
                        chain=chain,
                        address=addr,
                        start_ts=window_start if resume_bucket is None else max(window_start, (resume_bucket + 1) * 900),
                        end_ts=window_end,
                        force_refresh=need_force_refresh,
                        cache_ttl_seconds=cache_ttl_seconds,
                    )
                    if txs_raw is None:
                        txs_raw = []
                        cov_success = False
                        fail_reason = fail_reason or "fetch_failed"
                    normalized_new = [
                        _normalize_covalent_tx(raw_tx, addr, chain) for raw_tx in (txs_raw or [])
                    ]
                    if normalized_new:
                        for norm in normalized_new:
                            key = _tx_index_key(norm)
                            if key in tx_index_seen:
                                continue
                            tx_index_seen.add(key)
                            tx_index_records.append(norm)
                            tx_index_dirty = True
                        tx_index_records.sort(key=lambda x: x.get("timestamp", 0))
                        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                        with open(cache_path, "w", encoding="utf-8") as f:
                            json.dump(normalized_new, f)
                    txs = [
                        rec
                        for rec in tx_index_records
                        if window_start <= int(rec.get("timestamp") or 0) <= window_end
                    ]
                if diag:
                    total_buckets += diag.get("buckets_fetched", 0) or 0
                    total_errors += diag.get("errors_count", 0) or 0
                    total_tx_items += diag.get("tx_items_count", 0) or 0
                    cache_hits += diag.get("cache_hits", 0) or 0
                    cache_misses += diag.get("cache_misses", 0) or 0
                    api_calls += diag.get("api_calls", 0) or 0
                    http_429_count += diag.get("http_429_count", 0) or 0
                    sleep_seconds_total += diag.get("sleep_seconds_total", 0.0) or 0.0
                    buckets_needed += diag.get("buckets_needed", 0) or 0
                    last_bucket_processed = diag.get("last_bucket_processed")
                    if last_bucket_processed is not None:
                        progress[progress_key] = {
                            "last_bucket": last_bucket_processed,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "buckets_done": diag.get("buckets_fetched", 0) or 0,
                            "committed_bucket": last_bucket_processed,
                            "started_bucket": progress[progress_key].get("started_bucket"),
                            "started_at": progress[progress_key].get("started_at"),
                        }
                        _save_progress(progress)
            else:
                cov_success = False

            if not txs and cov_success is False:
                fail_reason = fail_reason or "no_txs_window"
            for tx in txs or []:
                amount_raw = tx.get("amount") or 0.0
                try:
                    amount_val = float(amount_raw)
                except Exception:
                    amount_val = 0.0
                usd_val_raw = tx.get("usd_value")
                usd_val = None
                if usd_val_raw is not None:
                    try:
                        usd_val = float(usd_val_raw)
                    except Exception:
                        usd_val = None
                pricing_missing_flag = bool(tx.get("pricing_missing")) or usd_val is None
                if pricing_missing_flag:
                    price_missing = True
                    pricing_missing_count += 1
                usd_abs = abs(usd_val) if usd_val is not None else 0.0
                from_addr = (
                    tx.get("from")
                    or tx.get("from_address")
                    or tx.get("from_addr")
                    or ""
                )
                to_addr = tx.get("to") or tx.get("to_address") or tx.get("to_addr") or ""
                from_addr_l = str(from_addr).lower()
                to_addr_l = str(to_addr).lower()
                direction = tx.get("direction")
                if direction not in {"in", "out"}:
                    direction = _compute_direction(addr, from_addr_l, to_addr_l)
                if direction not in {"in", "out"}:
                    continue
                transfer_events_total += 1
                if direction == "in":
                    sell_events += 1
                    has_sell_tx = True
                elif direction == "out":
                    buy_events += 1
                signed_usd = 0.0
                if usd_val is not None:
                    signed_usd = usd_abs if direction == "in" else -usd_abs
                ts = int(tx.get("timestamp") or 0)
                phase = "after"
                if ts < pump_start_ts:
                    pnl_before += signed_usd
                    phase = "before"
                elif ts <= pump_end_ts:
                    pnl_during += signed_usd
                    phase = "during"
                elif ts <= window_end:
                    pnl_after += signed_usd
                    phase = "after"
                tx_count += 1
                total_tx_items += 1
                fee_usd = 0.0
                if tx.get("fees_paid"):
                    try:
                        fee_usd = float(tx.get("fees_paid") or 0.0)
                    except Exception:
                        fee_usd = 0.0
                fees_total += fee_usd
                last_seen_tx_ts = max(last_seen_tx_ts, ts)

                if phase == "during":
                    if direction == "out" and usd_abs > 0:
                        invested_during += usd_abs
                        buy_pressure_usd += usd_abs
                    elif direction == "in" and usd_abs > 0:
                        cashed_during += usd_abs
                elif phase == "after":
                    if direction == "in" and usd_abs > 0:
                        cashed_after += usd_abs

                # normalized tx for warehouse
                normalized_txs.append(
                    {
                        "address": addr,
                        "chain": chain,
                        "symbol": meta.get("symbol"),
                        "token_address": tx.get("token_address"),
                        "token_symbol": tx.get("token_symbol"),
                        "amount": amount_val,
                        "usd_value": usd_val,
                        "fees_usd": fee_usd,
                        "timestamp": ts,
                        "phase": phase,
                        "pump_start_ts": pump_start_ts,
                        "pump_end_ts": pump_end_ts,
                        "direction": direction,
                        "pricing_missing": pricing_missing_flag,
                    }
                )
                # timeline bucket (15m)
                bucket = ts // 900
                tb = timeline_buckets.setdefault(
                    bucket,
                    {
                        "bucket": bucket,
                        "tx_count": 0,
                        "usd_volume": 0.0,
                        "fees_usd": 0.0,
                    },
                )
                tb["tx_count"] += 1
                tb["usd_volume"] += usd_abs
                tb["fees_usd"] += fee_usd
                # linked heuristic
                other_addr = None
                link_dir = None
                if direction == "out" and to_addr_l and to_addr_l != addr:
                    other_addr = to_addr_l
                    link_dir = "out"
                elif direction == "in" and from_addr_l and from_addr_l != addr:
                    other_addr = from_addr_l
                    link_dir = "in"
                if other_addr:
                    lnk = linked_map.setdefault(
                        other_addr,
                        {
                            "address": other_addr,
                            "transfers_count": 0,
                            "total_value_usd": 0.0,
                            "direction": "mixed",
                        },
                    )
                    lnk["transfers_count"] += 1
                    lnk["total_value_usd"] += usd_abs
                    if lnk["direction"] == "mixed":
                        lnk["direction"] = link_dir
                    elif link_dir and lnk["direction"] != link_dir:
                        lnk["direction"] = "mixed"

                # summary by token
                token_addr = tx.get("token_address") or ""
                token_symbol = tx.get("token_symbol")
                token_key = f"{chain}:{token_addr or token_symbol or 'unknown'}"
                tok = summary_tokens.setdefault(
                    token_key,
                    {
                        "chain": chain,
                        "token_address": token_addr,
                        "token_symbol": token_symbol,
                        "tx_count": 0,
                        "usd_volume": 0.0,
                    },
                )
                tok["tx_count"] += 1
                tok["usd_volume"] += usd_abs
                # per actor token summary
                actor_tok_key = f"{key}:{token_addr}"
                atok = summary_tokens_by_actor.setdefault(
                    actor_tok_key,
                    {
                        "address": addr,
                        "chain": chain,
                        "token_address": token_addr,
                        "token_symbol": token_symbol,
                        "tx_count": 0,
                        "usd_volume": 0.0,
                    },
                )
                atok["tx_count"] += 1
                atok["usd_volume"] += usd_abs

                if phase == "during" and usd_abs > 0:
                    tf = token_flows.setdefault(
                        token_key,
                        {
                            "token_key": token_key,
                            "token_address": token_addr,
                            "token_symbol": token_symbol,
                            "in_usd": 0.0,
                            "out_usd": 0.0,
                            "tx_in": 0,
                            "tx_out": 0,
                        },
                    )
                    if direction == "in":
                        tf["in_usd"] += usd_abs
                        tf["tx_in"] += 1
                    elif direction == "out":
                        tf["out_usd"] += usd_abs
                        tf["tx_out"] += 1

            token_flows_list = []
            if token_flows:
                token_flows_list = sorted(
                    [
                        {
                            **flow,
                            "total_usd": (flow["in_usd"] + flow["out_usd"]),
                        }
                        for flow in token_flows.values()
                    ],
                    key=lambda x: x["total_usd"],
                    reverse=True,
                )
            cashed_out_total = cashed_during + cashed_after
            realized_flow_pnl = cashed_out_total - invested_during
            roi_value = (
                (realized_flow_pnl / invested_during)
                if invested_during > 0 and cashed_out_total > 0
                else None
            )
            if transfer_events_total <= 0:
                roi_reason = "no_activity"
            elif invested_during <= 0:
                roi_reason = "no_buys"
            elif cashed_out_total <= 0:
                roi_reason = (
                    "accumulation"
                    if invested_during >= ACCUMULATION_PRESSURE_USD
                    else "no_sells"
                )
            elif cashed_out_total >= max(1.0, invested_during) * DISTRIBUTION_RATIO:
                roi_reason = "distribution"
            else:
                roi_reason = "ok"
            if pricing_missing_count > 0 and roi_reason == "ok":
                roi_reason = "mixed"
            if not cov_success:
                roi_reason = fail_reason or roi_reason or "covalent_partial"
            window_seconds = max(1, pump_end_ts - pump_start_ts) if pump_end_ts > pump_start_ts else 1
            window_hours = max(1.0, window_seconds / 3600.0)
            buy_ratio = (
                invested_during / max(1.0, invested_during + cashed_out_total)
                if (invested_during + cashed_out_total) > 0
                else 0.0
            )
            pump_payload = {
                "symbol": meta.get("symbol"),
                "chain": meta.get("chain"),
                "profile_file": meta.get("profile_file") or "",
                "pump_start_ts": pump_start_ts,
                "pump_end_ts": pump_end_ts,
                "window_start_ts": window_start,
                "window_end_ts": window_end,
                "pnl_before_usd": pnl_before if cov_success else None,
                "pnl_during_usd": pnl_during if cov_success else None,
                "pnl_after_usd": pnl_after if cov_success else None,
                "pnl_total_usd": (pnl_before + pnl_during + pnl_after) if cov_success else None,
                "roi_reason": roi_reason,
                "has_sell_tx": has_sell_tx,
                "price_missing": price_missing,
                "pricing_missing_count": pricing_missing_count,
                "tx_count": tx_count,
                "transfer_events_total": transfer_events_total,
                "buy_events": buy_events,
                "sell_events": sell_events,
                "covalent_used_chain_name": diag.get("chain_name"),
                "start_bucket": diag.get("start_bucket"),
                "end_bucket": diag.get("end_bucket"),
                "buckets_fetched": diag.get("buckets_fetched"),
                "tx_items_count": diag.get("tx_items_count"),
                "errors_count": diag.get("errors_count"),
                "cache_hits": diag.get("cache_hits"),
                "http_429_count": diag.get("http_429_count"),
                "sleep_seconds_total": diag.get("sleep_seconds_total"),
                "timeline_buckets": sorted(timeline_buckets.values(), key=lambda x: x["bucket"]),
                "pnl_mode": "value_minus_fees",
                "invested_usd": invested_during if cov_success else None,
                "cashed_out_usd": cashed_out_total if cov_success else None,
                "cashed_during_usd": cashed_during if cov_success else None,
                "cashed_after_usd": cashed_after if cov_success else None,
                "realized_pnl_usd": realized_flow_pnl if cov_success else None,
                "roi": roi_value if cov_success else None,
                "token_flows": token_flows_list[:20],
                "buy_pressure_usd": buy_pressure_usd if cov_success else None,
                "buy_intensity": (buy_pressure_usd / window_hours) if cov_success else None,
                "buy_ratio": buy_ratio if cov_success else None,
            }
            trust_info = _compute_pump_trust(pump_payload if cov_success else {})
            pump_payload["trust_pump"] = trust_info.get("trust")
            pump_payload["trust_reason"] = trust_info.get("reason")
            pump_payload["trust_components"] = trust_info.get("components")
            pump_payload["trust_debug"] = {
                "invested_usd": pump_payload.get("invested_usd"),
                "cashed_out_usd": pump_payload.get("cashed_out_usd"),
                "realized_pnl_usd": pump_payload.get("realized_pnl_usd"),
                "buy_pressure_usd": pump_payload.get("buy_pressure_usd"),
                "buy_ratio": pump_payload.get("buy_ratio"),
                "buy_intensity": pump_payload.get("buy_intensity"),
                "tx_count": pump_payload.get("tx_count"),
                "transfer_events_total": pump_payload.get("transfer_events_total"),
                "pricing_missing_count": pump_payload.get("pricing_missing_count"),
            }
            pumps_out.append(pump_payload)

            total_before += pnl_before
            total_during += pnl_during
            total_after += pnl_after

        if tx_index_dirty:
            try:
                _persist_tx_index_records(chain, addr, tx_index_records)
            except Exception:
                logger.warning("Failed to persist tx index for %s:%s", chain, addr)

        actor_trust, actor_trust_meta, last_trust_pump = _compute_actor_trust(pumps_out)
        actor_trust_meta["updated_at"] = int(now_dt.timestamp())
        actor_last_pump_summary: Dict[str, Any] = {}
        if last_trust_pump:
            actor_last_pump_summary = {
                "symbol": last_trust_pump.get("symbol"),
                "pump_start_ts": last_trust_pump.get("pump_start_ts"),
                "trust_pump": last_trust_pump.get("trust_pump"),
                "trust_reason": last_trust_pump.get("trust_reason"),
                "realized_pnl_usd": last_trust_pump.get("realized_pnl_usd"),
                "buy_pressure_usd": last_trust_pump.get("buy_pressure_usd"),
                "buy_ratio": last_trust_pump.get("buy_ratio"),
                "buy_intensity": last_trust_pump.get("buy_intensity"),
            }

        linked_addresses = [
            v
            for v in linked_map.values()
            if v.get("transfers_count", 0) >= 2
            or v.get("total_value_usd", 0.0) >= 100.0
        ]
        linked_addresses.sort(key=lambda x: x.get("total_value_usd", 0.0), reverse=True)

        realized_actor = (
            total_before + total_during + total_after if cov_success else None
        )
        unrealized_actor = portfolio_value if cov_success else None
        total_actor = (
            (realized_actor or 0.0) + (unrealized_actor or 0.0) if cov_success else None
        )

        if not cov_success and prev_actor and not diagnostic_mode:
            # reuse previous successful snapshot; keep diagnostics from current run
            old_pumps_map = {
                f"{p.get('symbol','')}:{p.get('pump_start_ts')}": p for p in prev_actor.get("pumps", []) or []
            }
            merged = []
            for pp in pumps_out:
                key_pp = f"{pp.get('symbol','')}:{pp.get('pump_start_ts')}"
                if (pp.get("pnl_total_usd") is None or pp.get("cov_pnl_total_usd") is None) and key_pp in old_pumps_map:
                    merged.append(old_pumps_map[key_pp])
                else:
                    merged.append(pp)
            # include pumps that were not recalculated this run
            for key_pp, opp in old_pumps_map.items():
                if key_pp not in {f"{p.get('symbol','')}:{p.get('pump_start_ts')}" for p in merged}:
                    merged.append(opp)
            pumps_out = merged
            holdings = prev_actor.get("current_holdings", holdings)
            portfolio_value = prev_actor.get("portfolio_value_usd", portfolio_value)
            total_before = prev_actor.get("pnl_aggregate", {}).get("total_before_usd", total_before)
            total_during = prev_actor.get("pnl_aggregate", {}).get("total_during_usd", total_during)
            total_after = prev_actor.get("pnl_aggregate", {}).get("total_after_usd", total_after)
            realized_actor = prev_actor.get("cov_realized_pnl_usd", realized_actor)
            unrealized_actor = prev_actor.get("cov_unrealized_pnl_usd", unrealized_actor)
            total_actor = prev_actor.get("cov_total_pnl_usd", total_actor)
            cov_success = False
            fail_reason = fail_reason or prev_actor.get("fail_reason") or "reuse_previous"

        if cov_success:
            success_cnt += 1
        else:
            fail_cnt += 1
            fr = fail_reason or "unknown"
            fail_reasons[fr] = fail_reasons.get(fr, 0) + 1

        cache_miss_global += cache_misses
        api_calls_global += api_calls
        buckets_needed_global += buckets_needed
        http_429_global += http_429_count
        sleep_seconds_global += sleep_seconds_total
        failed_buckets_global += total_errors

        # last_seen: либо по окнам пампов, либо по последнему tx в окне
        last_seen_ts = max(last_seen_ts, last_seen_tx_ts)
        last_seen_iso = (
            datetime.fromtimestamp(last_seen_ts, tz=timezone.utc).isoformat()
            if last_seen_ts
            else prev_actor.get("last_seen")
        )
        actors_out.append(
            {
                "address": addr,
                "chain": chain,
                "pumps_count": pumps_count,
                "best_pamper_level": best_level,
                "actor_score": actor_score,
                "last_seen": last_seen_iso,
                "profile_total_pnl_usd": profile_total_pnl,
                "profile_avg_roi": profile_avg_roi,
                "cov_pnl_before_usd": total_before if cov_success else None,
                "cov_pnl_during_usd": total_during if cov_success else None,
                "cov_pnl_after_usd": total_after if cov_success else None,
                "cov_pnl_total_usd": realized_actor if cov_success else None,
                "cov_realized_pnl_usd": realized_actor,
                "cov_unrealized_pnl_usd": unrealized_actor,
                "cov_total_pnl_usd": total_actor,
                "cov_fees_usd": fees_total if cov_success else None,
                "pumps": pumps_out,
                "pnl_aggregate": {
                    "total_before_usd": total_before if cov_success else None,
                    "total_during_usd": total_during if cov_success else None,
                    "total_after_usd": total_after if cov_success else None,
                    "total_pnl_usd": realized_actor if cov_success else None,
                },
                "current_holdings": holdings,
                "portfolio_value_usd": portfolio_value if cov_success else None,
                "covalent_success": cov_success and cov_enabled,
                "fail_reason": fail_reason,
                "buckets_fetched": total_buckets,
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "api_calls": api_calls,
                "buckets_needed": buckets_needed,
                "buckets_missing": max(0, buckets_needed - (cache_hits + cache_misses)),
                "http_429_count": http_429_count,
                "sleep_seconds_total": sleep_seconds_total,
                "tx_items_count": total_tx_items,
                "errors_count": total_errors,
                "linked_addresses": linked_addresses[:25],
                "timeline_buckets": sorted(
                    timeline_buckets.values(), key=lambda x: x["bucket"]
                ),
                "pnl_mode": "value_minus_fees",
                "actor_trust": actor_trust,
                "actor_trust_meta": actor_trust_meta,
                "actor_last_pump_summary": actor_last_pump_summary,
            }
        )
        features.append(
            {
                "address": addr,
                "chain": chain,
                "actor_score": actor_score,
                "role_guess": actor.get("role_guess"),
                "role_confidence": actor.get("role_confidence"),
                "pumps_count": pumps_count,
                "avg_roi": profile_avg_roi,
                "cov_total_pnl_usd": total_actor,
                "cov_realized_pnl_usd": realized_actor,
                "cov_unrealized_pnl_usd": unrealized_actor,
                "portfolio_value_usd": portfolio_value,
                "avg_share_of_pump_volume": actor.get("avg_share_of_pump_volume"),
                "badges": actor.get("badges", []),
                "last_seen": last_seen_iso,
                "actor_trust": actor_trust,
                "actor_trust_meta": actor_trust_meta,
                "actor_trust_updated_at": actor_trust_meta.get("updated_at"),
                "actor_last_pump_summary": actor_last_pump_summary,
            }
        )

    cache_meta["actors_processed"] = len(watchlist)
    cache_meta.update(
        {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "cache_hits": cache_hits_global,
            "cache_misses": cache_miss_global,
            "api_calls": api_calls_global,
            "buckets_cached": cache_hits_global,
            "buckets_missing": max(0, buckets_needed_global - cache_hits_global - cache_miss_global),
            "http_429_count": http_429_global,
            "sleep_seconds_total": sleep_seconds_global,
            "failed_buckets": failed_buckets_global,
            "report": {
                "covalent": {
                    "hits": cache_hits_global,
                    "misses": cache_miss_global,
                    "api_calls": api_calls_global,
                },
                "cache_tx": {
                    "windows_reused": cache_hits_global,
                    "windows_fetched": cache_miss_global,
                },
            },
        }
    )

    # агрегируем реальное состояние кэша по файловой системе
    buckets_cached_fs = 0
    pages_cached_fs = 0
    birdeye_pages_total = 0
    for root, _, files in os.walk(RAW_CACHE_COV):
        for fn in files:
            if fn.startswith("bucket_") and fn.endswith(".json"):
                buckets_cached_fs += 1
    for root, _, files in os.walk(RAW_CACHE_BE):
        for fn in files:
            if fn.endswith(".json"):
                pages_cached_fs += 1
                birdeye_pages_total += 1

    cache_meta["buckets_cached"] = max(cache_meta.get("buckets_cached", 0), buckets_cached_fs)
    cache_meta.setdefault("buckets_total", cache_meta.get("buckets_needed", buckets_needed_global))
    cache_meta["pages_cached"] = pages_cached_fs
    cache_meta["pages_total"] = max(cache_meta.get("pages_total", 0), birdeye_pages_total, pages_cached_fs)
    cache_meta.setdefault("pages_missing", max(0, cache_meta["pages_total"] - cache_meta["pages_cached"]))
    # birdeye snapshot counters
    if getattr(be_client, "cache_meta_snapshot", None):
        be_snap = be_client.cache_meta_snapshot
        cache_meta["pages_cached"] = max(cache_meta.get("pages_cached", 0), be_snap.get("pages_cached", 0))
        cache_meta["pages_missing"] = max(
            cache_meta.get("pages_missing", 0), be_snap.get("pages_missing", 0)
        )
        cache_meta["pages_total"] = max(
            cache_meta.get("pages_total", 0),
            be_snap.get("pages_cached", 0) + be_snap.get("pages_missing", 0),
        )
        cache_meta["birdeye_api_calls"] = max(
            cache_meta.get("birdeye_api_calls", 0), be_snap.get("api_calls", 0)
        )
        cache_meta["birdeye_cache_hits"] = be_snap.get("cache_hits", 0)
        cache_meta["birdeye_cache_misses"] = be_snap.get("cache_misses", 0)
        cache_meta["api_calls"] = cache_meta.get("api_calls", 0) + cache_meta["birdeye_api_calls"]
    # merge persisted birdeye cache_meta if exists
    be_meta_file = os.path.join(RAW_CACHE_BE, "cache_meta.json")
    if os.path.exists(be_meta_file):
        try:
            be_file = _load_json(be_meta_file) or {}
            if isinstance(be_file, dict):
                cache_meta["pages_cached"] = max(cache_meta.get("pages_cached", 0), be_file.get("pages_cached", 0))
                cache_meta["pages_missing"] = max(cache_meta.get("pages_missing", 0), be_file.get("pages_missing", 0))
                cache_meta["pages_total"] = max(cache_meta.get("pages_total", 0), be_file.get("pages_total", 0))
                cache_meta["birdeye_api_calls"] = max(
                    cache_meta.get("birdeye_api_calls", 0), be_file.get("api_calls", 0)
                )
                cache_meta["api_calls"] = cache_meta.get("api_calls", 0) + cache_meta.get("birdeye_api_calls", 0)
                cache_meta["first_seen"] = cache_meta.get("first_seen") or be_file.get("first_seen")
        except Exception:
            pass

    cov_meta = {
        "cache_hits": cache_meta.get("cache_hits", 0),
        "cache_misses": cache_meta.get("cache_misses", 0),
        "api_calls": cache_meta.get("covalent_api_calls", cache_meta.get("api_calls", 0)),
        "buckets_cached": cache_meta.get("buckets_cached", 0),
        "buckets_missing": cache_meta.get("buckets_missing", 0),
        "buckets_total": cache_meta.get("buckets_total", cache_meta.get("buckets_needed", buckets_needed_global)),
        "first_seen": cache_meta.get("first_seen"),
        "last_updated": cache_meta.get("last_updated"),
        "http_429_count": cache_meta.get("http_429_count", 0),
        "sleep_seconds_total": cache_meta.get("sleep_seconds_total", 0.0),
        "failed_buckets": cache_meta.get("failed_buckets", 0),
        "report": {
            "buckets_reused": cache_hits_global,
            "buckets_fetched": cache_miss_global,
            "windows_cached": len(os.listdir(CACHE_TX_DIR)) if os.path.isdir(CACHE_TX_DIR) else 0,
        },
    }
    bir_meta = {
        "cache_hits": cache_meta.get("cache_hits", 0) if cache_meta.get("birdeye_pages_cached") else cache_meta.get("birdeye_cache_hits", 0),
        "cache_misses": cache_meta.get("birdeye_cache_misses", cache_meta.get("pages_missing", 0)),
        "api_calls": cache_meta.get("birdeye_api_calls", 0),
        "pages_cached": cache_meta.get("pages_cached", cache_meta.get("birdeye_pages_cached", 0)),
        "pages_missing": cache_meta.get("pages_missing", cache_meta.get("birdeye_pages_missing", 0)),
        "pages_total": cache_meta.get("pages_total", cache_meta.get("birdeye_pages_total", 0)),
        "first_seen": cache_meta.get("first_seen"),
        "last_updated": cache_meta.get("last_updated"),
    }
    cache_meta_nested = {
        "covalent": cov_meta,
        "birdeye": bir_meta,
        "first_seen": cache_meta.get("first_seen"),
        "last_updated": cache_meta.get("last_updated"),
        "schema_version": 2,
        # legacy flat fields for backward compatibility
        "cache_hits": cache_meta.get("cache_hits", 0),
        "cache_misses": cache_meta.get("cache_misses", 0),
        "api_calls": cache_meta.get("api_calls", 0),
        "buckets_cached": cache_meta.get("buckets_cached", 0),
        "buckets_missing": cache_meta.get("buckets_missing", 0),
        "pages_cached": cache_meta.get("pages_cached", 0),
        "pages_missing": cache_meta.get("pages_missing", 0),
        "cache_version": CACHE_VERSION,
    }

    out = {
        "schema_version": 2,
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pre_days": pre_days,
            "post_hours": post_hours,
            "max_buckets_per_actor": 600,
            "cache_version": CACHE_VERSION,
            "success": success_cnt,
            "failed": fail_cnt,
            "fail_reasons": fail_reasons,
            "cache_hits": cache_hits_global,
            "cache_meta": cache_meta_nested,
        },
        "cache_meta": cache_meta_nested,
        "actors": actors_out,
    }
    _write_json_atomic(out, output_path)
    logger.info("Saved Covalent-based PnL for watchlist to %s", output_path)

    # warehouse outputs
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    # normalized txs
    if tx_csv_path and normalized_txs:
        headers = list({k for tx in normalized_txs for k in tx.keys()})
        with open(tx_csv_path, "w", encoding="utf-8") as f:
            f.write(",".join(headers) + "\n")
            for tx in normalized_txs:
                row = [str(tx.get(h, "")) for h in headers]
                f.write(",".join(row) + "\n")
    summary_wallets = [
        {
            "address": a["address"],
            "chain": a["chain"],
            "cov_total_pnl_usd": a.get("cov_total_pnl_usd"),
            "cov_realized_pnl_usd": a.get("cov_realized_pnl_usd"),
            "cov_unrealized_pnl_usd": a.get("cov_unrealized_pnl_usd"),
            "tx_items_count": a.get("tx_items_count"),
        }
        for a in actors_out
    ]
    if summary_wallets_path:
        with open(summary_wallets_path, "w", encoding="utf-8") as f:
            json.dump(summary_wallets, f, ensure_ascii=False, indent=2)

    if summary_tokens and summary_tokens_path:
        with open(summary_tokens_path, "w", encoding="utf-8") as f:
            json.dump(list(summary_tokens.values()), f, ensure_ascii=False, indent=2)
    if summary_tokens_by_actor and summary_tokens_by_actor_path:
        with open(summary_tokens_by_actor_path, "w", encoding="utf-8") as f:
            json.dump(list(summary_tokens_by_actor.values()), f, ensure_ascii=False, indent=2)
    # features artifact for hunter consumption
    if features_output:
        with open(features_output, "w", encoding="utf-8") as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        if label == "watchlist":
            watchlist_features_path = os.path.join(WAREHOUSE_DIR, "features_watchlist.json")
            with open(watchlist_features_path, "w", encoding="utf-8") as wf:
                json.dump(features, wf, ensure_ascii=False, indent=2)

    # timelines artifact
    if timeline_output:
        timeline_artifact = _build_timeline(normalized_txs, bucket_minutes=1)
        _write_json_atomic(timeline_artifact, timeline_output)

    # holdings overlap
    if overlap_output:
        overlap = _build_holdings_overlap(actors_out)
        _write_json_atomic(overlap, overlap_output)

    # network graph (actors + linked)
    nodes = []
    edges = []
    actor_addrs = set(a["address"] for a in actors_out)
    for a in actors_out:
        nodes.append(
            {
                "id": a["address"],
                "chain": a["chain"],
                "is_actor": True,
                "score": a.get("actor_score"),
                "pnl": a.get("cov_total_pnl_usd"),
            }
        )
        for l in a.get("linked_addresses") or []:
            if l["address"] not in actor_addrs:
                nodes.append({"id": l["address"], "is_actor": False})
            edges.append(
                {
                    "source": a["address"],
                    "target": l["address"],
                    "value": l.get("total_value_usd", 0.0),
                    "transfers": l.get("transfers_count", 0),
                }
            )
    if network_output:
        net_out = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "nodes": nodes,
            "edges": edges,
        }
        _write_json_atomic(net_out, network_output)
    # cache health artifact
    cache_health = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cache_version": CACHE_VERSION,
        "cache_meta": cache_meta_nested,
        "report": cache_meta_nested.get("report", {}),
        "progress": _load_progress(),
    }
    _write_json_atomic(cache_health, cache_health_path)

    # console summary
    top = sorted(
        actors_out, key=lambda x: x.get("cov_total_pnl_usd") or 0, reverse=True
    )[:5]
    logger.info(
        "Top actors by cov_total_pnl_usd: %s",
        [(t["address"], t.get("cov_total_pnl_usd")) for t in top],
    )
    logger.info(
        "Covalent success=%d failed=%d reasons=%s cache_hits=%d",
        success_cnt,
        fail_cnt,
        fail_reasons,
        cache_hits_global,
    )
    cov_stats = cov_client.stats_snapshot()
    logger.info(
        "Covalent summary: api_calls=%s cache_hits=%s cache_misses=%s http_429=%s sleep_total=%.1fs retry_after=%.1fs base_url=%s key=%s",
        cov_stats.get("api_calls", 0),
        cov_stats.get("cache_hits", 0),
        cov_stats.get("cache_misses", 0),
        cov_stats.get("http_429", 0),
        cov_stats.get("sleep_seconds_total", 0.0),
        cov_stats.get("retry_after_seconds_total", 0.0),
        cov_stats.get("base_url"),
        cov_stats.get("api_key_masked"),
    )
    if cov_client.debug_enabled and cov_stats.get("last_status") is not None:
        logger.info(
            "Covalent last request: status=%s url=%s",
            cov_stats.get("last_status"),
            cov_stats.get("last_url"),
        )

    return output_path


def recompute_actors_pnl(
    config: Dict[str, Any],
    logger: logging.Logger,
    profiles_dir: str = "data/alpha_profiler",
    output_path: str = os.path.join(SYSTEM_DIR, "pnl_actors.json"),
    pre_days: int = 3,
    post_hours: int = 2,
) -> str:
    system_dir = os.path.join(profiles_dir, "system")
    actors = _load_json(os.path.join(system_dir, "actors.json")) or []
    return recompute_watchlist_pnl(
        config=config,
        logger=logger,
        profiles_dir=profiles_dir,
        output_path=output_path,
        pre_days=pre_days,
        post_hours=post_hours,
        targets=actors,
        label="actors",
        timeline_output=os.path.join(system_dir, "timeline_actors.json"),
        overlap_output=os.path.join(system_dir, "holdings_overlap_actors.json"),
        network_output=os.path.join(system_dir, "network_actors.json"),
        features_output=os.path.join(WAREHOUSE_DIR, "features_actors.json"),
        cache_health_path=os.path.join(WAREHOUSE_DIR, "cache_health_actors.json"),
        tx_csv_path=os.path.join(WAREHOUSE_DIR, "tx_normalized_actors.csv"),
        summary_wallets_path=os.path.join(WAREHOUSE_DIR, "summary_wallets_actors.json"),
        summary_tokens_path=os.path.join(WAREHOUSE_DIR, "summary_tokens_actors.json"),
        summary_tokens_by_actor_path=os.path.join(
            WAREHOUSE_DIR, "summary_tokens_by_actor_actors.json"
        ),
        force_refresh=True,
    )

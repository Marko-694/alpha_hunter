import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .actors_builder import _load_json  # reuse loader
from .covalent_client import CovalentClient
from .birdeye_client import BirdEyeClient

PROGRESS_PATH = "data/alpha_profiler/pnl_progress.json"
WAREHOUSE_DIR = "data/alpha_profiler/warehouse"
CACHE_TX_DIR = "data/alpha_profiler/cache_tx"
RAW_CACHE_COV = "data/raw_cache/covalent"
RAW_CACHE_BE = "data/raw_cache/birdeye"
CACHE_VERSION = "1.0"
CACHE_HEALTH_PATH = "data/alpha_profiler/warehouse/cache_health.json"


def _write_json_atomic(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


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
    output_path: str = "data/alpha_profiler/pnl_watchlist.json",
    pre_days: int = 3,
    post_hours: int = 2,
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

    be_client = BirdEyeClient(config=config, logger=logger)

    watchlist = _load_json(os.path.join(profiles_dir, "watchlist.json")) or []
    actors = _load_json(os.path.join(profiles_dir, "actors.json")) or []
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
            price_fallback = float(meta.get("price_usd") or 0.0)

            # resume via progress
            progress_key = f"{chain}:{addr}:{window_start}:{window_end}"
            progress.setdefault(progress_key, {})
            progress[progress_key]["started_bucket"] = window_start // 900
            progress[progress_key].setdefault("started_at", datetime.now(timezone.utc).isoformat())
            resume_bucket = progress.get(progress_key, {}).get("committed_bucket") or progress.get(progress_key, {}).get("last_bucket")

            pnl_before = pnl_during = pnl_after = 0.0
            tx_count = 0
            txs: Optional[List[Dict[str, Any]]] = []
            diag: Dict[str, Any] = {}
            has_sell_tx = False
            price_missing = False
            last_seen_tx_ts = 0
            if cov_enabled:
                cache_path = os.path.join(
                    CACHE_TX_DIR,
                    f"{chain}_{addr}_{window_start}_{window_end}.json",
                )
                if os.path.exists(cache_path):
                    try:
                        with open(cache_path, "r", encoding="utf-8") as f:
                            txs = json.load(f) or []
                        cache_hits += 1
                        cache_hits_global += 1
                    except Exception:
                        txs = []
                if not txs:
                    txs, diag = cov_client.iter_transactions_window(
                        chain=chain,
                        address=addr,
                        start_ts=window_start if resume_bucket is None else max(window_start, (resume_bucket + 1) * 900),
                        end_ts=window_end,
                    )
                    if txs is None:
                        txs = []
                        cov_success = False
                        fail_reason = fail_reason or "fetch_failed"
                    if txs:
                        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                        with open(cache_path, "w", encoding="utf-8") as f:
                            json.dump(txs, f)
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
                amt = float(tx.get("amount") or 0.0)
                usd_val = tx.get("usd_value")
                if usd_val is None:
                    usd_val = amt * price_fallback
                    if price_fallback <= 0:
                        price_missing = True
                signed_usd = float(usd_val) if amt >= 0 else -float(usd_val)
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
                if amt < 0:
                    has_sell_tx = True
                last_seen_tx_ts = max(last_seen_tx_ts, ts)

                # normalized tx for warehouse
                normalized_txs.append(
                    {
                        "address": addr,
                        "chain": chain,
                        "symbol": meta.get("symbol"),
                        "token_address": tx.get("token_address"),
                        "token_symbol": tx.get("token_symbol"),
                        "amount": amt,
                        "usd_value": tx.get("usd_value"),
                        "fees_usd": fee_usd,
                        "timestamp": ts,
                        "phase": phase,
                        "pump_start_ts": pump_start_ts,
                        "pump_end_ts": pump_end_ts,
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
                tb["usd_volume"] += abs(float(tx.get("usd_value") or 0.0))
                tb["fees_usd"] += fee_usd
                # linked heuristic
                from_addr = (
                    tx.get("from_address") or tx.get("from") or tx.get("from_addr")
                )
                to_addr = tx.get("to_address") or tx.get("to") or tx.get("to_addr")
                direction = None
                if from_addr and str(from_addr).lower() != addr and signed_usd < 0:
                    other_addr = str(from_addr).lower()
                    direction = "out"
                elif to_addr and str(to_addr).lower() != addr and signed_usd > 0:
                    other_addr = str(to_addr).lower()
                    direction = "in"
                else:
                    other_addr = None
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
                    lnk["total_value_usd"] += abs(signed_usd)
                    if lnk["direction"] == "mixed":
                        lnk["direction"] = direction
                    elif lnk["direction"] != direction:
                        lnk["direction"] = "mixed"

                # summary by token
                token_key = f"{chain}:{tx.get('token_address')}"
                tok = summary_tokens.setdefault(
                    token_key,
                    {
                        "chain": chain,
                        "token_address": tx.get("token_address"),
                        "token_symbol": tx.get("token_symbol"),
                        "tx_count": 0,
                        "usd_volume": 0.0,
                    },
                )
                tok["tx_count"] += 1
                tok["usd_volume"] += abs(float(tx.get("usd_value") or 0.0))
                # per actor token summary
                actor_tok_key = f"{key}:{tx.get('token_address')}"
                atok = summary_tokens_by_actor.setdefault(
                    actor_tok_key,
                    {
                        "address": addr,
                        "chain": chain,
                        "token_address": tx.get("token_address"),
                        "token_symbol": tx.get("token_symbol"),
                        "tx_count": 0,
                        "usd_volume": 0.0,
                    },
                )
                atok["tx_count"] += 1
                atok["usd_volume"] += abs(float(tx.get("usd_value") or 0.0))

            roi_reason = None
            if not cov_success:
                roi_reason = fail_reason or "covalent_partial"
                if tx_count == 0 or not has_sell_tx:
                    roi_reason = "no_sells"
                elif price_missing:
                    roi_reason = "missing_entry_price" if price_fallback > 0 else "price_unavailable"
            else:
                # успешный fetch, но ROI мог не посчитаться
                if tx_count == 0 or not has_sell_tx:
                    roi_reason = "no_sells"
                elif price_missing:
                    roi_reason = "missing_entry_price" if price_fallback > 0 else "price_unavailable"
                elif pnl_before is None and pnl_during is None and pnl_after is None:
                    roi_reason = "price_unavailable"
            pumps_out.append(
                {
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
                    "pnl_total_usd": (pnl_before + pnl_during + pnl_after)
                    if cov_success
                    else None,
                    "roi_reason": roi_reason,
                    "has_sell_tx": has_sell_tx,
                    "price_missing": price_missing,
                    "tx_count": tx_count,
                    "covalent_used_chain_name": diag.get("chain_name"),
                    "start_bucket": diag.get("start_bucket"),
                    "end_bucket": diag.get("end_bucket"),
                    "buckets_fetched": diag.get("buckets_fetched"),
                    "tx_items_count": diag.get("tx_items_count"),
                    "errors_count": diag.get("errors_count"),
                    "cache_hits": diag.get("cache_hits"),
                    "http_429_count": diag.get("http_429_count"),
                    "sleep_seconds_total": diag.get("sleep_seconds_total"),
                    "timeline_buckets": sorted(
                        timeline_buckets.values(), key=lambda x: x["bucket"]
                    ),
                    "pnl_mode": "value_minus_fees",
                }
            )

            total_before += pnl_before
            total_during += pnl_during
            total_after += pnl_after

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

        prev_actor = prev_actors_map.get(f"{addr}:{chain}") or {}
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
            }
        )

    cache_meta.update(
        {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "cache_hits": cache_hits_global,
            "cache_misses": cache_miss_global,
            "api_calls": api_calls_global,
            "buckets_cached": cache_hits_global,
            "buckets_missing": max(0, buckets_needed_global - cache_hits_global - cache_miss_global),
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
    tx_csv = os.path.join(WAREHOUSE_DIR, "tx_normalized.csv")
    if normalized_txs:
        headers = list({k for tx in normalized_txs for k in tx.keys()})
        with open(tx_csv, "w", encoding="utf-8") as f:
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
    with open(
        os.path.join(WAREHOUSE_DIR, "summary_wallets.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(summary_wallets, f, ensure_ascii=False, indent=2)

    if summary_tokens:
        with open(
            os.path.join(WAREHOUSE_DIR, "summary_tokens.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(list(summary_tokens.values()), f, ensure_ascii=False, indent=2)
    if summary_tokens_by_actor:
        with open(
            os.path.join(WAREHOUSE_DIR, "summary_tokens_by_actor.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                list(summary_tokens_by_actor.values()), f, ensure_ascii=False, indent=2
            )
    # features artifact for hunter consumption
    with open(
        os.path.join(WAREHOUSE_DIR, "features.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(features, f, ensure_ascii=False, indent=2)

    # timelines artifact
    timeline_artifact = _build_timeline(normalized_txs, bucket_minutes=1)
    _write_json_atomic(timeline_artifact, os.path.join(WAREHOUSE_DIR, "..", "timeline_watchlist.json"))

    # holdings overlap
    overlap = _build_holdings_overlap(actors_out)
    _write_json_atomic(overlap, os.path.join(WAREHOUSE_DIR, "..", "holdings_overlap.json"))

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
    net_out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": nodes,
        "edges": edges,
    }
    _write_json_atomic(net_out, os.path.join(WAREHOUSE_DIR, "..", "network_watchlist.json"))
    # cache health artifact
    cache_health = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cache_version": CACHE_VERSION,
        "cache_meta": cache_meta_nested,
        "report": cache_meta_nested.get("report", {}),
        "progress": _load_progress(),
    }
    _write_json_atomic(cache_health, CACHE_HEALTH_PATH)

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

    return output_path

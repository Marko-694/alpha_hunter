import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .actors_builder import _load_json  # reuse loader
from .covalent_client import CovalentClient

PROGRESS_PATH = "data/alpha_profiler/pnl_progress.json"
WAREHOUSE_DIR = "data/alpha_profiler/warehouse"
CACHE_TX_DIR = "data/alpha_profiler/cache_tx"


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
    cov_cfg = config.get("covalent", {}) or {}
    cov_client = CovalentClient(
        base_url=cov_cfg.get("base_url", "https://api.covalenthq.com"),
        api_key=cov_cfg.get("api_key", ""),
        logger=logger,
        max_calls_per_minute=int(cov_cfg.get("max_calls_per_minute") or 0) or None,
    )
    cov_enabled = bool(cov_cfg.get("enabled")) and bool(cov_client.api_key)

    watchlist = _load_json(os.path.join(profiles_dir, "watchlist.json")) or []
    actors = _load_json(os.path.join(profiles_dir, "actors.json")) or []
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
    cache_hits_global = 0

    limit = os.getenv("PWL_LIMIT")
    if limit:
        try:
            limit = int(limit)
        except Exception:
            limit = None

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
        http_429_count = 0
        sleep_seconds_total = 0.0
        linked_map: Dict[str, Dict[str, Any]] = {}
        fees_total = 0.0
        timeline_buckets: Dict[int, Dict[str, Any]] = {}

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
            meta = prof.get("meta") or {}
            pump_start_ts = int(meta.get("start_ts") or meta.get("pump_start_ts") or 0)
            pump_end_ts = int(meta.get("end_ts") or meta.get("pump_end_ts") or 0)
            if pump_start_ts <= 0:
                pump_start_ts = int(
                    parse_dt_unsafe(meta.get("pump_start", "")).timestamp()
                )
            if pump_end_ts <= 0:
                pump_end_ts = int(parse_dt_unsafe(meta.get("pump_end", "")).timestamp())

            window_start = pump_start_ts - pre_days * 86400
            window_end = pump_end_ts + post_hours * 3600
            price_fallback = float(meta.get("price_usd") or 0.0)

            # resume via progress
            progress_key = f"{chain}:{addr}:{window_start}:{window_end}"
            resume_bucket = progress.get(progress_key, {}).get("last_bucket")

            pnl_before = pnl_during = pnl_after = 0.0
            tx_count = 0
            txs: Optional[List[Dict[str, Any]]] = []
            diag: Dict[str, Any] = {}
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
                        start_ts=window_start,
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
                    http_429_count += diag.get("http_429_count", 0) or 0
                    sleep_seconds_total += diag.get("sleep_seconds_total", 0.0) or 0.0
                    last_bucket_processed = diag.get("last_bucket_processed")
                    if last_bucket_processed is not None:
                        progress[progress_key] = {
                            "last_bucket": last_bucket_processed,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "buckets_done": diag.get("buckets_fetched", 0) or 0,
                        }
                        _save_progress(progress)
            else:
                cov_success = False

            for tx in txs:
                amt = float(tx.get("amount") or 0.0)
                usd_val = tx.get("usd_value")
                if usd_val is None:
                    usd_val = amt * price_fallback
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

        if cov_success:
            success_cnt += 1
        else:
            fail_cnt += 1
            fr = fail_reason or "unknown"
            fail_reasons[fr] = fail_reasons.get(fr, 0) + 1

        actors_out.append(
            {
                "address": addr,
                "chain": chain,
                "pumps_count": pumps_count,
                "best_pamper_level": best_level,
                "actor_score": actor_score,
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

    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pre_days": pre_days,
            "post_hours": post_hours,
            "max_buckets_per_actor": 600,
            "success": success_cnt,
            "failed": fail_cnt,
            "fail_reasons": fail_reasons,
            "cache_hits": cache_hits_global,
        },
        "actors": actors_out,
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
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

    # timelines artifact
    timeline_artifact = _build_timeline(normalized_txs, bucket_minutes=1)
    with open(
        os.path.join(WAREHOUSE_DIR, "..", "timeline_watchlist.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(timeline_artifact, f, ensure_ascii=False, indent=2)

    # holdings overlap
    overlap = _build_holdings_overlap(actors_out)
    with open(
        os.path.join(WAREHOUSE_DIR, "..", "holdings_overlap.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(overlap, f, ensure_ascii=False, indent=2)

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
    with open(
        os.path.join(WAREHOUSE_DIR, "..", "network_watchlist.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(net_out, f, ensure_ascii=False, indent=2)

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

import json
import os
import time
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .logger import get_logger
from .explorer_client import ExplorerClient
from .price_client import PriceClient
from .nansen_client import NansenClient
from .birdeye_client import fetch_for_window

DUMP_WINDOW_DEFAULT_MINUTES = 60


@dataclass
class PumpWindowConfig:
    symbol: str
    chain: str
    contract: str
    start_ts: int
    end_ts: int
    pre_window_minutes: int
    min_whale_net_usd: float
    use_nansen: bool = False
    pump_start: Optional[datetime] = None
    pump_end: Optional[datetime] = None


def ensure_alpha_profiler_dir(base_dir: str = "data/alpha_profiler") -> str:
    os.makedirs(base_dir, exist_ok=True)
    return base_dir


def save_json(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _calculate_pamper_score(stats: Dict[str, Any]) -> Dict[str, Any]:
    score = 0.0
    factors: Dict[str, float] = {}

    balance_before = stats.get("balance_before_pump_tokens", 0.0)
    sold_ratio = stats.get("sold_after_peak_ratio", 0.0)
    exit_speed = stats.get("exit_speed_seconds")

    if balance_before > 0:
        score += 2.0
        factors["early_holder"] = 2.0

    pre_buy_usd = max(stats.get("pre_buy_usd", 0.0), 1.0)
    pump_buy_usd = stats.get("pump_buy_usd", 0.0)
    buy_ratio = pump_buy_usd / pre_buy_usd
    if buy_ratio > 3:
        add = 2.0
        if buy_ratio > 5:
            add = 4.0
        score += add
        factors["aggressive_buy_ratio"] = add

    if sold_ratio > 0.5:
        add = 3.0
        if sold_ratio > 0.8:
            add = 6.0
        if sold_ratio > 0.95:
            add = 10.0
        score += add
        factors["dump_on_highs"] = add

    if exit_speed is not None:
        if exit_speed <= 120:
            score += 5.0
            factors["fast_exit"] = 5.0
        elif exit_speed <= 600:
            score += 3.0
            factors["medium_exit"] = 3.0

    if stats.get("pre_tx_count", 0) >= 3:
        score += 1.0
        factors["pre_activity"] = 1.0
    if stats.get("pump_tx_count", 0) >= 5:
        score += 3.0
        factors["pump_activity"] = 3.0

    if balance_before == 0 and stats.get("pump_buy_usd", 0.0) < stats.get("dump_sell_usd", 0.0):
        score -= 1.0
        factors["likely_retail"] = -1.0

    stats["pamper_score"] = score
    stats["pamper_factors"] = factors
    return stats


def analyze_single_pump(
    cfg: PumpWindowConfig,
    explorer_client: Optional[ExplorerClient],
    price_client: PriceClient,
    nansen_client: Optional[NansenClient] = None,
    logger=None,
    output_dir: str = "data/alpha_profiler",
    retention_days: int = 30,
) -> str:
    logger = logger or get_logger("alpha_profiler")

    pump_start_dt = cfg.pump_start or datetime.fromtimestamp(cfg.start_ts, tz=timezone.utc)
    pump_end_dt = cfg.pump_end or datetime.fromtimestamp(cfg.end_ts, tz=timezone.utc)

    price_usd = price_client.get_price_usd(
        symbol=cfg.symbol,
        chain=cfg.chain,
        contract=cfg.contract,
    )
    if price_usd is None:
        logger.warning(
            "Price for %s (chain=%s, contract=%s) not found, aborting analysis",
            cfg.symbol,
            cfg.chain,
            cfg.contract,
        )
        return ""

    dump_window_minutes = DUMP_WINDOW_DEFAULT_MINUTES
    pre_start_ts = cfg.start_ts - cfg.pre_window_minutes * 60
    pump_start_ts = cfg.start_ts
    pump_end_ts = cfg.end_ts
    dump_end_ts = pump_end_ts + dump_window_minutes * 60
    # гарантируем корректный порядок времён для BirdEye
    if pre_start_ts > dump_end_ts:
        logger.warning(
            "[%s] pre_start_ts > dump_end_ts (%s > %s), swapping to satisfy BirdEye",
            cfg.symbol,
            pre_start_ts,
            dump_end_ts,
        )
        pre_start_ts, dump_end_ts = dump_end_ts, pre_start_ts

    window_start_dt = datetime.fromtimestamp(pre_start_ts, tz=timezone.utc)
    window_end_dt = datetime.fromtimestamp(pump_end_ts, tz=timezone.utc)
    dump_end_dt = datetime.fromtimestamp(dump_end_ts, tz=timezone.utc)

    transfers: List[Dict[str, Any]] = []
    if cfg.chain == "bsc":
        label = f"{cfg.symbol}_BIRDEYE"
        transfers = fetch_for_window(
            token_address=cfg.contract,
            start_ts=pre_start_ts,
            end_ts=dump_end_ts,
            label=label,
            logger=logger,
        )
        unique_wallets = len({str(t.get("owner") or t.get("from") or "").lower() for t in transfers})
        logger.info(
            "BirdEye returned %d txs for %s (unique wallets: %d)",
            len(transfers),
            cfg.contract,
            unique_wallets,
        )
    elif explorer_client is not None:
        transfers = explorer_client.get_recent_token_transfers(
            chain=cfg.chain, address=cfg.contract, start_block=None, sort="asc"
        )

    if not transfers:
        logger.warning("No transfers fetched for %s on %s", cfg.contract, cfg.chain)
        transfers = []

    wallet_stats: Dict[str, Dict[str, Any]] = {}

    def _phase(ts_dt: datetime) -> str:
        ts = int(ts_dt.timestamp())
        if ts < pump_start_ts:
            return "pre"
        if ts <= pump_end_ts:
            return "pump"
        if ts <= dump_end_ts:
            return "dump"
        return "later"

    if cfg.chain == "bsc":
        for tx in transfers:
            try:
                ts_int = int(tx.get("block_unix_time") or tx.get("timestamp") or 0)
                ts_dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
                phase = _phase(ts_dt)
                if phase == "later":
                    continue

                side = str(tx.get("side") or "").lower()
                wallet = str(tx.get("owner") or tx.get("from") or tx.get("from_address") or "").lower()
                token_amount = float(tx.get("volume") or 0.0)
                usd_amount = float(tx.get("volume_usd") or 0.0)
                if usd_amount == 0 and token_amount != 0:
                    usd_amount = token_amount * price_usd

                if side == "sell":
                    token_amount = -abs(token_amount)
                    usd_amount = -abs(usd_amount)
                elif side == "buy":
                    token_amount = abs(token_amount)
                    usd_amount = abs(usd_amount)
                else:
                    continue

                entry = wallet_stats.setdefault(
                    wallet,
                    {
                        "net_tokens": 0.0,
                        "net_usd": 0.0,
                        "tx_count": 0,
                        "first_ts": ts_dt,
                        "last_ts": ts_dt,
                        "pre_buy_tokens": 0.0,
                        "pre_sell_tokens": 0.0,
                        "pump_buy_tokens": 0.0,
                        "pump_sell_tokens": 0.0,
                        "dump_buy_tokens": 0.0,
                        "dump_sell_tokens": 0.0,
                        "pre_buy_usd": 0.0,
                        "pre_sell_usd": 0.0,
                        "pump_buy_usd": 0.0,
                        "pump_sell_usd": 0.0,
                        "dump_buy_usd": 0.0,
                        "dump_sell_usd": 0.0,
                        "pre_tx_count": 0,
                        "pump_tx_count": 0,
                        "dump_tx_count": 0,
                        "exit_first_sell_ts": None,
                    },
                )

                entry["net_tokens"] += token_amount
                entry["net_usd"] += usd_amount
                entry["tx_count"] += 1
                entry["first_ts"] = min(entry["first_ts"], ts_dt)
                entry["last_ts"] = max(entry["last_ts"], ts_dt)

                if phase == "pre":
                    if token_amount > 0:
                        entry["pre_buy_tokens"] += token_amount
                        entry["pre_buy_usd"] += max(usd_amount, 0.0)
                    else:
                        entry["pre_sell_tokens"] += -token_amount
                        entry["pre_sell_usd"] += -min(usd_amount, 0.0)
                    entry["pre_tx_count"] += 1
                elif phase == "pump":
                    if token_amount > 0:
                        entry["pump_buy_tokens"] += token_amount
                        entry["pump_buy_usd"] += max(usd_amount, 0.0)
                    else:
                        entry["pump_sell_tokens"] += -token_amount
                        entry["pump_sell_usd"] += -min(usd_amount, 0.0)
                    entry["pump_tx_count"] += 1
                    if token_amount < 0 and entry["exit_first_sell_ts"] is None:
                        entry["exit_first_sell_ts"] = ts_dt
                elif phase == "dump":
                    if token_amount > 0:
                        entry["dump_buy_tokens"] += token_amount
                        entry["dump_buy_usd"] += max(usd_amount, 0.0)
                    else:
                        entry["dump_sell_tokens"] += -token_amount
                        entry["dump_sell_usd"] += -min(usd_amount, 0.0)
                        if entry["exit_first_sell_ts"] is None:
                            entry["exit_first_sell_ts"] = ts_dt
                    entry["dump_tx_count"] += 1
            except Exception:
                continue
    else:
        def _update_wallet(address: str, delta: float, ts: datetime) -> None:
            entry = wallet_stats.setdefault(
                address,
                {"in_amount": 0.0, "out_amount": 0.0, "net_amount": 0.0, "first_ts": ts, "last_ts": ts, "tx_count": 0},
            )
            if delta > 0:
                entry["in_amount"] += delta
            else:
                entry["out_amount"] += -delta
            entry["net_amount"] += delta
            entry["tx_count"] += 1
            if ts < entry["first_ts"]:
                entry["first_ts"] = ts
            if ts > entry["last_ts"]:
                entry["last_ts"] = ts

        for tx in transfers:
            try:
                ts_int = int(tx.get("timestamp") or tx.get("timeStamp") or 0)
                ts_dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
                if ts_dt < window_start_dt or ts_dt > window_end_dt:
                    continue

                token_decimals = int(tx.get("tokenDecimal") or 18)
                value_raw = int(tx.get("value_raw") or tx.get("value") or 0)
                amount = value_raw / (10**token_decimals) if token_decimals >= 0 else float(value_raw)
                from_addr = str(tx.get("from") or tx.get("fromAddress") or "").lower()
                to_addr = str(tx.get("to") or tx.get("toAddress") or "").lower()
            except Exception:
                continue

            if to_addr:
                _update_wallet(to_addr, amount, ts_dt)
            if from_addr:
                _update_wallet(from_addr, -amount, ts_dt)

    # compute total pump volume for share and prepare scores
    for stats in wallet_stats.values():
        stats["pump_volume_usd_total"] = stats.get("pump_buy_usd", 0.0) + abs(stats.get("pump_sell_usd", 0.0))
    total_pump_volume_usd = sum(s["pump_volume_usd_total"] for s in wallet_stats.values()) or 1.0

    pamper_scores_all: List[float] = []
    whales: List[Dict[str, Any]] = []
    for addr, stats in wallet_stats.items():
        if cfg.chain == "bsc":
            balance_before = stats.get("pre_buy_tokens", 0.0) - stats.get("pre_sell_tokens", 0.0)
            balance_peak = balance_before + (stats.get("pump_buy_tokens", 0.0) - stats.get("pump_sell_tokens", 0.0))
            sold_dump_tokens = stats.get("dump_sell_tokens", 0.0)
            sold_dump_usd = stats.get("dump_sell_usd", 0.0)

            exit_first_sell_ts = stats.get("exit_first_sell_ts")
            exit_speed_seconds = None
            if exit_first_sell_ts:
                exit_speed_seconds = (exit_first_sell_ts - pump_end_dt).total_seconds()

            stats["balance_before_pump_tokens"] = max(balance_before, 0.0)
            stats["balance_at_peak_tokens"] = max(balance_peak, 0.0)
            stats["sold_after_peak_tokens"] = sold_dump_tokens
            stats["sold_after_peak_usd"] = sold_dump_usd
            stats["sold_after_peak_ratio"] = (
                sold_dump_tokens / stats["balance_at_peak_tokens"] if stats["balance_at_peak_tokens"] > 0 else 0.0
            )
            stats["exit_speed_seconds"] = exit_speed_seconds
            stats["net_amount"] = stats.get("net_tokens", 0.0)
            stats["net_usd"] = stats.get("net_usd", stats["net_amount"] * price_usd)

            invested_usd = stats.get("pre_buy_usd", 0.0) + stats.get("pump_buy_usd", 0.0)
            cashed_out_usd = (
                abs(stats.get("pre_sell_usd", 0.0))
                + abs(stats.get("pump_sell_usd", 0.0))
                + abs(stats.get("dump_sell_usd", 0.0))
            )
            realized_pnl_usd = cashed_out_usd - invested_usd
            roi = realized_pnl_usd / invested_usd if invested_usd > 0 else 0.0
            stats["realized_pnl_usd"] = realized_pnl_usd
            stats["roi"] = roi

            stats = _calculate_pamper_score(stats)
            pamper_scores_all.append(stats.get("pamper_score", 0.0))

            threshold_usd = getattr(cfg, "min_whale_net_usd", None)
            if threshold_usd is None:
                threshold_usd = getattr(cfg, "min_whale_buy_usd", 0)

            if stats["net_usd"] >= threshold_usd:
                pump_volume_wallet = stats["pump_volume_usd_total"]
                whales.append(
                    {
                        "address": addr,
                        "net_amount": stats["net_amount"],
                        "net_usd": stats["net_usd"],
                        "tx_count": stats["tx_count"],
                        "first_ts": stats["first_ts"].isoformat(),
                        "last_ts": stats["last_ts"].isoformat(),
                        "net_tokens": stats["net_tokens"],
                        "pre_buy_usd": stats.get("pre_buy_usd", 0.0),
                        "pump_buy_usd": stats.get("pump_buy_usd", 0.0),
                        "dump_sell_usd": stats.get("dump_sell_usd", 0.0),
                        "balance_before_pump_tokens": stats.get("balance_before_pump_tokens", 0.0),
                        "balance_at_peak_tokens": stats.get("balance_at_peak_tokens", 0.0),
                        "sold_after_peak_tokens": stats.get("sold_after_peak_tokens", 0.0),
                        "sold_after_peak_usd": stats.get("sold_after_peak_usd", 0.0),
                        "sold_after_peak_ratio": stats.get("sold_after_peak_ratio", 0.0),
                        "exit_speed_seconds": stats.get("exit_speed_seconds"),
                        "pamper_score": stats.get("pamper_score", 0.0),
                        "pamper_factors": stats.get("pamper_factors", {}),
                        "share_of_pump_volume": pump_volume_wallet / total_pump_volume_usd,
                        "realized_pnl_usd": stats.get("realized_pnl_usd", 0.0),
                        "roi": stats.get("roi", 0.0),
                    }
                )
        else:
            net_amount = stats["net_amount"]
            net_usd = net_amount * price_usd
            first_ts = stats["first_ts"]
            last_ts = stats["last_ts"]
            tx_count = stats["tx_count"]

            threshold_usd = getattr(cfg, "min_whale_net_usd", None)
            if threshold_usd is None:
                threshold_usd = getattr(cfg, "min_whale_buy_usd", 0)

            if net_usd >= threshold_usd:
                whales.append(
                    {
                        "address": addr,
                        "net_amount": net_amount,
                        "net_usd": net_usd,
                        "tx_count": tx_count,
                        "first_ts": first_ts.isoformat(),
                        "last_ts": last_ts.isoformat(),
                    }
                )

    if pamper_scores_all:
        sorted_scores = sorted(pamper_scores_all)
        n_scores = len(sorted_scores)

        def _percentile(score: float) -> float:
            count = sum(1 for s in sorted_scores if s <= score)
            return count / n_scores if n_scores > 0 else 0.0

        def _classify(level_score: float, share: float) -> str:
            if level_score >= 18 and share > 0.02:
                return "core_operator"
            if level_score >= 12 and share > 0.01:
                return "inner_circle"
            if level_score >= 7:
                return "outer_circle"
            return "retail"

        for w in whales:
            score = float(w.get("pamper_score", 0.0))
            share = float(w.get("share_of_pump_volume", 0.0))
            w["pamper_percentile"] = _percentile(score)
            w["pamper_level"] = _classify(score, share)

    whales.sort(key=lambda w: w.get("pamper_score", w.get("net_usd", 0.0)), reverse=True)

    if cfg.use_nansen and nansen_client is not None and whales:
        top_wallets = whales[:50]
        for w in top_wallets:
            addr = w.get("address")
            if not addr:
                continue
            try:
                labels_resp = nansen_client.holders({"wallet": addr}) or {}
                labels = labels_resp.get("labels") or labels_resp.get("data") or []
                if isinstance(labels, list):
                    w["nansen_tags"] = [str(l) for l in labels]
            except Exception:
                continue

    nansen_summary = {}
    if cfg.use_nansen and nansen_client is not None:
        try:
            resp = nansen_client.smart_money_netflows(
                {"chain": cfg.chain, "token_address": cfg.contract, "window": "24h"}
            )
            if resp and isinstance(resp, dict):
                nansen_summary = {
                    "smart_money_net_inflow_usd": float(resp.get("netflow_usd", 0) or 0.0),
                    "active_smart_wallets": int(resp.get("smart_wallets", 0) or 0),
                }
        except Exception:
            nansen_summary = {}

    result = {
        "meta": {
            "symbol": cfg.symbol,
            "chain": cfg.chain,
            "contract": cfg.contract,
            "pump_start": pump_start_dt.isoformat(),
            "pump_end": pump_end_dt.isoformat(),
            "pre_window_minutes": cfg.pre_window_minutes,
            "dump_window_minutes": dump_window_minutes,
            "min_whale_net_usd": getattr(cfg, "min_whale_net_usd", None) or getattr(cfg, "min_whale_buy_usd", 0),
            "start_ts": cfg.start_ts,
            "end_ts": cfg.end_ts,
            "price_usd": price_usd,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "wallets": whales,
    }
    if nansen_summary:
        result["nansen"] = nansen_summary

    ensure_alpha_profiler_dir(output_dir)
    filename = f"{cfg.symbol}_{cfg.chain}_{int(time.time())}_profile.json"
    path = os.path.join(output_dir, filename)
    save_json(result, path)
    logger.info("Saved pump profile to %s", path)

    try:
        profile_dir = Path(output_dir)
        cleanup_old_profiles(profile_dir, retention_days, logger)
    except Exception as exc:
        logger.debug("Cleanup profiles failed: %s", exc)

    return path


def build_clusters(
    profiles_dir: str = "data/alpha_profiler",
    output_path: str = "data/alpha_profiler/clusters.json",
    logger=None,
) -> str:
    logger = logger or get_logger("alpha_profiler_clusters")

    clusters: Dict[str, Dict[str, Any]] = {}
    if not os.path.isdir(profiles_dir):
        ensure_alpha_profiler_dir(profiles_dir)
        save_json([], output_path)
        return output_path

    for name in os.listdir(profiles_dir):
        if not name.endswith("_profile.json"):
            continue
        path = os.path.join(profiles_dir, name)
        data = load_json(path)
        if not isinstance(data, dict):
            continue
        meta = data.get("meta") or {}
        wallets = data.get("wallets") or []
        symbol = meta.get("symbol", "")
        chain = meta.get("chain", "")

        for w in wallets:
            addr = str(w.get("address") or "").lower()
            if not addr:
                continue
            net_usd = float(w.get("net_usd") or 0.0)
            entry = clusters.setdefault(
                addr,
                {
                    "address": addr,
                    "tokens": set(),
                    "chains": set(),
                    "pumps_participated": 0,
                    "total_net_usd": 0.0,
                },
            )
            if symbol:
                entry["tokens"].add(symbol)
            if chain:
                entry["chains"].add(chain)
            entry["pumps_participated"] += 1
            entry["total_net_usd"] += net_usd

    cluster_list: List[Dict[str, Any]] = []
    for addr, entry in clusters.items():
        cluster_list.append(
            {
                "address": addr,
                "tokens": sorted(list(entry["tokens"])),
                "chains": sorted(list(entry["chains"])),
                "pumps_participated": entry["pumps_participated"],
                "total_net_usd": entry["total_net_usd"],
            }
        )

    save_json(cluster_list, output_path)
    logger.info("Saved clusters to %s", output_path)
    return output_path


def cleanup_old_profiles(base_dir: Path, retention_days: int, logger: logging.Logger) -> None:
    """
    Удаляет JSON-профили пампов старше retention_days из каталога base_dir (clusters.json не трогаем).
    """
    if retention_days < 0:
        return
    if not base_dir.exists():
        return
    cutoff = time.time() - retention_days * 86400
    for path in base_dir.glob("*_profile.json"):
        try:
            if path.name == "clusters.json":
                continue
            if path.stat().st_mtime < cutoff:
                path.unlink()
                logger.info("Deleted old profile %s", path)
        except Exception as exc:
            logger.debug("Failed to delete %s: %s", path, exc)


def build_actors_and_watchlist(
    base_dir: str = "data/alpha_profiler",
    logger: Optional[logging.Logger] = None,
) -> Dict[str, str]:
    """
    Строит агрегированный реестр акторов и watchlist на основе всех *_profile.json.
    """
    log = logger or get_logger("actors_builder")
    profiles_dir = base_dir
    actors_path = os.path.join(base_dir, "actors.json")
    watchlist_path = os.path.join(base_dir, "watchlist.json")

    level_priority = {"core_operator": 3, "inner_circle": 2, "outer_circle": 1, "retail": 0}

    def best_level(levels: List[str]) -> str:
        return max(levels, key=lambda x: level_priority.get(x, -1)) if levels else "retail"

    actors: Dict[tuple, Dict[str, Any]] = {}
    if not os.path.isdir(profiles_dir):
        log.warning("Profiles dir %s not found", profiles_dir)
        return {"actors": actors_path, "watchlist": watchlist_path}

    profiles = [f for f in os.listdir(profiles_dir) if f.endswith("_profile.json")]
    log.info("Building actors from %d profiles", len(profiles))

    for fname in profiles:
        path = os.path.join(profiles_dir, fname)
        data = load_json(path)
        if not isinstance(data, dict):
            continue
        meta = data.get("meta") or {}
        wallets = data.get("wallets") or []
        symbol = meta.get("symbol", "")
        chain = meta.get("chain", "")
        pump_start = meta.get("pump_start")

        for w in wallets:
            addr = str(w.get("address") or "").lower()
            if not addr:
                continue
            pamper_score = float(w.get("pamper_score") or 0.0)
            pamper_level = str(w.get("pamper_level") or "retail")
            realized_pnl_usd = float(w.get("realized_pnl_usd") or 0.0)
            roi_val = w.get("roi")
            roi = float(roi_val) if roi_val is not None else None
            share = float(w.get("share_of_pump_volume") or 0.0)

            key = (chain, addr)
            actor = actors.setdefault(
                key,
                {
                    "address": addr,
                    "chain": chain,
                    "pumps": [],
                    "pamper_scores": [],
                    "pamper_levels": [],
                    "pnl_list": [],
                    "roi_list": [],
                    "share_list": [],
                },
            )
            actor["pumps"].append(
                {
                    "profile_file": fname,
                    "symbol": symbol,
                    "chain": chain,
                    "pump_start": pump_start,
                    "pamper_score": pamper_score,
                    "pamper_level": pamper_level,
                    "realized_pnl_usd": realized_pnl_usd,
                    "roi": roi,
                    "share_of_pump_volume": share,
                }
            )
            actor["pamper_scores"].append(pamper_score)
            actor["pamper_levels"].append(pamper_level)
            actor["pnl_list"].append(realized_pnl_usd)
            if roi is not None:
                actor["roi_list"].append(roi)
            actor["share_list"].append(share)

    actors_list: List[Dict[str, Any]] = []
    for (_, addr), actor in actors.items():
        pumps = actor.get("pumps", [])
        scores = actor.get("pamper_scores", [])
        levels = actor.get("pamper_levels", [])
        pnl_list = actor.get("pnl_list", [])
        roi_list = actor.get("roi_list", [])
        share_list = actor.get("share_list", [])

        pumps_count = len(pumps)
        max_score = max(scores) if scores else 0.0
        avg_score = sum(scores) / len(scores) if scores else 0.0
        avg_roi = sum(roi_list) / len(roi_list) if roi_list else 0.0
        total_pnl = sum(pnl_list)
        avg_share = sum(share_list) / len(share_list) if share_list else 0.0
        best_level_val = best_level(levels)
        actor_score = max_score * math.log(1 + pumps_count) * (1 + max(avg_roi, 0.0))

        actors_list.append(
            {
                "address": actor.get("address", addr),
                "chain": actor.get("chain", ""),
                "pumps_count": pumps_count,
                "pumps": pumps,
                "max_pamper_score": max_score,
                "avg_pamper_score": avg_score,
                "best_pamper_level": best_level_val,
                "total_realized_pnl_usd": total_pnl,
                "avg_roi": avg_roi,
                "avg_share_of_pump_volume": avg_share,
                "actor_score": actor_score,
            }
        )

    actors_list.sort(key=lambda x: x.get("actor_score", 0.0), reverse=True)
    save_json(actors_list, actors_path)
    log.info("Saved %d actors to %s", len(actors_list), actors_path)

    watchlist = [
        {
            "address": a.get("address", ""),
            "chain": a.get("chain", ""),
            "pumps_count": a.get("pumps_count", 0),
            "best_pamper_level": a.get("best_pamper_level", ""),
            "actor_score": a.get("actor_score", 0.0),
            "avg_roi": a.get("avg_roi", 0.0),
            "avg_share_of_pump_volume": a.get("avg_share_of_pump_volume", 0.0),
        }
        for a in actors_list
        if a.get("pumps_count", 0) >= 2 and a.get("best_pamper_level") in {"core_operator", "inner_circle"}
    ]
    watchlist.sort(key=lambda x: x.get("actor_score", 0.0), reverse=True)
    save_json(watchlist, watchlist_path)
    log.info("Saved %d watchlist entries to %s", len(watchlist), watchlist_path)

    return {"actors": actors_path, "watchlist": watchlist_path}

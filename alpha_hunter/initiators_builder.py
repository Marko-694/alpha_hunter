import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .logger import get_logger

LEVEL_PRIORITY = {
    "core_operator": 3,
    "inner_circle": 2,
    "outer_circle": 1,
    "retail": 0,
}

LEVEL_BONUS = {
    "core_operator": 1.0,
    "inner_circle": 0.85,
    "outer_circle": 0.65,
    "retail": 0.35,
}


def _to_timestamp(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_initiators_index(
    profiles_dir: str = "data/alpha_profiler",
    output_path: str = "data/alpha_profiler/warehouse/initiators.json",
    *,
    logger=None,
    min_campaigns: int = 2,
) -> str:
    log = logger or get_logger("initiators_builder")
    base = Path(profiles_dir)
    if not base.exists():
        log.warning("Profiles directory %s not found", profiles_dir)
        return output_path

    cluster_map = _load_cluster_map(base.parent / "clusters.json")
    aggregations: Dict[Tuple[str, str], Dict[str, Any]] = {}

    profile_files = sorted(p for p in base.glob("*_profile.json") if p.is_file())
    for path in profile_files:
        data = _load_json(path)
        if not isinstance(data, dict):
            continue
        meta = data.get("meta") or {}
        symbol = str(meta.get("symbol") or "").upper()
        chain = str(meta.get("chain") or "").lower() or "unknown"
        campaign_id = path.name
        wallets = data.get("wallets") or []
        for wallet in wallets:
            if not isinstance(wallet, dict):
                continue
            addr = str(wallet.get("address") or "").lower()
            if not addr:
                continue
            key = (chain, addr)
            entry = aggregations.setdefault(
                key,
                {
                    "address": addr,
                    "chain": chain,
                    "symbols": set(),
                    "campaign_ids": set(),
                    "pre_buy": [],
                    "pump_buy": [],
                    "dump_sell": [],
                    "sold_ratio": [],
                    "exit_speed": [],
                    "share_volume": [],
                    "pamper_levels": [],
                    "pamper_percentiles": [],
                    "realized_pnl": [],
                    "tokens": set(),
                    "last_seen_ts": 0,
                    "tx_counts": [],
                },
            )
            entry["campaign_ids"].add(campaign_id)
            entry["symbols"].add(symbol)
            entry["tokens"].add(symbol)
            entry["pre_buy"].append(float(wallet.get("pre_buy_usd") or 0.0))
            entry["pump_buy"].append(float(wallet.get("pump_buy_usd") or 0.0))
            entry["dump_sell"].append(float(wallet.get("dump_sell_usd") or 0.0))
            entry["sold_ratio"].append(float(wallet.get("sold_after_peak_ratio") or 0.0))
            entry["share_volume"].append(float(wallet.get("share_of_pump_volume") or 0.0))
            entry["pamper_levels"].append(str(wallet.get("pamper_level") or "unknown").lower())
            entry["pamper_percentiles"].append(float(wallet.get("pamper_percentile") or 0.0))
            entry["realized_pnl"].append(float(wallet.get("realized_pnl_usd") or 0.0))
            exit_speed = wallet.get("exit_speed_seconds")
            if exit_speed is not None:
                entry["exit_speed"].append(float(exit_speed))
            tx_count = wallet.get("tx_count")
            if tx_count is not None:
                entry["tx_counts"].append(int(tx_count))
            last_ts = _to_timestamp(wallet.get("last_ts") or wallet.get("lastTimestamp"))
            if last_ts:
                entry["last_seen_ts"] = max(entry["last_seen_ts"], last_ts)

    initiators: List[Dict[str, Any]] = []
    for (chain, addr), entry in aggregations.items():
        campaigns_count = len(entry["campaign_ids"])
        if campaigns_count < min_campaigns:
            continue
        avg_pre = _avg(entry["pre_buy"])
        avg_pump = _avg(entry["pump_buy"])
        avg_dump = _avg(entry["dump_sell"])
        avg_sold_ratio = _avg(entry["sold_ratio"])
        avg_share = _avg(entry["share_volume"])
        avg_tx = _avg(entry["tx_counts"])
        median_exit = statistics.median(entry["exit_speed"]) if entry["exit_speed"] else 0.0
        pamper_percentile_mean = _avg(entry["pamper_percentiles"])
        best_level = _best_level(entry["pamper_levels"])
        total_realized = sum(entry["realized_pnl"])

        initiator_score = _compute_initiator_score(
            avg_pre_buy_usd=avg_pre,
            avg_sold_ratio=avg_sold_ratio,
            median_exit_seconds=median_exit,
            campaigns_count=campaigns_count,
            pamper_percentile=pamper_percentile_mean,
            pamper_level=best_level,
        )

        initiators.append(
            {
                "address": addr,
                "chain": chain,
                "cluster_id": cluster_map.get(addr),
                "campaigns_count": campaigns_count,
                "initiator_score": initiator_score,
                "avg_pre_buy_usd": avg_pre,
                "avg_pump_buy_usd": avg_pump,
                "avg_dump_sell_usd": avg_dump,
                "avg_sold_after_peak_ratio": avg_sold_ratio,
                "median_exit_speed_seconds": median_exit,
                "avg_share_of_pump_volume": avg_share,
                "best_pamper_level": best_level,
                "pamper_percentile_mean": pamper_percentile_mean,
                "total_realized_pnl_usd": total_realized,
                "tokens": sorted(entry["tokens"]),
                "last_seen_ts": entry["last_seen_ts"],
                "avg_tx_count": avg_tx,
            }
        )

    initiators.sort(key=lambda x: x.get("initiator_score", 0.0), reverse=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_campaigns": min_campaigns,
        "initiators": initiators,
    }

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Initiators artifact saved to %s (count=%d)", output_path, len(initiators))
    return output_path


def _avg(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _best_level(levels: List[str]) -> str:
    best = "unknown"
    best_score = -1
    for lvl in levels:
        score = LEVEL_PRIORITY.get(lvl, -1)
        if score > best_score:
            best = lvl
            best_score = score
    return best


def _compute_initiator_score(
    *,
    avg_pre_buy_usd: float,
    avg_sold_ratio: float,
    median_exit_seconds: float,
    campaigns_count: int,
    pamper_percentile: float,
    pamper_level: str,
) -> float:
    pre_buy_strength = _clamp(math.log1p(avg_pre_buy_usd) / math.log1p(5000), 0.0, 1.0)
    dump_aggression = _clamp(avg_sold_ratio / 1.0, 0.0, 1.0)
    if median_exit_seconds <= 600:
        exit_speed_factor = 1.0
    elif median_exit_seconds >= 7200:
        exit_speed_factor = 0.0
    else:
        exit_speed_factor = _clamp(1 - (median_exit_seconds - 600) / (7200 - 600), 0.0, 1.0)
    repeatability_factor = _clamp(campaigns_count / 5.0, 0.0, 1.0)
    percentile = _clamp(pamper_percentile, 0.0, 1.0)
    level_bonus = LEVEL_BONUS.get(pamper_level, 0.4)
    pamper_quality = _clamp(0.6 * percentile + 0.4 * level_bonus, 0.0, 1.0)

    score = (
        0.30 * pre_buy_strength
        + 0.25 * dump_aggression
        + 0.20 * exit_speed_factor
        + 0.15 * repeatability_factor
        + 0.10 * pamper_quality
    )
    return _clamp(score, 0.0, 1.0)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _load_cluster_map(path: Path) -> Dict[str, str]:
    data = _load_json(path) or []
    cluster_map: Dict[str, str] = {}
    if isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                addr = str(entry.get("address") or "").lower()
                if addr:
                    cluster_map[addr] = entry.get("address") or addr
    return cluster_map

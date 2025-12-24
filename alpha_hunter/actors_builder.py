import json
import math
import os
from typing import Any, Dict, List, Optional

from .logger import get_logger

SYSTEM_DIR = os.path.join("data", "alpha_profiler", "system")

def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_json(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_actors_watchlist(
    profiles_dir: str = "data/alpha_profiler",
    actors_path: str = os.path.join(SYSTEM_DIR, "actors.json"),
    watchlist_path: str = os.path.join(SYSTEM_DIR, "watchlist.json"),
    logger=None,
) -> Dict[str, str]:
    log = logger or get_logger("actors_builder")

    def best_level(levels: List[str]) -> str:
        priority = {"core_operator": 3, "inner_circle": 2, "outer_circle": 1, "retail": 0}
        return max(levels, key=lambda x: priority.get(x, -1)) if levels else "retail"

    actors: Dict[str, Dict[str, Any]] = {}

    if not os.path.isdir(profiles_dir):
        log.warning("Profiles directory %s not found", profiles_dir)
        return {"actors": actors_path, "watchlist": watchlist_path}

    for fname in os.listdir(profiles_dir):
        if not fname.endswith("_profile.json"):
            continue
        path = os.path.join(profiles_dir, fname)
        data = _load_json(path)
        if not isinstance(data, dict):
            continue
        meta = data.get("meta") or {}
        wallets = data.get("wallets") or []
        symbol = meta.get("symbol", "")
        chain = meta.get("chain", "")
        pump_start = meta.get("pump_start", "")
        profile_name = os.path.splitext(fname)[0]

        for w in wallets:
            addr = str(w.get("address") or "").lower()
            if not addr:
                continue
            pamper_score = float(w.get("pamper_score") or 0.0)
            pamper_level = str(w.get("pamper_level") or "retail")
            roi = w.get("roi")
            roi_val = float(roi) if roi is not None else None
            share = float(w.get("share_of_pump_volume") or 0.0)
            pnl = float(w.get("realized_pnl_usd") or 0.0)

            actor = actors.setdefault(
                addr,
                {
                    "address": addr,
                    "chain": chain,
                    "pumps": [],
                    "pamper_scores": [],
                    "pamper_levels": [],
                    "realized_pnl_list": [],
                    "roi_list": [],
                    "share_list": [],
                },
            )
            actor["chain"] = actor.get("chain") or chain
            actor["pumps"].append(
                {
                    "profile_name": profile_name,
                    "symbol": symbol,
                    "chain": chain,
                    "pump_start": pump_start,
                    "pamper_score": pamper_score,
                }
            )
            actor["pamper_scores"].append(pamper_score)
            actor["pamper_levels"].append(pamper_level)
            actor["realized_pnl_list"].append(pnl)
            if roi_val is not None:
                actor.setdefault("roi_list", []).append(roi_val)
            actor["share_list"].append(share)

    # финальный расчёт агрегатов
    actors_list: List[Dict[str, Any]] = []
    for addr, actor in actors.items():
        pumps = actor.get("pumps", [])
        scores = actor.get("pamper_scores", [])
        levels = actor.get("pamper_levels", [])
        pnl_list = actor.get("realized_pnl_list", [])
        roi_list = actor.get("roi_list", [])
        share_list = actor.get("share_list", [])

        pumps_count = len(pumps)
        max_score = max(scores) if scores else 0.0
        avg_score = sum(scores) / len(scores) if scores else 0.0
        avg_roi = sum(roi_list) / len(roi_list) if roi_list else 0.0
        total_pnl = sum(pnl_list)
        avg_share = sum(share_list) / len(share_list) if share_list else 0.0
        best_level_val = best_level(levels)
        level_priority = {"core_operator": 3, "inner_circle": 2, "outer_circle": 1, "retail": 0}
        pamper_level_weight = level_priority.get(best_level_val, 0) / 3.0
        roi_weight = max(avg_roi, 0)
        volume_share_weight = max(avg_share, 0)
        stability_weight = 1.0  # placeholder, можем заменить на дисперсию ROI
        actor_score_factors = {
            "roi_weight": roi_weight,
            "volume_share_weight": volume_share_weight,
            "pamper_level_weight": pamper_level_weight,
            "stability_weight": stability_weight,
        }
        actor_score = max_score * math.log(1 + pumps_count) * (1 + roi_weight + volume_share_weight + pamper_level_weight)

        actors_list.append(
            {
                "address": addr,
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
                "actor_score_factors": actor_score_factors,
            }
        )

    _save_json(actors_list, actors_path)
    log.info("Saved actors to %s", actors_path)

    watchlist = [
        {
            "address": a["address"],
            "chain": a.get("chain", ""),
            "actor_score": a.get("actor_score", 0.0),
            "pumps_count": a.get("pumps_count", 0),
            "best_pamper_level": a.get("best_pamper_level", ""),
            "avg_roi": a.get("avg_roi", 0.0),
            "max_pamper_score": a.get("max_pamper_score", 0.0),
        }
        for a in actors_list
        if a.get("pumps_count", 0) >= 2
        and a.get("best_pamper_level") in {"core_operator", "inner_circle"}
    ]
    watchlist.sort(key=lambda x: x.get("actor_score", 0.0), reverse=True)
    _save_json(watchlist, watchlist_path)
    log.info("Saved watchlist to %s", watchlist_path)

    return {"actors": actors_path, "watchlist": watchlist_path}

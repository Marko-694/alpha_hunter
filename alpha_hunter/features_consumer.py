import json
import logging
import os
from typing import Any, Dict, List


def load_features(path: str = "data/alpha_profiler/warehouse/features.json") -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or []
        if isinstance(data, list):
            return data
    except Exception:
        return []
    return []


def evaluate_signals(
    features: List[Dict[str, Any]],
    thresholds: Dict[str, Any],
) -> List[str]:
    signals: List[str] = []
    score_th = float(thresholds.get("actor_score", 10))
    pnl_th = float(thresholds.get("realized_pnl_usd", 0))
    overlap_th = float(thresholds.get("avg_share_of_pump_volume", 0.01))
    for f in features:
        score = float(f.get("actor_score") or 0)
        realized = float(f.get("cov_realized_pnl_usd") or 0)
        share = float(f.get("avg_share_of_pump_volume") or 0)
        if score >= score_th and realized >= pnl_th and share >= overlap_th:
            signals.append(
                f"[FEATURE] {f.get('address')} {f.get('chain')} score={score:.2f} pnl={realized:.0f} share={share:.3f}"
            )
    return signals

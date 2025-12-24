import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .logger import get_logger

SYSTEM_DIR = Path("data/alpha_profiler/system")


def _safe_load_json(path: str, logger: logging.Logger | None = None) -> Any:
    p = Path(path)
    if not p.exists():
        if logger:
            logger.warning("Profile artifact not found: %s", path)
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover
        if logger:
            logger.warning("Failed to load %s: %s", path, exc)
        return None


def _norm_addr(addr: str | None) -> str:
    if not addr:
        return ""
    s = str(addr).strip().lower()
    if ":" in s:
        s = s.split(":", 1)[1]
    return s


def _normalized_features_payload(data: Any, logger: logging.Logger | None = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Ensure we always return {"actors": [...]} even if source is a bare list.
    """
    if isinstance(data, dict):
        actors = data.get("actors")
        if isinstance(actors, list):
            return {"actors": actors}
        if isinstance(data, list):
            return {"actors": data}
        if logger:
            logger.warning("features payload missing 'actors' list, defaulting to empty")
        return {"actors": []}
    if isinstance(data, list):
        return {"actors": data}
    if logger:
        logger.warning("features payload is neither dict nor list")
    return {"actors": []}


def load_features(path: str = "data/alpha_profiler/warehouse/features.json", logger: logging.Logger | None = None) -> Dict[str, Any]:
    data = _safe_load_json(path, logger) or {}
    return _normalized_features_payload(data, logger)


def load_features_if_exists(path: str, logger: logging.Logger | None = None) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"actors": []}
    return load_features(path=path, logger=logger)


def _extract_trust_map(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    actors = _normalized_features_payload(payload, None).get("actors") or []
    for entry in actors:
        if not isinstance(entry, dict):
            continue
        addr = _norm_addr(entry.get("address"))
        if not addr:
            continue
        trust = entry.get("actor_trust")
        if trust is None:
            continue
        last_summary = entry.get("actor_last_pump_summary") or {}
        result[addr] = {
            "trust": float(trust),
            "last_summary": last_summary,
            "meta": entry.get("actor_trust_meta") or {},
        }
    return result


def build_actor_trust_map(
    primary: Optional[Dict[str, Any]],
    secondary: Optional[Dict[str, Any]],
    tertiary: Optional[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    trust_map: Dict[str, Dict[str, Any]] = {}
    for payload in (primary, secondary, tertiary):
        if not payload:
            continue
        trust_map.update(_extract_trust_map(payload))
    return trust_map


def get_actor_trust_map(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return snapshot.get("trust_map") or {}


def load_watchlist(path: str = str(SYSTEM_DIR / "watchlist.json"), logger: logging.Logger | None = None) -> List[str]:
    data = _safe_load_json(path, logger) or []
    addrs: List[str] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                addr = _norm_addr(item.get("address"))
            else:
                addr = _norm_addr(str(item))
            if addr:
                addrs.append(addr)
    return addrs


def load_watchlist_records(path: str = str(SYSTEM_DIR / "watchlist.json"), logger: logging.Logger | None = None) -> List[Dict[str, Any]]:
    data = _safe_load_json(path, logger) or []
    return data if isinstance(data, list) else []


def load_cache_health(path: str = "data/alpha_profiler/warehouse/cache_health.json", logger: logging.Logger | None = None) -> Dict[str, Any]:
    data = _safe_load_json(path, logger) or {}
    return data if isinstance(data, dict) else {}


def load_initiators(path: str = "data/alpha_profiler/warehouse/initiators.json", logger: logging.Logger | None = None) -> Dict[str, Any]:
    data = _safe_load_json(path, logger) or {}
    initiators_list: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        initiators_list = data.get("initiators") or []
    elif isinstance(data, list):
        initiators_list = data
    result = {
        "initiator_addrs": set(),
        "addr_score": {},
        "addr_campaigns": {},
        "addr_tokens": {},
        "addr_cluster_id": {},
        "addr_details": {},
    }
    for entry in initiators_list:
        if not isinstance(entry, dict):
            continue
        addr = _norm_addr(entry.get("address"))
        if not addr:
            continue
        result["initiator_addrs"].add(addr)
        result["addr_score"][addr] = float(entry.get("initiator_score") or 0.0)
        result["addr_campaigns"][addr] = int(entry.get("campaigns_count") or 0)
        result["addr_tokens"][addr] = entry.get("tokens") or []
        cluster_id = entry.get("cluster_id")
        result["addr_cluster_id"][addr] = cluster_id if cluster_id else None
        result["addr_details"][addr] = entry
    return result


def load_exit_signatures(
    path: str = "data/alpha_profiler/warehouse/exit_signatures.json",
    logger: logging.Logger | None = None,
) -> Dict[str, Dict[str, Any]]:
    data = _safe_load_json(path, logger) or {}
    actors = []
    if isinstance(data, dict):
        actors = data.get("actors") or []
    elif isinstance(data, list):
        actors = data
    index: Dict[str, Dict[str, Any]] = {}
    for entry in actors:
        if not isinstance(entry, dict):
            continue
        addr = _norm_addr(entry.get("address"))
        if not addr:
            continue
        chain = str(entry.get("chain") or "bsc").lower()
        key = f"{chain}:{addr}"
        index[key] = entry
        index[addr] = entry
    return index


def get_top_actors(
    features: Any,
    watchlist_addrs: Set[str],
    *,
    min_score: float,
    min_pnl: float,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Возвращает топ акторов по score/pnl, фильтруя по watchlist при наличии.
    Ожидаемый формат features: {"actors": [ {"address": "..", "actor_score": ..., "cov_realized_pnl_usd": ...}, ... ]}
    """
    payload = _normalized_features_payload(features, None)
    actors = payload.get("actors")
    if not isinstance(actors, list):
        return []
    rows: List[Dict[str, Any]] = []
    for a in actors:
        if not isinstance(a, dict):
            continue
        addr = _norm_addr(a.get("address"))
        if not addr:
            continue
        if watchlist_addrs and addr not in watchlist_addrs:
            continue
        score = float(a.get("actor_score") or 0.0)
        pnl = float(a.get("cov_realized_pnl_usd") or a.get("realized_pnl_usd") or 0.0)
        if score < min_score or pnl < min_pnl:
            continue
        rows.append(
            {
                "address": addr,
                "actor_score": score,
                "realized_pnl_usd": pnl,
            }
        )
    rows.sort(key=lambda x: (x.get("actor_score", 0), x.get("realized_pnl_usd", 0)), reverse=True)
    return rows[:limit]


def load_actors_raw(path: str = str(SYSTEM_DIR / "actors.json"), logger: logging.Logger | None = None) -> List[Dict[str, Any]]:
    data = _safe_load_json(path, logger) or []
    return data if isinstance(data, list) else []


def load_clusters_raw(path: str = str(SYSTEM_DIR / "clusters.json"), logger: logging.Logger | None = None) -> List[Dict[str, Any]]:
    data = _safe_load_json(path, logger) or []
    return data if isinstance(data, list) else []


def build_initiator_index(
    actors: List[Dict[str, Any]],
    clusters: List[Dict[str, Any]],
    features: Any,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build lightweight index of likely initiators (actors/clusters) for realtime checks.
    """
    norm_features = _normalized_features_payload(features, None)
    feature_scores: Dict[str, Dict[str, Any]] = {}
    for entry in norm_features.get("actors", []):
        if isinstance(entry, dict):
            addr = _norm_addr(entry.get("address"))
            if addr:
                feature_scores[addr] = entry

    cluster_map: Dict[str, Dict[str, Any]] = {}
    cluster_size: Dict[str, int] = {}
    for entry in clusters or []:
        if isinstance(entry, dict):
            addr = _norm_addr(entry.get("address"))
            if not addr:
                continue
            cluster_map[addr] = entry
            size = entry.get("pumps_participated")
            if isinstance(size, int):
                cluster_size[addr] = size
            else:
                cluster_size[addr] = len(entry.get("tokens") or []) or len(entry.get("chains") or []) or 0

    settings = settings or {}
    min_pumps = int(settings.get("min_pumps_count", 2))
    min_actor_score = float(settings.get("min_actor_score", 15.0))
    allowed_levels = {lvl.lower() for lvl in settings.get("allowed_levels", ["core_operator", "inner_circle", "outer_circle"])}

    initiator_addrs: Set[str] = set()
    addr_score: Dict[str, float] = {}
    addr_pumps: Dict[str, int] = {}
    addr_cluster: Dict[str, Optional[str]] = {}
    addr_tokens: Dict[str, List[str]] = {}

    for actor in actors or []:
        if not isinstance(actor, dict):
            continue
        addr = _norm_addr(actor.get("address"))
        if not addr:
            continue
        pumps_count = int(actor.get("pumps_count") or 0)
        best_level = str(actor.get("best_pamper_level") or "").lower()
        score = float(actor.get("actor_score") or 0.0)
        if addr in feature_scores:
            score = float(feature_scores[addr].get("actor_score") or score)
        qualifies = pumps_count >= min_pumps and (
            best_level in allowed_levels or score >= min_actor_score
        )
        if not qualifies:
            continue
        initiator_addrs.add(addr)
        addr_score[addr] = score
        addr_pumps[addr] = pumps_count
        tokens = set()
        for pump in actor.get("pumps") or []:
            symbol = str((pump or {}).get("symbol") or "")
            if symbol:
                tokens.add(symbol.upper())
        addr_tokens[addr] = sorted(tokens)
        cluster_entry = cluster_map.get(addr)
        addr_cluster[addr] = cluster_entry.get("address") if cluster_entry else None

    return {
        "initiator_addrs": initiator_addrs,
        "addr_score": addr_score,
        "addr_pumps": addr_pumps,
        "addr_cluster": addr_cluster,
        "cluster_size": cluster_size,
        "addr_campaigns": addr_pumps,
        "addr_tokens": addr_tokens,
        "addr_cluster_id": addr_cluster,
        "addr_details": {},
    }


def get_mtime(path: str) -> Optional[float]:
    p = Path(path)
    try:
        return p.stat().st_mtime
    except FileNotFoundError:
        return None


class ArtifactsCache:
    def __init__(self, logger=None) -> None:
        self.logger = logger or get_logger("artifacts_cache")
        self.paths = {
            "features": "data/alpha_profiler/warehouse/features.json",
            "features_actors": "data/alpha_profiler/warehouse/features_actors.json",
            "features_watchlist": "data/alpha_profiler/warehouse/features_watchlist.json",
            "watchlist": str(SYSTEM_DIR / "watchlist.json"),
            "cache_health": "data/alpha_profiler/warehouse/cache_health.json",
            "actors": str(SYSTEM_DIR / "actors.json"),
            "clusters": str(SYSTEM_DIR / "clusters.json"),
            "initiators": "data/alpha_profiler/warehouse/initiators.json",
            "exit_signatures": "data/alpha_profiler/warehouse/exit_signatures.json",
        }
        self.mtimes: Dict[str, Optional[float]] = {}
        self.snapshot: Dict[str, Any] = {
            "features": {"actors": []},
            "features_watchlist": {"actors": []},
            "features_actors": {"actors": []},
            "watchlist_records": [],
            "watchlist_addrs": set(),
            "cache_health": {},
            "actors_raw": [],
            "clusters_raw": [],
            "initiators": {},
            "exit_signatures": {},
            "trust_map": {},
        }
        self.refresh_if_changed(force=True)

    def refresh_if_changed(self, force: bool = False) -> bool:
        changed = False
        for key, path in self.paths.items():
            mtime = get_mtime(path)
            if force or self.mtimes.get(key) != mtime:
                self._reload_key(key, path)
                self.mtimes[key] = mtime
                changed = True
        return changed

    def _reload_key(self, key: str, path: str) -> None:
        if key == "features":
            self.snapshot["features"] = load_features(path=path, logger=self.logger)
            self._rebuild_trust_map()
        elif key == "features_actors":
            self.snapshot["features_actors"] = load_features_if_exists(path, logger=self.logger)
            self._rebuild_trust_map()
        elif key == "features_watchlist":
            self.snapshot["features_watchlist"] = load_features_if_exists(path, logger=self.logger)
            self._rebuild_trust_map()
        elif key == "watchlist":
            records = load_watchlist_records(path=path, logger=self.logger)
            self.snapshot["watchlist_records"] = records
            addrs = set()
            for item in records:
                addr = _norm_addr((item or {}).get("address"))
                if addr:
                    addrs.add(addr)
            self.snapshot["watchlist_addrs"] = addrs
        elif key == "cache_health":
            self.snapshot["cache_health"] = load_cache_health(path=path, logger=self.logger)
        elif key == "actors":
            self.snapshot["actors_raw"] = load_actors_raw(path=path, logger=self.logger)
        elif key == "clusters":
            self.snapshot["clusters_raw"] = load_clusters_raw(path=path, logger=self.logger)
        elif key == "initiators":
            self.snapshot["initiators"] = load_initiators(path=path, logger=self.logger)
        elif key == "exit_signatures":
            self.snapshot["exit_signatures"] = load_exit_signatures(path=path, logger=self.logger)

    def _rebuild_trust_map(self) -> None:
        self.snapshot["trust_map"] = build_actor_trust_map(
            self.snapshot.get("features_actors"),
            self.snapshot.get("features"),
            self.snapshot.get("features_watchlist"),
        )

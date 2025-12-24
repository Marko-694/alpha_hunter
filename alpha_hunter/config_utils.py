import logging
from typing import Any, Dict, Tuple


def resolve_nansen_config(
    config: Dict[str, Any] | None,
    logger: logging.Logger | None = None,
) -> Tuple[Dict[str, Any], str]:
    """
    Support both legacy config layouts:
    nansen.enabled/api_key...
    nansen.nansen.enabled/api_key...
    Returns (config_dict, variant_name).
    """
    base = config or {}
    raw = base.get("nansen") or {}
    variant = "flat"
    resolved: Dict[str, Any] = {}
    if isinstance(raw, dict):
        if any(k in raw for k in ("enabled", "api_key", "base_url")):
            resolved = raw
        nested = raw.get("nansen")
        if isinstance(nested, dict):
            resolved = nested
            variant = "nested"
    elif isinstance(raw, list):
        resolved = {}
    else:
        resolved = {}
    resolved = dict(resolved or {})
    if logger:
        logger.info(
            "Nansen config resolved (%s variant, enabled=%s)",
            variant,
            resolved.get("enabled"),
        )
    return resolved, variant

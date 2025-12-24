import json
import time
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional

from .logger import get_logger


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _iter_profile_files(directory: Path) -> List[Path]:
    return sorted(directory.glob("*_profile.json"))


def build_exit_signatures(
    profiles_dir: str = "data/alpha_profiler",
    out_path: str = "data/alpha_profiler/warehouse/exit_signatures.json",
    logger=None,
) -> str:
    logger = logger or get_logger("exit_signatures_builder")
    base = Path(profiles_dir)
    if not base.exists():
        raise FileNotFoundError(f"Profiles directory not found: {profiles_dir}")

    actors: Dict[str, Dict[str, Any]] = {}
    for profile_path in _iter_profile_files(base):
        try:
            data = json.loads(profile_path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to read %s: %s", profile_path, exc)
            continue
        chain = str(data.get("chain") or "bsc").lower()
        wallets = data.get("wallet_sample") or data.get("wallets") or []
        for wallet in wallets:
            if not isinstance(wallet, dict):
                continue
            addr = str(wallet.get("address") or "").strip().lower()
            if not addr:
                continue
            bucket = actors.setdefault(
                addr,
                {
                    "address": wallet.get("address"),
                    "chain": chain,
                    "exit_speed_values": [],
                    "dump_ratio_values": [],
                    "campaigns": 0,
                },
            )
            exit_speed = wallet.get("exit_speed_seconds")
            if isinstance(exit_speed, (int, float)) and exit_speed > 0:
                bucket["exit_speed_values"].append(float(exit_speed))
            dump_ratio = wallet.get("sold_after_peak_ratio")
            if isinstance(dump_ratio, (int, float)) and dump_ratio > 0:
                bucket["dump_ratio_values"].append(float(dump_ratio))
            bucket["campaigns"] += 1

    records: List[Dict[str, Any]] = []
    for payload in actors.values():
        record: Dict[str, Any] = {
            "address": payload["address"],
            "chain": payload["chain"],
            "campaigns": payload["campaigns"],
        }
        if payload["exit_speed_values"]:
            record["exit_speed_p50_seconds"] = int(median(payload["exit_speed_values"]))
        if payload["dump_ratio_values"]:
            record["dump_ratio_p50"] = float(median(payload["dump_ratio_values"]))
        records.append(record)

    records.sort(key=lambda item: (item.get("campaigns", 0), item.get("address", "")), reverse=True)
    output = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "actors": records,
    }
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(out_file, json.dumps(output, ensure_ascii=False, indent=2))
    logger.info("Exit signatures saved to %s (actors=%d)", out_file, len(records))
    return str(out_file)

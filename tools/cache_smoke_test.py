"""
Smoke-test Covalent bucket cache: выполняет 2 прогона подряд по одному и тому же окну/адресу,
затем выводит cache_meta.report before/after. Автопоиск первого *_profile.json в data/alpha_profiler/.

Запуск (PowerShell):
  $env:PYTHONPATH="C:\\Users\\1245\\v0 Alpha Hunter"; `
  $env:LOG_LEVEL="DEBUG"; `
  python tools\\cache_smoke_test.py

CLI:
  --profile PATH            путь до *_profile.json
  --addr/--chain/--start/--end  ручной режим окна
  --limit-profiles N        ограничить автопоиск профилей (default=10)
  --runs N                  число прогонов подряд (default=2)
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import yaml


def _prep_sys_path() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root))


_prep_sys_path()

from alpha_hunter.logger import get_logger  # noqa: E402
from alpha_hunter.pnl_tracer import parse_dt_unsafe, recompute_watchlist_pnl  # noqa: E402
from alpha_hunter.actors_builder import _load_json  # noqa: E402


def load_cache_report(p: Path) -> dict:
    data = _load_json(str(p)) or {}
    cm = data.get("cache_meta") or data.get("meta", {}).get("cache_meta") or {}
    report = cm.get("report") or {}
    cov = cm.get("covalent", {})
    if cov:
        report.setdefault("covalent", {})
        for k in ("hits", "misses", "api_calls", "buckets_reused", "buckets_fetched"):
            if k in cov.get("report", {}):
                report["covalent"][k] = cov["report"][k]
    return report


def _parse_profile_file(path: Path) -> Optional[Tuple[str, str, int, int, str]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    meta = data.get("meta") or {}
    chain = meta.get("chain") or None
    if not chain:
        parts = path.name.split("_")
        for part in parts:
            if part in {"bsc", "eth", "arb", "polygon", "base", "optimism"}:
                chain = part
                break
    symbol = meta.get("symbol") or path.name.split("_")[0]
    start_ts = int(meta.get("start_ts") or meta.get("pump_start_ts") or 0)
    end_ts = int(meta.get("end_ts") or meta.get("pump_end_ts") or 0)
    if start_ts <= 0 and meta.get("pump_start"):
        start_ts = int(parse_dt_unsafe(meta.get("pump_start")).timestamp())
    if end_ts <= 0 and meta.get("pump_end"):
        end_ts = int(parse_dt_unsafe(meta.get("pump_end")).timestamp())
    if start_ts <= 0 or end_ts <= 0:
        ts = int(meta.get("pump_ts") or 0)
        if ts > 0:
            start_ts = ts - 7200
            end_ts = ts + 7200
    # ограничения
    if start_ts <= 0 or end_ts <= start_ts:
        return None
    if (end_ts - start_ts) > 7 * 86400:
        end_ts = start_ts + 7 * 86400
    addr = None
    for w in data.get("wallets") or []:
        addr = w.get("address")
        if addr:
            break
    if not all([chain, addr, start_ts, end_ts]):
        return None
    return chain, addr, start_ts, end_ts, symbol


def pick_profile(base_dir: Path, limit: int = 10) -> Optional[Tuple[str, str, int, int, str]]:
    count = 0
    for p in base_dir.glob("*_profile.json"):
        count += 1
        if limit and count > limit:
            break
        res = _parse_profile_file(p)
        if res:
            return res
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", help="path to *_profile.json")
    ap.add_argument("--addr", help="wallet address")
    ap.add_argument("--chain", help="chain (bsc,eth,...)")
    ap.add_argument("--start", type=int, help="window start ts")
    ap.add_argument("--end", type=int, help="window end ts")
    ap.add_argument("--limit-profiles", type=int, default=10, help="limit number of profiles to scan")
    ap.add_argument("--runs", type=int, default=2, help="number of consecutive runs")
    args = ap.parse_args()

    logger = get_logger("cache_smoke_test")
    if os.getenv("LOG_LEVEL", "").upper() == "DEBUG":
        logger.setLevel(logging.DEBUG)

    base_dir = Path("data/alpha_profiler")
    if not base_dir.exists():
        print("NO_PROFILE_FOUND")
        sys.exit(1)

    profile_info = None
    if args.profile:
        profile_info = _parse_profile_file(Path(args.profile))
    elif args.addr and args.chain and args.start and args.end:
        profile_info = (args.chain, args.addr, args.start, args.end, "CLI")
    else:
        profile_info = pick_profile(base_dir, limit=args.limit_profiles)

    if not profile_info:
        print("NO_PROFILE_FOUND")
        sys.exit(1)

    chain, addr, start_ts, end_ts, symbol = profile_info
    logger.info(
        "Smoke-test profile: chain=%s addr=%s window=%s..%s symbol=%s",
        chain,
        addr,
        start_ts,
        end_ts,
        symbol,
    )
    start_bucket = start_ts // 900
    end_bucket = end_ts // 900
    logger.info("Buckets range: %s..%s (count=%d)", start_bucket, end_bucket, end_bucket - start_bucket + 1)

    # ensure covalent key present
    cfg_path = Path("config.yaml")
    if not cfg_path.exists():
        logger.error("config.yaml not found")
        sys.exit(1)
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    cov_key = (cfg.get("covalent") or {}).get("api_key")
    if not cov_key:
        logger.error("COVALENT_KEY_PRESENT: no")
        sys.exit(1)
    logger.info("COVALENT_KEY_PRESENT: yes")

    # force env for pnl_tracer
    os.environ["PWL_FORCE_CHAIN"] = chain
    os.environ["PWL_FORCE_ADDR"] = addr
    os.environ["PWL_FORCE_WINDOW"] = f"{start_ts}:{end_ts}"
    os.environ["PWL_LIMIT"] = "1"
    os.environ["PWL_DIAGNOSTIC"] = "1"

    # replace watchlist temporarily
    wl_path = base_dir / "watchlist.json"
    wl_backup = wl_path.read_text(encoding="utf-8") if wl_path.exists() else None
    wl_path.write_text(json.dumps([{"address": addr, "chain": chain}], ensure_ascii=False, indent=2), encoding="utf-8")

    reports = []
    for _ in range(max(1, args.runs)):
        recompute_watchlist_pnl(config=cfg, logger=logger)
        rep = load_cache_report(Path("data/alpha_profiler/pnl_watchlist.json"))
        reports.append(rep)

    if wl_backup is not None:
        wl_path.write_text(wl_backup, encoding="utf-8")

    print("=== Cache delta ===")
    print(json.dumps({"runs": reports}, indent=2))


if __name__ == "__main__":
    main()

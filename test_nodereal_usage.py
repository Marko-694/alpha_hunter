import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import yaml

from alpha_hunter.bsc_rpc_client import BscRpcClient
from alpha_hunter.logger import get_logger

TEST_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"  # USDT on BSC


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main() -> None:
    print("=== NodeReal BSC RPC usage tester ===")
    cfg = load_config()
    bsc_cfg = cfg.get("bsc_rpc") or {}
    url = str(bsc_cfg.get("url", "")).strip()
    if not url:
        print("bsc_rpc.url is empty in config.yaml")
        return

    logger = get_logger("nodereal_test")
    client = BscRpcClient(url=url, logger=logger)

    lookback_hours = 4.5
    now_utc = datetime.now(timezone.utc)
    start_ts = int((now_utc - timedelta(hours=lookback_hours)).timestamp())
    end_ts = int(now_utc.timestamp())

    print(f"Using RPC URL: {url}")
    print(
        "Time window: "
        f"{datetime.fromtimestamp(start_ts, tz=timezone.utc)} .. "
        f"{datetime.fromtimestamp(end_ts, tz=timezone.utc)} "
        f"(≈ {lookback_hours}h)"
    )
    print(f"Test contract: {TEST_CONTRACT}")

    start_block = client.find_block_by_timestamp(start_ts)
    end_block = client.find_block_by_timestamp(end_ts)
    if start_block is None or end_block is None:
        print("Failed to resolve block range for the requested window")
        return

    start_block = max(1, start_block - 100)  # небольшая страховка
    end_block = max(start_block, end_block + 100)

    t0 = time.time()
    client.stats_last_run_requests = 0
    try:
        logs = list(
            client.get_erc20_transfers(
                contract=TEST_CONTRACT,
                from_block=start_block,
                to_block=end_block,
            )
        )
    except Exception as exc:
        print(f"RPC error while fetching logs: {exc}")
        return
    dt = time.time() - t0

    reqs = client.stats_last_run_requests
    print("\n=== Result ===")
    print(f"Block range: {start_block}..{end_block}")
    print(f"Fetched logs: {len(logs)}")
    print(f"RPC requests in this run: {reqs}")
    print(f"Elapsed time: {dt:.2f} s")

    if reqs > 0:
        print(f"Approx CUs used (assuming 1 CU per RPC): ~{reqs}")
        per_hour = reqs / lookback_hours
        print(f"Approx RPC per hour for this window: {per_hour:.1f}")
        print("Compare this with NodeReal dashboard (Remaining CUs)")


if __name__ == "__main__":
    main()

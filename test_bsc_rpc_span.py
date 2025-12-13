import sys
import time
from typing import Any, Dict, Tuple

import requests
import yaml

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _rpc_post(url: str, payload: dict, timeout: int = 10) -> Tuple[dict | None, dict | None]:
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
    except Exception as exc:
        return None, {"code": "network", "message": str(exc)}

    if resp.status_code != 200:
        return None, {"code": "http", "message": f"HTTP {resp.status_code}: {resp.text[:200]}"}

    data = resp.json()
    err = data.get("error")
    if err:
        return None, err
    return data, None


def get_latest_block(url: str) -> int:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
    data, err = _rpc_post(url, payload)
    if err:
        raise RuntimeError(f"blockNumber error on {url}: {err}")
    return int(data["result"], 16)


def test_span(url: str, contract: str, max_try: int = 50000) -> int:
    """
    Бинарный поиск максимального допустимого диапазона блоков для eth_getLogs.
    Возвращает максимальный span (toBlock - fromBlock), который проходит без error -32005.
    """
    latest = get_latest_block(url)
    print(f"Latest block on {url}: {latest}")

    low = 10
    high = max_try
    best_ok = 0

    while low <= high:
        mid = (low + high) // 2
        from_block = max(1, latest - mid)
        to_block = latest

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "address": contract,
                    "topics": [],
                }
            ],
        }

        t0 = time.time()
        _, err = _rpc_post(url, payload)
        dt = time.time() - t0

        if err and err.get("code") == -32005:
            print(f"  span={mid:6d} -> limit exceeded ({dt:.2f}s)")
            high = mid - 1
        elif err:
            print(f"  span={mid:6d} -> OTHER ERROR {err} ({dt:.2f}s), считаем как fail")
            high = mid - 1
        else:
            print(f"  span={mid:6d} -> OK ({dt:.2f}s)")
            best_ok = mid
            low = mid + 1

    return best_ok


def main() -> None:
    cfg = load_config()
    bsc_cfg = cfg.get("bsc_rpc", {}) or {}
    urls = bsc_cfg.get("urls") or []
    contract = bsc_cfg.get("test_contract") or "0x55d398326f99059fF775485246999027B3197955"

    if not urls:
        print(f"{RED}No bsc_rpc.urls in config.yaml{RESET}")
        sys.exit(1)

    print("=== BSC RPC span tester ===")
    print(f"Test contract: {contract}")
    print()

    for url in urls:
        print(f"Testing {url} ...")
        try:
            span = test_span(url, contract)
            if span > 0:
                print(f"{GREEN}  Max safe span on {url}: {span} blocks{RESET}\n")
            else:
                print(f"{RED}  Could not find safe span on {url}{RESET}\n")
        except Exception as exc:
            print(f"{RED}  ERROR on {url}: {exc}{RESET}\n")


if __name__ == "__main__":
    main()

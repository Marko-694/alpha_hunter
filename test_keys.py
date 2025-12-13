import sys
from typing import Any, Dict, Tuple

import requests
import yaml

from alpha_hunter.explorer_client import BASE_API_URL, CHAIN_IDS

TEST_ADDRESS = "0x000000000000000000000000000000000000dead"

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

EXPLORER_NAMES = {
    "eth": "Ethereum",
    "bsc": "BNB",
    "arb": "Arbitrum",
    "polygon": "Polygon",
    "optimism": "Optimism",
    "base": "Base",
}


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        print(f"{RED}config.yaml not found{RESET}")
        return {}
    except Exception as exc:
        print(f"{RED}Failed to load config.yaml: {exc}{RESET}")
        return {}


def _print_result(ok: bool, service: str, message: str) -> None:
    status = f"{GREEN}OK{RESET}" if ok else f"{RED}FAIL{RESET}"
    print(f"[{status}] {service} — {message}")


def _check_explorer(api_key: str, chainid: int, service: str) -> Tuple[bool, str]:
    if not api_key:
        return False, "ключ отсутствует в config.yaml"
    params = {
        "chainid": chainid,
        "module": "account",
        "action": "balance",
        "address": TEST_ADDRESS,
        "tag": "latest",
        "apikey": api_key,
    }
    try:
        resp = requests.get(BASE_API_URL, params=params, timeout=10)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        data = resp.json()
        status = str(data.get("status"))
        message = data.get("message", "")
        result = data.get("result")
        if status != "1":
            return False, f"status={status}, message={message}, result={result}"
        return True, "ключ рабочий"
    except Exception as exc:
        return False, str(exc)


def check_nansen(api_key: str, base_url: str) -> Tuple[bool, str]:
    if not api_key:
        return False, "ключ отсутствует в config.yaml"

    url = base_url.rstrip("/") + "/api/v1/profiler/address/current-balance"
    payload = {
        "address": TEST_ADDRESS,
        "chain": "ethereum",
        "hide_spam_token": True,
        "pagination": {"page": 1, "per_page": 1},
    }
    headers = {
        "apiKey": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        if resp.status_code == 401:
            return False, "Unauthorized (проверь apiKey)"
        if resp.status_code == 403:
            return False, "Forbidden (нет доступа к эндпоинту на текущем плане)"
        if resp.status_code in (402, 429):
            return True, f"ключ рабочий, но нет юнитов или превышен лимит (HTTP {resp.status_code})"
        if resp.status_code >= 400:
            text = resp.text.strip().replace("\n", " ")
            return False, f"HTTP {resp.status_code}: {text[:160]}"
        return True, "ключ рабочий (получен 200)"
    except Exception as exc:
        return False, str(exc)


def main() -> None:
    print("=== API KEY TESTER ===")
    cfg = load_config()
    explorers = cfg.get("explorers", {}) or {}
    nansen_cfg = cfg.get("nansen", {}) or {}

    etherscan_key = explorers.get("etherscan_api_key", "")

    checks = []
    for chain, chainid in CHAIN_IDS.items():
        name = EXPLORER_NAMES.get(chain, chain)
        checks.append((name, lambda k, c=chainid, n=name: _check_explorer(k, c, n), etherscan_key))

    nansen_api_key = nansen_cfg.get("api_key", "")
    nansen_base_url = nansen_cfg.get("base_url", "https://api.nansen.ai")
    checks.append(("Nansen", lambda k: check_nansen(k, nansen_base_url), nansen_api_key))

    for service, func, key in checks:
        try:
            ok, msg = func(key)
        except Exception as exc:
            ok, msg = False, str(exc)
        _print_result(ok, service, msg)


if __name__ == "__main__":
    sys.exit(main() or 0)

# Примечание: V1 эндпоинты Etherscan-подобных API deprecated и отвечают NOTOK.
# Скрипт переведён на V2 (единый https://api.etherscan.io/v2/api + chainid).
# При status != 1 печатается полный ответ status/message/result для отладки.
# Для тяжёлой BSC-истории оффлайн-профайлер использует RPC, а Etherscan-ключ нужен для лёгких проверок.

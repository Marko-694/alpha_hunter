import requests
from typing import Any, Dict, List, Optional

# Etherscan V2 base endpoint (единый для всех поддерживаемых сетей)
BASE_API_URL = "https://api.etherscan.io/v2/api"

# Соответствие chain -> chainid
CHAIN_IDS: Dict[str, int] = {
    "eth": 1,
    "bsc": 56,
    "arb": 42161,
    "polygon": 137,
    "optimism": 10,
    "base": 8453,
}

# Для формирования ссылок на tx в UI-сканерах
CHAIN_WEB_URLS: Dict[str, str] = {
    "eth": "https://etherscan.io",
    "bsc": "https://bscscan.com",
    "arb": "https://arbiscan.io",
    "polygon": "https://polygonscan.com",
    "optimism": "https://optimistic.etherscan.io",
    "base": "https://basescan.org",
}


class ExplorerClient:
    """
    Клиент для Etherscan V2 (использует единый API-ключ etherscan_api_key и chainid).
    """

    def __init__(self, config: Dict[str, Any], logger) -> None:
        # сохраняем полный конфиг, чтобы другие модули могли его читать
        self.config = config
        self.logger = logger
        explorers_cfg = config.get("explorers", {}) or {}
        # Используем только etherscan_api_key для всех цепей (остальные считаем deprecated)
        self.etherscan_key: str = explorers_cfg.get("etherscan_api_key", "")

    def has_api_for_chain(self, chain: str) -> bool:
        return chain in CHAIN_IDS and bool(self.etherscan_key)

    def get_tx_url(self, chain: str, tx_hash: str) -> Optional[str]:
        base = CHAIN_WEB_URLS.get(chain)
        if not base:
            return None
        return f"{base}/tx/{tx_hash}"

    def _request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            resp = requests.get(BASE_API_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                self.logger.error("Unexpected explorer response: %r", data)
                return None
            return data
        except Exception as exc:
            self.logger.error("Explorer request failed: %s", exc)
            return None

    def get_recent_token_transfers(
        self,
        chain: str,
        address: str,
        start_block: Optional[int] = None,
        sort: str = "asc",
    ) -> List[Dict[str, Any]]:
        if chain not in CHAIN_IDS or not self.etherscan_key:
            return []

        params: Dict[str, Any] = {
            "chainid": CHAIN_IDS[chain],
            "module": "account",
            "action": "tokentx",
            "address": address,
            "sort": sort,
            "apikey": self.etherscan_key,
        }
        if start_block is not None:
            params["startblock"] = int(start_block)

        data = self._request(params)
        if not data:
            return []

        status = str(data.get("status"))
        message = data.get("message", "")
        result = data.get("result")
        if status != "1":
            self.logger.error(
                "Explorer V2 non-OK (%s): status=%s, message=%s, result=%s",
                chain,
                status,
                message,
                result,
            )
            return []

        if not isinstance(result, list):
            self.logger.error("Explorer result is not a list (%s): %s", chain, result)
            return []

        return result

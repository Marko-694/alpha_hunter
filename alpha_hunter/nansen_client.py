import json
import logging
from typing import Any, Dict, Optional

import requests

from .logger import get_logger


class NansenClient:
    """
    Минимальный клиент для Nansen API.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.nansen.ai",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.logger = logger or get_logger("nansen_client")

    def _post(self, path: str, json_body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        headers = {
            "x-api-key": self.api_key,
            "accept": "application/json",
        }
        try:
            resp = requests.post(url, headers=headers, json=json_body, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                self.logger.error("Nansen %s returned non-dict payload: %r", path, data)
                return None
            return data
        except Exception as exc:
            self.logger.error("Nansen %s error: %s", path, exc)
            return None

    def token_screener(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self._post("/api/v1/token-screener", payload)

    def smart_money_netflows(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self._post("/api/v1/smart-money/netflows", payload)

    def holders(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self._post("/api/v1/token-god-mode/holders", payload)

    def smart_money_dex_trades(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self._post("/api/v1/smart-money/dex-trades", payload)

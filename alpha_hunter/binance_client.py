import logging
from typing import List, Optional, Dict, Any

import requests

from .logger import get_logger


class BinanceClient:
    BASE_URL = "https://api.binance.com"
    TIMEOUT = 10

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or get_logger(self.__class__.__name__)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        url = f"{self.BASE_URL}{path}"
        try:
            response = requests.get(url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            self.logger.error("Binance GET %s failed: %s", url, exc)
            return None

    def get_24h_tickers(self) -> List[Dict[str, Any]]:
        """
        Fetch 24h ticker stats for all symbols.
        """
        data = self._get("/api/v3/ticker/24hr")
        if not isinstance(data, list):
            return []
        return data

    def get_symbol_price(self, symbol: str) -> Optional[float]:
        """
        Fetch latest price for a symbol.
        """
        data = self._get("/api/v3/ticker/price", params={"symbol": symbol})
        if not data or "price" not in data:
            return None
        try:
            return float(data["price"])
        except (TypeError, ValueError):
            self.logger.error("Unexpected price format for %s: %s", symbol, data)
            return None

    def get_ticker_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Return raw ticker price payload for a symbol, e.g. {"symbol": "BTCUSDT", "price": "42000.00"}.
        """
        url = f"{self.BASE_URL}/api/v3/ticker/price"
        try:
            resp = requests.get(url, params={"symbol": symbol}, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                self.logger.error("Unexpected ticker price response for %s: %r", symbol, data)
                return None
            return data
        except Exception as exc:
            self.logger.error("Error fetching ticker price for %s: %s", symbol, exc)
            return None

    def get_recent_klines(
        self, symbol: str, interval: str = "1m", limit: int = 11
    ) -> List[List[Any]]:
        """
        Fetch recent klines for a symbol.

        Uses /api/v3/klines and defaults to 11 candles:
        - 10 previous for average volume
        - 1 latest for current volume/price.
        """
        url = f"{self.BASE_URL}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        try:
            resp = requests.get(url, params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                self.logger.error("Unexpected kline response for %s: %s", symbol, data)
                return []
            return data
        except Exception as exc:  # broad to catch JSON/HTTP issues
            self.logger.error("Error fetching klines for %s: %s", symbol, exc)
            return []

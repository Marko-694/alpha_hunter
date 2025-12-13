import requests
from typing import Any, Dict, Optional


class BinanceFuturesClient:
    """
    Публичный клиент Binance Futures для open interest и funding rate.
    """

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, logger) -> None:
        self.logger = logger

    def get_premium_index(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        /fapi/v1/premiumIndex — возвращает markPrice, lastFundingRate и т.д.
        """
        url = f"{self.BASE_URL}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}
        try:
            resp = requests.get(url, params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                self.logger.error("Unexpected premiumIndex response for %s: %s", symbol, data)
                return None
            return data
        except Exception as exc:
            self.logger.error("Error fetching premiumIndex for %s: %s", symbol, exc)
            return None

    def get_open_interest(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        /fapi/v1/openInterest — текущий open interest в контрактах.
        """
        url = f"{self.BASE_URL}/fapi/v1/openInterest"
        params = {"symbol": symbol}
        try:
            resp = requests.get(url, params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                self.logger.error("Unexpected openInterest response for %s: %s", symbol, data)
                return None
            return data
        except Exception as exc:
            self.logger.error("Error fetching openInterest for %s: %s", symbol, exc)
            return None

    def get_futures_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Возвращает словарь с open interest, funding rate и mark price для символа.
        """
        oi_data = self.get_open_interest(symbol)
        prem_data = self.get_premium_index(symbol)
        if not oi_data or not prem_data:
            return None

        try:
            open_interest = float(oi_data.get("openInterest", 0.0))
        except (TypeError, ValueError):
            open_interest = 0.0

        try:
            funding_rate = float(prem_data.get("lastFundingRate", 0.0))
        except (TypeError, ValueError):
            funding_rate = 0.0

        try:
            mark_price = float(prem_data.get("markPrice", 0.0))
        except (TypeError, ValueError):
            mark_price = 0.0

        return {
            "open_interest": open_interest,
            "funding_rate": funding_rate,
            "mark_price": mark_price,
        }

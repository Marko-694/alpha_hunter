import logging
from typing import Any, Dict, Optional

import requests

from .logger import get_logger
from .binance_client import BinanceClient


class PriceClient:
    """
    Комбинированный клиент для получения цены токена в USD:
    1) Binance spot (symbolUSDT)
    2) DexScreener (по контракту)
    """

    def __init__(
        self,
        binance_client: BinanceClient,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or get_logger("price_client")
        self.binance_client = binance_client

    def _get_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            resp = requests.get(url, headers=headers or {}, params=params or {}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                return data
            self.logger.error("Unexpected JSON from %s: %r", url, data)
            return None
        except Exception as exc:
            self.logger.error("HTTP GET failed for %s: %s", url, exc)
            return None

    def get_price_usd(self, symbol: str, chain: Optional[str] = None, contract: Optional[str] = None) -> Optional[float]:
        """
        Возвращает цену токена в USD или None.
        Приоритет:
        1) Binance spot (SYMBOLUSDT)
        2) DexScreener по контракту
        """
        symbol = (symbol or "").upper().strip()
        contract = (contract or "").strip()

        price = self._get_binance_spot_price(symbol)
        if price is not None:
            return price

        if contract:
            price = self._get_dexscreener_price(contract)
            if price is not None:
                return price

        self.logger.warning("Unable to get USD price for symbol=%s contract=%s", symbol, contract)
        return None

    def _get_binance_spot_price(self, symbol: str) -> Optional[float]:
        try:
            ticker = f"{symbol}USDT"
            data = self.binance_client.get_ticker_price(ticker)
            if not data:
                return None
            price_str = data.get("price")
            if price_str is None:
                return None
            price = float(price_str)
            if price <= 0:
                return None
            self.logger.debug("Binance spot price for %s: %f", ticker, price)
            return price
        except Exception as exc:
            self.logger.error("Binance spot price failed for %s: %s", symbol, exc)
            return None

    def _get_dexscreener_price(self, contract: str) -> Optional[float]:
        url = f"https://api.dexscreener.io/latest/dex/tokens/{contract}"
        try:
            data = self._get_json(url)
            if not data:
                return None
            pairs = data.get("pairs") or []
            if not pairs:
                self.logger.warning("DexScreener: no pairs for contract %s", contract)
                return None
            first = pairs[0]
            price_usd_str = first.get("priceUsd")
            if not price_usd_str:
                self.logger.warning("DexScreener: priceUsd missing for contract %s", contract)
                return None
            price = float(price_usd_str)
            if price <= 0:
                return None
            self.logger.debug("DexScreener price for %s: %f", contract, price)
            return price
        except Exception as exc:
            self.logger.error("DexScreener price failed for %s: %s", contract, exc)
            return None

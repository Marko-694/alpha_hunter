import time
from typing import Any, Dict

import yaml

from alpha_hunter.binance_client import BinanceClient
from alpha_hunter.binance_futures_client import BinanceFuturesClient
from alpha_hunter.explorer_client import ExplorerClient
from alpha_hunter.logger import get_logger
from alpha_hunter.scanner import AlphaScanner
from alpha_hunter.stablecoin_inflow_monitor import StablecoinInflowMonitor
from alpha_hunter.telegram_alerts import TelegramNotifier
from alpha_hunter.nansen_client import NansenClient
from alpha_hunter.nansen_monitor import NansenSmartMoneyMonitor


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def run_hunter() -> None:
    logger = get_logger("alpha_hunter")
    config = load_config("config.yaml")

    telegram_cfg = config.get("telegram", {})
    bot_token = telegram_cfg.get("bot_token", "")
    chat_ids_cfg = telegram_cfg.get("chat_ids") or []
    if not chat_ids_cfg and "chat_id" in telegram_cfg:
        chat_ids_cfg = [telegram_cfg.get("chat_id")]

    chat_ids: list[int] = []
    for cid in chat_ids_cfg:
        try:
            chat_ids.append(int(cid))
        except (TypeError, ValueError):
            logger.error("Invalid chat_id in config (skipped): %s", cid)
    if not bot_token or not chat_ids:
        logger.error("Telegram bot_token or chat_ids missing/invalid in config")
        return

    binance_client = BinanceClient(logger=logger)
    explorer_client = ExplorerClient(config=config, logger=logger)
    notifier = TelegramNotifier(bot_token=bot_token, chat_ids=chat_ids, logger=logger)

    nansen_cfg = config.get("nansen", {}) or {}
    nansen_client = None
    nansen_monitor = None
    api_key = nansen_cfg.get("api_key")
    if nansen_cfg.get("enabled") and api_key:
        nansen_client = NansenClient(
            api_key=api_key,
            base_url=nansen_cfg.get("base_url", "https://api.nansen.ai"),
            logger=logger,
        )
        nansen_monitor = NansenSmartMoneyMonitor(
            nansen_client=nansen_client,
            notifier=notifier,
            config=config,
            logger=logger,
        )

    try:
        notifier.send_message("✅ Alpha Hunter запущен. Тестовое сообщение.")
    except Exception as exc:
        logger.error("Failed to send startup test message: %s", exc)

    futures_client = BinanceFuturesClient(logger=logger)

    scanner = AlphaScanner(
        binance_client,
        notifier,
        config=config,
        logger=logger,
        futures_client=futures_client,
        nansen_client=nansen_client,
    )
    stablecoin_inflow_monitor = StablecoinInflowMonitor(
        notifier=notifier,
        config=config,
        logger=logger,
        explorer_client=explorer_client,
    )

    interval_seconds = scanner.interval_seconds

    logger.info("Alpha Hunter started with interval %ss", interval_seconds)
    try:
        while True:
            logger.info("Scan cycle start")
            scanner.scan_once()
            stablecoin_inflow_monitor.run_once()
            alpha_tokens = config.get("alpha_tokens", [])
            if nansen_monitor is not None and alpha_tokens:
                nansen_monitor.run_once(alpha_tokens)
            logger.info("Scan cycle end")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    run_hunter()

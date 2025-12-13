import logging
from typing import Iterable, Optional

import requests

from .logger import get_logger


class TelegramNotifier:
    API_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"
    TIMEOUT = 10

    def __init__(
        self,
        bot_token: str,
        chat_ids: Iterable[int],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.bot_token = bot_token
        # дедуплицируем и приводим к списку int
        self.chat_ids = [int(cid) for cid in dict.fromkeys(chat_ids)]
        self.logger = logger or get_logger(self.__class__.__name__)

    def send_message(self, text: str) -> None:
        url = self.API_URL_TEMPLATE.format(token=self.bot_token)
        for chat_id in self.chat_ids:
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
            try:
                resp = requests.post(url, json=payload, timeout=self.TIMEOUT)
                resp.raise_for_status()
            except requests.RequestException as exc:
                self.logger.error("Telegram send failed for chat %s: %s", chat_id, exc)

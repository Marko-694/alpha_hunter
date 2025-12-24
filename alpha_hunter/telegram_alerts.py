import logging
import os
import time
from typing import Any, Dict, Iterable, Optional

import requests

from .hunter_state import can_send_alert, record_alert_sent
from .logger import get_logger


class TelegramNotifier:
    API_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"
    TIMEOUT = 10

    def __init__(
        self,
        bot_token: str,
        chat_ids: Iterable[int],
        *,
        dry_run: bool | None = None,
        state: Optional[Dict[str, Any]] = None,
        alerts_cfg: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.bot_token = bot_token
        self.chat_ids = [int(cid) for cid in dict.fromkeys(chat_ids)]
        self.logger = logger or get_logger(self.__class__.__name__)
        alerts_cfg = alerts_cfg or {}
        env_dry_run = os.getenv("HUNTER_DRY_RUN") == "1"
        self.dry_run = bool(dry_run) or env_dry_run or bool(alerts_cfg.get("dry_run"))
        self.state = state
        self.max_per_hour = int(alerts_cfg.get("max_per_hour", 0))
        self.rate_limit_seconds = int(alerts_cfg.get("per_address_cooldown_seconds", 90))
        self._last_sent: Dict[str, float] = {}

    def send_message(
        self,
        text: str,
        *,
        alert_kind: Optional[str] = None,
        address: Optional[str] = None,
        parse_mode: Optional[str] = "Markdown",
    ) -> None:
        flattened = text.replace("\n", " | ")
        if self.dry_run:
            self.logger.info("[DRY-RUN] would_send: %s", flattened)
            return

        now_ts = time.time()
        rate_key = self._build_rate_key(alert_kind, address, flattened)
        last_ts = self._last_sent.get(rate_key)
        if last_ts and (now_ts - last_ts) < self.rate_limit_seconds:
            wait = self.rate_limit_seconds - (now_ts - last_ts)
            self.logger.warning(
                "[DROP RATE] key=%s wait=%.1fs text=%s",
                rate_key,
                max(wait, 0.0),
                flattened,
            )
            return

        if self.state is not None and self.max_per_hour > 0:
            if not can_send_alert(self.state, now_ts, self.max_per_hour):
                self.logger.warning("[DROP STORM] text=%s", flattened)
                return
            record_alert_sent(self.state, now_ts)

        url = self.API_URL_TEMPLATE.format(token=self.bot_token)
        for chat_id in self.chat_ids:
            payload = {"chat_id": chat_id, "text": text}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            self._send_with_fallback(url, payload, chat_id, flattened)

        self._last_sent[rate_key] = now_ts
        self.logger.debug("[SEND] key=%s text=%s", rate_key, flattened)

    def _send_with_fallback(
        self,
        url: str,
        payload: Dict[str, Any],
        chat_id: int,
        flattened: str,
    ) -> None:
        try:
            resp = requests.post(url, json=payload, timeout=self.TIMEOUT)
            if resp.status_code == 400 and payload.get("parse_mode"):
                self.logger.warning(
                    "[TG][400] chat=%s len=%d parse_mode=%s resp=%s",
                    chat_id,
                    len(payload.get("text") or ""),
                    payload.get("parse_mode"),
                    resp.text[:200],
                )
                fallback = dict(payload)
                fallback.pop("parse_mode", None)
                resp = requests.post(url, json=fallback, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except requests.HTTPError as exc:
            resp = getattr(exc, "response", None)
            body = resp.text[:200] if resp is not None and resp.text else ""
            self.logger.error(
                "Telegram send failed chat=%s status=%s body=%s payload={len=%d parse=%s}",
                chat_id,
                getattr(resp, "status_code", None),
                body,
                len(payload.get("text") or ""),
                payload.get("parse_mode"),
            )
            if resp is not None and resp.status_code in (400, 403):
                self.logger.error(
                    "Telegram chat unreachable (chat_id=%s). Ensure bot is added and /start was sent.",
                    chat_id,
                )
        except requests.RequestException as exc:
            self.logger.error("Telegram send failed for chat %s: %s (%s)", chat_id, exc, flattened)

    def _build_rate_key(self, alert_kind: Optional[str], address: Optional[str], text: str) -> str:
        parts = []
        if alert_kind:
            parts.append(str(alert_kind).lower())
        if address:
            parts.append(str(address).lower())
        if not parts:
            parts.append("generic")
            parts.append(str(hash(text))[-6:])
        return "|".join(parts)

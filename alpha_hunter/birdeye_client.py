import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

API_URL = "https://public-api.birdeye.so/defi/v3/token/txs"
PAGE_LIMIT = 100
MAX_RETRIES = 5
SPENT_FILE = "data/birdeye_api_keys_spent.json"
PROXY_FILE = "data/birdeye_proxies.txt"
ROTATE_STATUSES = {401, 402, 403, 409, 420, 429}
CACHE_DIR = "data/raw_cache/birdeye"


def _get_logger(logger: Optional[logging.Logger]) -> logging.Logger:
    return logger or logging.getLogger(__name__)


def _load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _save_yaml(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True)


def _load_proxies(path: str = PROXY_FILE) -> List[str]:
    if not os.path.exists(path):
        return []
    proxies: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            val = line.strip()
            if val:
                proxies.append(val)
    return proxies


def _page_cache_path(
    chain: str, contract: str, start_ts: int, end_ts: int, offset: int, limit: int
) -> str:
    window = f"window_{start_ts}_{end_ts}"
    fname = f"page_{offset}_{limit}.json"
    return os.path.join(CACHE_DIR, chain, contract.lower(), window, fname)


def _read_cache(path: str) -> Optional[Any]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.getLogger(__name__).debug("CACHE_HIT birdeye path=%s", path)
        return data
    except Exception:
        return None


def _write_cache(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)
    logging.getLogger(__name__).debug("CACHE_WRITE birdeye path=%s", path)


class BirdEyeKeyManager:
    def __init__(
        self,
        api_keys: List[str],
        proxies: List[str],
        config_path: str = "config.yaml",
        spent_path: str = SPENT_FILE,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.api_keys = [k for k in api_keys if k]
        self.proxies = proxies or []
        self.config_path = config_path
        self.spent_path = spent_path
        self.logger = _get_logger(logger)
        self.index = 0

    def has_keys(self) -> bool:
        return len(self.api_keys) > 0

    def current(self) -> Tuple[Optional[str], Optional[str]]:
        if not self.api_keys:
            return None, None
        key = self.api_keys[self.index]
        proxy = None
        if self.proxies:
            proxy = self.proxies[self.index % len(self.proxies)]
        return key, proxy

    def rotate(self) -> Tuple[Optional[str], Optional[str]]:
        if not self.api_keys:
            return None, None
        self.index = (self.index + 1) % len(self.api_keys)
        key, proxy = self.current()
        self.logger.info(
            "BirdEye rotating to key #%d (%s...%s) proxy=%s",
            self.index,
            key[:4],
            key[-4:],
            proxy or "none",
        )
        return key, proxy

    def _append_spent(self, api_key: str, proxy: Optional[str], reason: str) -> None:
        os.makedirs(os.path.dirname(self.spent_path) or ".", exist_ok=True)
        spent: List[Dict[str, Any]] = []
        if os.path.exists(self.spent_path):
            try:
                with open(self.spent_path, "r", encoding="utf-8") as f:
                    spent = json.load(f) or []
            except Exception:
                spent = []
        spent.append(
            {
                "api_key": api_key,
                "proxy": proxy,
                "reason": reason,
                "spent_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )
        with open(self.spent_path, "w", encoding="utf-8") as f:
            json.dump(spent, f, ensure_ascii=False, indent=2)

    def _remove_from_config(self, api_key: str) -> None:
        cfg = _load_yaml(self.config_path)
        birdeye = cfg.get("birdeye", {}) or {}
        keys = birdeye.get("api_keys") or []
        if api_key in keys:
            keys = [k for k in keys if k != api_key]
            birdeye["api_keys"] = keys
            cfg["birdeye"] = birdeye
            _save_yaml(self.config_path, cfg)
            self.logger.info("BirdEye key removed from config: %s...%s", api_key[:4], api_key[-4:])

    def mark_spent(self, reason: str) -> None:
        key, proxy = self.current()
        if not key:
            return
        self.logger.warning(
            "BirdEye key marked spent (%s...%s): %s", key[:4], key[-4:], reason
        )
        self._append_spent(key, proxy, reason)
        self._remove_from_config(key)
        # remove from in-memory list
        if key in self.api_keys:
            idx = self.api_keys.index(key)
            self.api_keys.pop(idx)
            if self.index >= len(self.api_keys):
                self.index = 0


class BirdEyeClient:
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        config_path: str = "config.yaml",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.logger = _get_logger(logger)
        self.config_path = config_path
        self.config = config or _load_yaml(config_path)
        birdeye_cfg = self.config.get("birdeye", {}) if isinstance(self.config, dict) else {}
        keys = birdeye_cfg.get("api_keys") or []
        if not keys and birdeye_cfg.get("api_key"):
            keys = [birdeye_cfg.get("api_key")]
        proxies = _load_proxies()
        self.chain = birdeye_cfg.get("chain", "bsc")
        self.rotate_statuses = set(birdeye_cfg.get("rotate_on_http_status") or ROTATE_STATUSES)
        self.manager = BirdEyeKeyManager(
            api_keys=keys,
            proxies=proxies,
            config_path=config_path,
            logger=self.logger,
        )
        self.cache_stats: Dict[str, int] = {
            "cache_hits": 0,
            "cache_misses": 0,
            "api_calls": 0,
            "pages_cached": 0,
            "pages_missing": 0,
        }
        self.cache_meta_snapshot: Dict[str, Any] = {}
        self.contract_calls: Dict[str, int] = {}
        self.contract_pages: Dict[str, int] = {}
        self.page_cache_enabled = bool(birdeye_cfg.get("page_cache", {}).get("enabled", False))

    def _get_headers(self, api_key: str) -> Dict[str, str]:
        return {
            "accept": "application/json",
            "x-api-key": api_key,
            "x-chain": self.chain,
        }

    def _should_mark_spent(self, resp: requests.Response, data: Any) -> bool:
        if resp.status_code in self.rotate_statuses:
            return True
        if isinstance(data, dict):
            msg = str(data.get("message") or data.get("error") or "").lower()
            if any(k in msg for k in ["quota", "limit", "exceeded", "insufficient", "plan"]):
                return True
        return False

    def _request_page(
        self,
        params: Dict[str, Any],
        label: str,
    ) -> Optional[requests.Response]:
        attempts = 0
        while self.manager.has_keys() and attempts < max(MAX_RETRIES, len(self.manager.api_keys) * 2):
            api_key, proxy = self.manager.current()
            if not api_key:
                return None
            proxies = {"http": proxy, "https": proxy} if proxy else None
            self.logger.info(
                "[%s] using key %s...%s proxy=%s",
                label,
                api_key[:4],
                api_key[-4:],
                proxy or "none",
            )
            try:
                self.cache_stats["api_calls"] = self.cache_stats.get("api_calls", 0) + 1
                resp = requests.get(
                    API_URL,
                    headers=self._get_headers(api_key),
                    params=params,
                    timeout=30,
                    proxies=proxies,
                )
            except Exception as exc:
                attempts += 1
                self.logger.error("[%s] request error: %s", label, exc)
                self.manager.rotate()
                continue

            if resp.status_code in self.rotate_statuses:
                self.manager.mark_spent(f"HTTP {resp.status_code}")
                self.manager.rotate()
                attempts += 1
                continue

            if resp.status_code != 200:
                attempts += 1
                self.logger.error("[%s] HTTP %s: %s", label, resp.status_code, resp.text[:200])
                self.manager.rotate()
                continue

            try:
                data = resp.json()
            except Exception:
                data = None

            if self._should_mark_spent(resp, data):
                self.manager.mark_spent(f"quota/limit message: {resp.text[:120]}")
                self.manager.rotate()
                attempts += 1
                continue

            return resp

        return None

    def fetch_for_window(
        self,
        token_address: str,
        start_ts: int,
        end_ts: int,
        label: str,
        chain: Optional[str] = None,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Получает swap-транзакции токена за окно [start_ts; end_ts] через BirdEye API.
        Включена ротация API-ключей и прокси.
        """
        if not self.manager.has_keys():
            self.logger.error("[%s] No BirdEye API keys available", label)
            return []

        chain_use = chain or self.chain
        self.chain = chain_use  # обновим для заголовков

        offset = 0
        page = 1
        items_all: List[Dict[str, Any]] = []

        self.logger.info("[%s] Unix window: %s .. %s", label, start_ts, end_ts)

        while True:
            params = {
                "address": token_address,
                "offset": offset,
                "limit": PAGE_LIMIT,
                "tx_type": "swap",
                "after_time": int(start_ts),
                "before_time": int(end_ts),
            }

            cache_path = _page_cache_path(self.chain, token_address, start_ts, end_ts, offset, PAGE_LIMIT)
            data = None
            if self.page_cache_enabled and not force_refresh:
                cached = _read_cache(cache_path)
                if cached is not None:
                    self.cache_stats["cache_hits"] = self.cache_stats.get("cache_hits", 0) + 1
                    data = cached
                else:
                    self.cache_stats["cache_misses"] = self.cache_stats.get("cache_misses", 0) + 1

            if data is None:
                resp = self._request_page(params, f"{label} page={page}")
                if resp is None:
                    break
                try:
                    data = resp.json()
                except Exception as exc:
                    self.logger.error("[%s] invalid JSON: %s", label, exc)
                    break
                if self.page_cache_enabled:
                    try:
                        _write_cache(cache_path, data)
                        self.cache_stats["pages_cached"] = self.cache_stats.get("pages_cached", 0) + 1
                    except Exception:
                        pass

            if isinstance(data, dict) and data.get("success") is False:
                msg = data.get("message") or ""
                self.logger.error("[%s] API error: %s", label, msg)
                # если квота — отметим ключ и вращаем
                if any(k in msg.lower() for k in ["quota", "limit", "exceeded", "plan"]):
                    self.manager.mark_spent(f"API error: {msg}")
                    self.manager.rotate()
                    continue
                break

            data_root = data.get("data", {}) if isinstance(data, dict) else {}
            items = data_root.get("items") or []
            has_next = bool(data_root.get("has_next"))
            # per-contract accounting
            self.contract_calls[token_address] = self.contract_calls.get(token_address, 0) + 1
            self.contract_pages[token_address] = self.contract_pages.get(token_address, 0) + 1

            self.logger.info("[%s] page %s: %d items, has_next=%s", label, page, len(items), has_next)

            if not items:
                self.cache_stats["pages_missing"] = self.cache_stats.get("pages_missing", 0) + 1
                break

            items_all.extend(items)
            # если есть next_page_key в ответе (новая схема), можно использовать offset обновлённый
            # здесь оставляем offset += PAGE_LIMIT для совместимости

            if not has_next:
                self.logger.info("[%s] pagination finished", label)
                break

            offset += PAGE_LIMIT
            page += 1
            time.sleep(0.5)

        self.logger.info("[%s] total items fetched: %d", label, len(items_all))
        # снимок кэша для cache_meta
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.cache_meta_snapshot = {
            "pages_cached": self.cache_stats.get("pages_cached", 0),
            "cache_hits": self.cache_stats.get("cache_hits", 0),
            "cache_misses": self.cache_stats.get("cache_misses", 0),
            "pages_missing": self.cache_stats.get("pages_missing", 0),
            "api_calls": self.cache_stats.get("api_calls", 0),
            "last_updated": now_iso,
            "contracts_tracked": len(self.contract_calls),
            "api_calls_by_contract": self.contract_calls,
            "pages_by_contract": self.contract_pages,
            "page_cache_enabled": self.page_cache_enabled,
        }
        try:
            cache_meta_path = os.path.join("data", "raw_cache", "birdeye", "cache_meta.json")
            os.makedirs(os.path.dirname(cache_meta_path), exist_ok=True)
            existing = {}
            if os.path.exists(cache_meta_path):
                try:
                    with open(cache_meta_path, "r", encoding="utf-8") as f:
                        existing = json.load(f) or {}
                except Exception:
                    existing = {}
            if isinstance(existing, dict):
                first_seen = existing.get("first_seen") or now_iso
            else:
                first_seen = now_iso
            payload = dict(existing if isinstance(existing, dict) else {})
            payload.update(self.cache_meta_snapshot)
            payload["first_seen"] = first_seen
            with open(cache_meta_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        return items_all


_default_client: Optional[BirdEyeClient] = None


def _get_default_client(logger: Optional[logging.Logger] = None) -> BirdEyeClient:
    global _default_client
    if _default_client is None:
        _default_client = BirdEyeClient(logger=logger)
    return _default_client


def fetch_for_window(
    token_address: str,
    start_ts: int,
    end_ts: int,
    label: str,
    logger: Optional[logging.Logger] = None,
    chain: Optional[str] = None,
    client: Optional[BirdEyeClient] = None,
) -> List[Dict[str, Any]]:
    client_use = client or _get_default_client(logger=logger)
    return client_use.fetch_for_window(
        token_address=token_address,
        start_ts=start_ts,
        end_ts=end_ts,
        label=label,
        chain=chain,
    )


def dry_run_rotation_simulated(reason: str = "forced_429", logger: Optional[logging.Logger] = None) -> None:
    """
    Простейший dry-run: помечает текущий ключ как потраченный и вращает.
    """
    client = _get_default_client(logger=logger)
    client.manager.mark_spent(reason)
    client.manager.rotate()

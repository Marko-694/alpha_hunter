import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# chain ids (balances) and names (bucket API)
CHAIN_IDS: Dict[str, int] = {
    "eth": 1,
    "bsc": 56,
    "polygon": 137,
    "arb": 42161,
    "optimism": 10,
    "base": 8453,
}

CHAIN_NAMES: Dict[str, str] = {
    "eth": "eth-mainnet",
    "bsc": "bsc-mainnet",
    "polygon": "matic-mainnet",
    "arb": "arb-mainnet",
    "optimism": "opt-mainnet",
    "base": "base-mainnet",
}


class CovalentClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        logger: Optional[logging.Logger] = None,
        timeout: int = 40,
        max_retries: int = 6,
        max_calls_per_minute: Optional[int] = None,
        cache_dir: str = "data/raw_cache/covalent",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.logger = logger or logging.getLogger(__name__)
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_calls_per_minute = max_calls_per_minute
        self.cache_dir = cache_dir
        self._calls: List[float] = []
        self.stats = self._init_stats()
        self.api_key_masked = self._mask_api_key(api_key)
        self.debug_enabled = os.getenv("COVALENT_DEBUG") == "1"
        self.cache_ttl_default = int(os.getenv("COVALENT_CACHE_TTL", "600") or 600)
        self._sampled_addresses: Set[str] = set()

    # --- rate limiting ---
    def _throttle(self) -> None:
        if not self.max_calls_per_minute or self.max_calls_per_minute <= 0:
            return
        now = time.time()
        window = 60.0
        self._calls = [t for t in self._calls if now - t < window]
        if len(self._calls) >= self.max_calls_per_minute:
            earliest = min(self._calls)
            sleep_s = window - (now - earliest)
            sleep_s = max(sleep_s, 0.0) + random.random() * 0.5
            self.logger.info("[COVALENT][LOCAL THROTTLE] sleeping %.2fs (rate limit)", sleep_s)
            self._record_sleep(sleep_s)
            time.sleep(sleep_s)
        self._calls.append(time.time())

    # --- cache helpers ---
    def _cache_path(self, chain_name: str, address: str, bucket: int) -> str:
        return os.path.join(self.cache_dir, chain_name, address.lower(), f"bucket_{bucket}.json")

    def _read_cache(
        self,
        chain_name: str,
        address: str,
        bucket: int,
        *,
        cache_ttl_seconds: int,
        force_refresh: bool,
    ) -> Optional[Dict[str, Any]]:
        if force_refresh:
            self.logger.info(
                "[COVALENT][FORCE] chain=%s bucket=%s addr=%s",
                chain_name,
                bucket,
                self._short_addr(address),
            )
            return None
        path = self._cache_path(chain_name, address, bucket)
        if not os.path.exists(path):
            self.logger.info(
                "[COVALENT][CACHE MISS] chain=%s bucket=%s addr=%s",
                chain_name,
                bucket,
                self._short_addr(address),
            )
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return None
        items = None
        meta = {}
        if isinstance(data, dict):
            if "items" in data:
                items = data["items"]
                meta = data.get("_meta") or {}
            elif "data" in data and isinstance(data["data"], dict):
                items = data["data"].get("items")
        if items is None:
            items = data
        fetched_at = meta.get("fetched_at")
        now = time.time()
        age = None
        if fetched_at:
            try:
                dt = datetime.fromisoformat(str(fetched_at).replace("Z", "+00:00"))
                age = max(0.0, now - dt.timestamp())
            except Exception:
                age = None
        if age is None or age > cache_ttl_seconds:
            self.logger.info(
                "[COVALENT][STALE] chain=%s bucket=%s addr=%s age=%.0fs ttl=%ss",
                chain_name,
                bucket,
                self._short_addr(address),
                age or -1,
                cache_ttl_seconds,
            )
            return None
        self.stats["cache_hits"] += 1
        self.logger.info(
            "[COVALENT][CACHE HIT] chain=%s bucket=%s addr=%s age=%.0fs",
            chain_name,
            bucket,
            self._short_addr(address),
            age,
        )
        return {"items": items, "_meta": meta}

    def _write_cache(self, chain_name: str, address: str, bucket: int, data: Dict[str, Any]) -> None:
        path = self._cache_path(chain_name, address, bucket)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        payload = {"_meta": {"fetched_at": datetime.now(timezone.utc).isoformat()}, "items": data.get("items", data)}
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, path)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("CACHE_WRITE bucket key=%s/%s/%s path=%s", chain_name, address.lower(), bucket, path)

    def _request(
        self,
        path: str,
        params: Dict[str, Any],
        diag: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        params = dict(params or {})
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        backoff_base = 5
        for attempt in range(self.max_retries):
            self._throttle()
            self._log_http(context, attempt + 1, url)
            try:
                if diag is not None:
                    diag["api_calls"] = diag.get("api_calls", 0) + 1
                self.stats["api_calls"] += 1
                if diag is not None:
                    diag["last_url"] = url
                resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
                status_code = resp.status_code
                self.stats["last_status"] = status_code
                self.stats["last_url"] = resp.url
                if self.debug_enabled:
                    self.logger.info("[COVALENT][DEBUG] status=%s url=%s", status_code, resp.url)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    retry_after_val = None
                    if retry_after:
                        try:
                            retry_after_val = float(retry_after)
                            sleep_s = retry_after_val
                        except ValueError:
                            sleep_s = backoff_base * (2 ** attempt)
                    else:
                        sleep_s = backoff_base * (2 ** attempt)
                    sleep_s = min(sleep_s, 60) + random.random() * 0.5
                    bucket = (context or {}).get("bucket")
                    addr = (context or {}).get("address")
                    self.logger.warning(
                        "[COVALENT][429] bucket=%s addr=%s retry_after=%s attempt=%d/%d",
                        bucket,
                        self._short_addr(addr),
                        retry_after,
                        attempt + 1,
                        self.max_retries,
                    )
                    if diag is not None:
                        diag["http_429_count"] = diag.get("http_429_count", 0) + 1
                    self.stats["http_429"] += 1
                    if retry_after_val is not None:
                        self.stats["retry_after_seconds_total"] += max(retry_after_val, 0.0)
                    self.logger.info("[COVALENT][SERVER THROTTLE] sleeping %.1fs", sleep_s)
                    self._record_sleep(sleep_s)
                    time.sleep(sleep_s)
                    continue

                resp.raise_for_status()
                self.stats["http_200"] += 1
                data = resp.json()
                if not isinstance(data, dict):
                    self.logger.error("Covalent response not dict: %s", data)
                    return None
                if data.get("error"):
                    self.logger.error("Covalent error: %s", data.get("error_message"))
                    return None
                return data
            except requests.HTTPError as exc:
                status = getattr(exc.response, "status_code", None)
                if status is not None and status != 429:
                    self.stats["http_other"] += 1
                self.logger.error(
                    "Covalent request failed %s (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    self.max_retries,
                    exc,
                )
                if attempt < self.max_retries - 1:
                    sleep_s = min(backoff_base * (2 ** attempt), 60) + random.random() * 0.5
                    self.logger.info("Sleeping %.1fs before retry", sleep_s)
                    self._record_sleep(sleep_s)
                    time.sleep(sleep_s)
                else:
                    return None
            except Exception as exc:
                self.logger.error(
                    "Covalent request failed %s (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    self.max_retries,
                    exc,
                )
                if attempt < self.max_retries - 1:
                    sleep_s = min(backoff_base * (2 ** attempt), 60) + random.random() * 0.5
                    self.logger.info("Sleeping %.1fs before retry", sleep_s)
                    self._record_sleep(sleep_s)
                    time.sleep(sleep_s)
                else:
                    return None
        return None

    # legacy wrapper for compatibility
    def get_token_transfers(
        self,
        chain: str,
        address: str,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        txs, _ = self.iter_transactions_window(
            chain=chain,
            address=address,
            start_ts=from_ts or 0,
            end_ts=to_ts or int(time.time()),
        )
        return txs

    def get_time_bucket_transactions(
        self,
        chain_name: str,
        address: str,
        bucket: int,
        quote_currency: str = "USD",
        no_logs: bool = False,
        use_cache: bool = True,
        diag: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        *,
        cache_ttl_seconds: Optional[int] = None,
        force_refresh: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        if not self.api_key:
            return None

        ttl = cache_ttl_seconds if cache_ttl_seconds is not None else self.cache_ttl_default
        if use_cache:
            cached = self._read_cache(
                chain_name,
                address,
                bucket,
                cache_ttl_seconds=ttl,
                force_refresh=force_refresh,
            )
            if cached is not None:
                if diag is not None:
                    diag["cache_hits"] = diag.get("cache_hits", 0) + 1
                return cached.get("items") if isinstance(cached, dict) else cached

        path = f"/v1/{chain_name}/bulk/transactions/{address}/{bucket}/"
        params: Dict[str, Any] = {
            "quote-currency": quote_currency,
            "no-logs": str(no_logs).lower(),
        }
        if diag is not None:
            diag["cache_misses"] = diag.get("cache_misses", 0) + 1
        self.stats["cache_misses"] += 1
        data = self._request(path, params=params, diag=diag, context=context)
        if not data:
            return None
        items = data.get("data", {}).get("items") or []
        try:
            self._write_cache(chain_name, address, bucket, {"items": items})
        except Exception:
            pass
        return items

    def iter_transactions_window(
        self,
        chain: str,
        address: str,
        start_ts: int,
        end_ts: int,
        quote_currency: str = "USD",
        no_logs: bool = False,
        empty_streak_limit: int = 20,
        max_buckets: int = 600,
        *,
        cache_ttl_seconds: Optional[int] = None,
        force_refresh: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        chain_name = CHAIN_NAMES.get(chain)
        if chain_name is None or not self.api_key:
            return [], {}

        ttl = cache_ttl_seconds if cache_ttl_seconds is not None else self.cache_ttl_default
        start_bucket = start_ts // 900
        end_bucket = end_ts // 900
        items_out: List[Dict[str, Any]] = []
        buckets_fetched = 0
        errors = 0
        cache_hits = 0
        cache_misses = 0
        api_calls = 0
        http_429 = 0
        failed_buckets: List[int] = []
        step = 1
        empty_streak = 0
        processed = 0
        last_bucket = start_bucket - 1
        sleep_start = self.stats["sleep_seconds_total"]

        dropped_stats = {
            "dropped_empty_token": 0,
            "dropped_no_log_events": 0,
            "dropped_zero_amount_no_usd": 0,
            "dropped_missing_log_index": 0,
        }
        current = start_bucket
        while current <= end_bucket:
            if processed >= max_buckets:
                self.logger.error(
                    "Reached max buckets (%d) for %s:%s", max_buckets, chain, address
                )
                break
            processed += 1
            attempts = 0
            bucket_items: Optional[List[Dict[str, Any]]] = None
            while attempts < self.max_retries:
                if processed % 10 == 0 or attempts > 0:
                    self.logger.info(
                        "Covalent bucket %s/%s for %s (%s), attempt %d",
                        current,
                        end_bucket,
                        address,
                        chain_name,
                        attempts + 1,
                    )
                try:
                    before = time.time()
                    # try cache
                    cached = self._read_cache(
                        chain_name,
                        address,
                        current,
                        cache_ttl_seconds=ttl,
                        force_refresh=force_refresh,
                    )
                    if cached is not None:
                        bucket_items = cached.get("items") if isinstance(cached, dict) else cached
                        cache_hits += 1
                        break

                    # fetch
                    diag_bucket = {"api_calls": 0, "http_429_count": 0, "cache_misses": 0, "cache_hits": 0}
                    bucket_items = self.get_time_bucket_transactions(
                        chain_name=chain_name,
                        address=address,
                        bucket=current,
                        quote_currency=quote_currency,
                        no_logs=no_logs,
                        use_cache=False,
                        diag=diag_bucket,
                        context={
                            "bucket": current,
                            "end_bucket": end_bucket,
                            "address": address,
                            "chain": chain_name,
                        },
                        cache_ttl_seconds=ttl,
                        force_refresh=force_refresh,
                    )
                    cache_misses += diag_bucket.get("cache_misses", 0)
                    api_calls += diag_bucket.get("api_calls", 0)
                    http_429 += diag_bucket.get("http_429_count", 0)
                    if bucket_items is None:
                        attempts += 1
                        continue
                    break
                except requests.HTTPError as he:
                    attempts += 1
                    if hasattr(he, "response") and he.response is not None and he.response.status_code == 429:
                        http_429 += 1
                    continue
                except Exception:
                    attempts += 1
                    continue

            if bucket_items is None:
                errors += 1
                failed_buckets.append(current)
                current += step
                continue

            last_bucket = current
            buckets_fetched += 1
            if not bucket_items:
                empty_streak += 1
            else:
                empty_streak = 0

            if empty_streak >= empty_streak_limit:
                step = min(step * 4, 16)
            else:
                step = 1

            if bucket_items:
                self._maybe_log_sample(address, bucket_items[0])
            normalized, drop_diag = self._extract_token_transfers(
                bucket_items or [],
                wallet=address,
                chain=chain,
                chain_name=chain_name,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            items_out.extend(normalized)
            for key in dropped_stats:
                dropped_stats[key] += drop_diag.get(key, 0)

            current += step

        items_out.sort(key=lambda x: x.get("timestamp", 0))
        sleep_total = max(0.0, self.stats["sleep_seconds_total"] - sleep_start)
        diag = {
            "chain_name": chain_name,
            "start_bucket": start_bucket,
            "end_bucket": end_bucket,
            "last_bucket_processed": last_bucket,
            "buckets_fetched": buckets_fetched,
            "buckets_total": max(0, end_bucket - start_bucket + 1),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "buckets_needed": max(0, end_bucket - start_bucket + 1),
            "api_calls": api_calls,
            "http_429_count": http_429,
            "sleep_seconds_total": sleep_total,
            "tx_items_count": len(items_out),
            "errors_count": errors,
            "max_buckets": max_buckets,
            "failed_buckets": failed_buckets,
            "dropped_empty_token": dropped_stats["dropped_empty_token"],
            "dropped_no_log_events": dropped_stats["dropped_no_log_events"],
            "dropped_zero_amount_no_usd": dropped_stats["dropped_zero_amount_no_usd"],
            "dropped_missing_log_index": dropped_stats["dropped_missing_log_index"],
        }
        return items_out, diag

    def get_token_balances(self, chain: str, address: str) -> List[Dict[str, Any]]:
        chain_id = CHAIN_IDS.get(chain)
        if chain_id is None or not self.api_key:
            return []
        path = f"/v1/{chain_id}/address/{address}/balances_v2/"
        data = self._request(path, params={}, context={"address": address})
        if not data:
            return []
        items = data.get("data", {}).get("items") or []
        balances: List[Dict[str, Any]] = []
        for it in items:
            try:
                balance_raw = it.get("balance") or "0"
                decimals = int(it.get("contract_decimals") or 0)
                bal = (
                    float(balance_raw) / (10**decimals)
                    if decimals >= 0
                    else float(balance_raw)
                )
                if bal == 0:
                    continue
                usd_val = it.get("quote") or it.get("quote_rate") or None
                usd_val = float(usd_val) if usd_val is not None else None
                balances.append(
                    {
                        "token_address": str(it.get("contract_address") or "").lower(),
                        "token_symbol": it.get("contract_ticker_symbol"),
                        "token_decimals": decimals,
                        "balance": bal,
                        "usd_value": usd_val,
                    }
                )
            except Exception:
                continue
        balances.sort(key=lambda x: x.get("usd_value") or 0, reverse=True)
        return balances

    # --- helpers ---
    def _init_stats(self) -> Dict[str, Any]:
        return {
            "api_calls": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "http_200": 0,
            "http_429": 0,
            "http_other": 0,
            "retry_after_seconds_total": 0.0,
            "sleep_seconds_total": 0.0,
            "last_status": None,
            "last_url": None,
        }

    def stats_snapshot(self, reset: bool = False) -> Dict[str, Any]:
        snap = dict(self.stats)
        snap["base_url"] = self.base_url
        snap["api_key_masked"] = self.api_key_masked
        if reset:
            self.stats = self._init_stats()
        return snap

    def _mask_api_key(self, key: str) -> str:
        if not key:
            return ""
        if len(key) <= 10:
            return f"{key[:2]}***{key[-2:]}"
        return f"{key[:6]}...{key[-4:]}"

    def _short_addr(self, addr: Optional[str]) -> str:
        if not addr:
            return ""
        addr = addr.lower()
        if len(addr) <= 12:
            return addr
        return f"{addr[:6]}..{addr[-4:]}"

    def _record_sleep(self, seconds: float) -> None:
        if seconds is None:
            return
        self.stats["sleep_seconds_total"] += max(seconds, 0.0)

    def _log_cache_hit(self, chain_name: str, address: str, bucket: int) -> None:
        self.logger.info(
            "[COVALENT][CACHE HIT] chain=%s bucket=%s addr=%s",
            chain_name,
            bucket,
            self._short_addr(address),
        )

    def _maybe_log_sample(self, address: str, item: Dict[str, Any]) -> None:
        addr_key = str(address).lower()
        if not item or addr_key in self._sampled_addresses:
            return
        self._sampled_addresses.add(addr_key)
        keys = sorted(list(item.keys()))[:12]
        log_events = item.get("log_events") or []
        self.logger.info(
            "[COVALENT] sample keys=%s has_log_events=%s log_events=%d addr=%s",
            keys,
            bool(log_events),
            len(log_events),
            self._short_addr(address),
        )

    @staticmethod
    def _parse_timestamp(item: Dict[str, Any]) -> Optional[int]:
        ts_iso = item.get("block_signed_at") or item.get("timestamp")
        if ts_iso:
            try:
                ts_dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
                return int(ts_dt.replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                pass
        try:
            return int(item.get("block_height") or 0)
        except Exception:
            return None

    @staticmethod
    def _decoded_param(params: List[Dict[str, Any]], names: List[str]) -> Optional[str]:
        target = {str(name).lower() for name in names}
        for param in params or []:
            name = str(param.get("name") or "").lower()
            if name in target:
                value = param.get("value")
                if isinstance(value, dict):
                    return value.get("value")
                return value
        return None

    @staticmethod
    def _normalize_numeric(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                if value.startswith("0x"):
                    return float(int(value, 16))
                return float(value)
            except Exception:
                try:
                    return float(int(value))
                except Exception:
                    return 0.0
        return 0.0

    @staticmethod
    def _direction_for_wallet(wallet: str, from_addr: str, to_addr: str) -> Optional[str]:
        wallet_norm = wallet.lower()
        if wallet_norm and wallet_norm == from_addr:
            return "out"
        if wallet_norm and wallet_norm == to_addr:
            return "in"
        return None

    def _extract_token_transfers(
        self,
        tx_items: List[Dict[str, Any]],
        *,
        wallet: str,
        chain: str,
        chain_name: str,
        start_ts: int,
        end_ts: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        records: List[Dict[str, Any]] = []
        diag = {
            "dropped_empty_token": 0,
            "dropped_no_log_events": 0,
            "dropped_zero_amount_no_usd": 0,
            "dropped_missing_log_index": 0,
        }
        wallet_norm = wallet.lower()
        native_token_key = f"{chain_name}:native"
        for tx in tx_items:
            ts_int = self._parse_timestamp(tx)
            if ts_int is None or ts_int < start_ts or ts_int > end_ts:
                continue
            log_events = tx.get("log_events") or []
            tx_hash = tx.get("tx_hash") or tx.get("hash")
            fees_paid = tx.get("fees_paid") or tx.get("gas_spent_quote")
            if log_events:
                for log_event in log_events:
                    decoded = log_event.get("decoded") or {}
                    event_name = str(decoded.get("name") or "").lower()
                    if event_name != "transfer":
                        continue
                    params = decoded.get("params") or []
                    from_addr = str(
                        self._decoded_param(params, ["from", "src", "sender"]) or ""
                    ).lower()
                    to_addr = str(
                        self._decoded_param(params, ["to", "dst", "recipient"]) or ""
                    ).lower()
                    direction = self._direction_for_wallet(wallet_norm, from_addr, to_addr)
                    if direction is None:
                        continue
                    raw_value = self._decoded_param(params, ["value", "wad", "amount"])
                    amount = self._normalize_numeric(raw_value)
                    decimals = int(log_event.get("sender_contract_decimals") or 0)
                    if decimals > 0:
                        amount = amount / (10 ** decimals)
                    amount = abs(amount)
                    token_address = str(
                        log_event.get("sender_address")
                        or log_event.get("sender_contract_address")
                        or ""
                    ).lower()
                    if not token_address:
                        diag["dropped_empty_token"] += 1
                        continue
                    log_index = log_event.get("log_offset")
                    if log_index is None:
                        log_index = log_event.get("log_index")
                    if log_index is None:
                        diag["dropped_missing_log_index"] += 1
                        continue
                    usd_value = log_event.get("value_quote")
                    pricing_missing = usd_value is None
                    signed_amount = amount if direction == "in" else -amount
                    if usd_value is None and signed_amount == 0:
                        diag["dropped_zero_amount_no_usd"] += 1
                        continue
                    records.append(
                        {
                            "timestamp": ts_int,
                            "tx_hash": tx_hash or "",
                            "wallet": wallet_norm,
                            "token_address": token_address,
                            "token_symbol": log_event.get("sender_contract_ticker_symbol"),
                            "token_decimals": decimals,
                            "amount": signed_amount,
                            "usd_value": usd_value,
                            "from_address": from_addr,
                            "to_address": to_addr,
                            "fees_paid": fees_paid,
                            "direction": direction,
                            "log_index": int(log_index),
                            "chain": chain,
                            "pricing_missing": pricing_missing,
                        }
                    )
                continue
            value_quote = tx.get("value_quote") or tx.get("fiat_value")
            native_value = self._normalize_numeric(tx.get("value"))
            if not value_quote and native_value == 0:
                diag["dropped_no_log_events"] += 1
                continue
            from_addr = str(tx.get("from_address") or "").lower()
            to_addr = str(tx.get("to_address") or "").lower()
            direction = self._direction_for_wallet(wallet_norm, from_addr, to_addr)
            if direction is None:
                diag["dropped_no_log_events"] += 1
                continue
            signed_amount = native_value if direction == "in" else -native_value
            log_index = tx.get("tx_offset") or tx.get("log_index")
            if log_index is None:
                log_index = 0
            pricing_missing = value_quote is None
            records.append(
                {
                    "timestamp": ts_int,
                    "tx_hash": tx_hash or "",
                    "wallet": wallet_norm,
                    "token_address": native_token_key,
                    "token_symbol": chain.upper(),
                    "token_decimals": 18,
                    "amount": signed_amount,
                    "usd_value": value_quote,
                    "from_address": from_addr,
                    "to_address": to_addr,
                    "fees_paid": fees_paid,
                    "direction": direction,
                    "log_index": int(log_index),
                    "chain": chain,
                    "pricing_missing": pricing_missing,
                }
            )
        return records, diag

    def _log_http(self, context: Optional[Dict[str, Any]], attempt: int, url: str) -> None:
        if context:
            bucket = context.get("bucket")
            end_bucket = context.get("end_bucket")
            addr = context.get("address")
            self.logger.info(
                "[COVALENT][HTTP] bucket=%s/%s addr=%s attempt=%d url=%s",
                bucket,
                end_bucket,
                self._short_addr(addr),
                attempt,
                url,
            )
        else:
            self.logger.info("[COVALENT][HTTP] attempt=%d url=%s", attempt, url)

import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
        cache_dir: str = "data/cache/covalent",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.logger = logger or logging.getLogger(__name__)
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_calls_per_minute = max_calls_per_minute
        self.cache_dir = cache_dir
        self._calls: List[float] = []

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
            self.logger.info("Covalent throttle: sleeping %.2fs (rate limit)", sleep_s)
            time.sleep(sleep_s)
        self._calls.append(time.time())

    # --- cache helpers ---
    def _cache_path(self, chain_name: str, address: str, bucket: int) -> str:
        return os.path.join(self.cache_dir, chain_name, address.lower(), f"{bucket}.json")

    def _read_cache(self, chain_name: str, address: str, bucket: int) -> Optional[Dict[str, Any]]:
        path = self._cache_path(chain_name, address, bucket)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _write_cache(self, chain_name: str, address: str, bucket: int, data: Dict[str, Any]) -> None:
        path = self._cache_path(chain_name, address, bucket)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)

    def _request(self, path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        params = dict(params or {})
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        backoff_base = 5
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            sleep_s = float(retry_after)
                        except ValueError:
                            sleep_s = backoff_base * (2 ** attempt)
                    else:
                        sleep_s = backoff_base * (2 ** attempt)
                    sleep_s = min(sleep_s, 60) + random.random() * 0.5
                    self.logger.warning(
                        "Covalent 429, sleeping %.1fs (attempt %d/%d)",
                        sleep_s,
                        attempt + 1,
                        self.max_retries,
                    )
                    time.sleep(sleep_s)
                    continue

                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    self.logger.error("Covalent response not dict: %s", data)
                    return None
                if data.get("error"):
                    self.logger.error("Covalent error: %s", data.get("error_message"))
                    return None
                return data
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
        no_logs: bool = True,
        use_cache: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        if not self.api_key:
            return None

        if use_cache:
            cached = self._read_cache(chain_name, address, bucket)
            if cached is not None:
                return cached.get("items") if isinstance(cached, dict) else cached

        path = f"/v1/{chain_name}/bulk/transactions/{address}/{bucket}/"
        params: Dict[str, Any] = {
            "quote-currency": quote_currency,
            "no-logs": str(no_logs).lower(),
        }
        data = self._request(path, params=params)
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
        no_logs: bool = True,
        empty_streak_limit: int = 20,
        max_buckets: int = 600,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        chain_name = CHAIN_NAMES.get(chain)
        if chain_name is None or not self.api_key:
            return [], {}

        start_bucket = start_ts // 900
        end_bucket = end_ts // 900
        items_out: List[Dict[str, Any]] = []
        buckets_fetched = 0
        errors = 0
        cache_hits = 0
        http_429 = 0
        sleep_total = 0.0

        step = 1
        empty_streak = 0
        processed = 0
        last_bucket = start_bucket - 1

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
                    cached = self._read_cache(chain_name, address, current)
                    if cached is not None:
                        bucket_items = cached.get("items") if isinstance(cached, dict) else cached
                        cache_hits += 1
                        break
                    bucket_items = self.get_time_bucket_transactions(
                        chain_name=chain_name,
                        address=address,
                        bucket=current,
                        quote_currency=quote_currency,
                        no_logs=no_logs,
                        use_cache=False,
                    )
                    sleep_total += max(time.time() - before, 0)
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

            for it in bucket_items:
                try:
                    ts_iso = it.get("block_signed_at") or ""
                    if not ts_iso:
                        continue
                    ts_dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                    ts_int = int(ts_dt.replace(tzinfo=timezone.utc).timestamp())
                    if ts_int < start_ts or ts_int > end_ts:
                        continue
                    from_addr = str(it.get("from_address") or "").lower()
                    to_addr = str(it.get("to_address") or "").lower()
                    token_address = str(it.get("contract_address") or "").lower()
                    token_symbol = it.get("contract_ticker_symbol")
                    decimals = int(it.get("contract_decimals") or 0)
                    raw_delta = it.get("delta") or "0"
                    amount = (
                        float(raw_delta) / (10**decimals)
                        if decimals >= 0
                        else float(raw_delta)
                    )
                    usd_val = it.get("fiat_value") or it.get("value_quote") or None
                    usd_val = float(usd_val) if usd_val is not None else None
                    direction_amount = 0.0
                    if address.lower() == to_addr:
                        direction_amount = abs(amount)
                    elif address.lower() == from_addr:
                        direction_amount = -abs(amount)
                    else:
                        continue
                    items_out.append(
                        {
                            "timestamp": ts_int,
                            "tx_hash": it.get("tx_hash") or "",
                            "wallet": address.lower(),
                            "token_address": token_address,
                            "token_symbol": token_symbol,
                            "token_decimals": decimals,
                            "amount": direction_amount,
                            "usd_value": usd_val,
                            "from_address": from_addr,
                            "to_address": to_addr,
                        }
                    )
                except Exception:
                    continue

            current += step

        items_out.sort(key=lambda x: x.get("timestamp", 0))
        diag = {
            "chain_name": chain_name,
            "start_bucket": start_bucket,
            "end_bucket": end_bucket,
            "last_bucket_processed": last_bucket,
            "buckets_fetched": buckets_fetched,
            "cache_hits": cache_hits,
            "http_429_count": http_429,
            "sleep_seconds_total": sleep_total,
            "tx_items_count": len(items_out),
            "errors_count": errors,
            "max_buckets": max_buckets,
        }
        return items_out, diag

    def get_token_balances(self, chain: str, address: str) -> List[Dict[str, Any]]:
        chain_id = CHAIN_IDS.get(chain)
        if chain_id is None or not self.api_key:
            return []
        path = f"/v1/{chain_id}/address/{address}/balances_v2/"
        data = self._request(path, params={})
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

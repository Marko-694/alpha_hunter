import logging
from typing import Any, Dict, Iterable, List, Optional

import requests

from .logger import get_logger

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class BscRpcClient:
    """
    Лёгкий RPC-клиент для BSC (без web3), с кэшем блоков/таймстампов, чанками и подсчётом запросов.
    """

    MAX_BLOCK_SPAN = 4000

    def __init__(
        self,
        url: str,
        logger: Optional[logging.Logger] = None,
        max_block_span: Optional[int] = None,
        timeout: int = 10,
    ) -> None:
        self.logger = logger or get_logger("bsc_rpc")
        self.urls: List[str] = [url] if url else ["https://bsc-dataseed.binance.org"]
        self._url_index = 0
        self.base_url = self.urls[self._url_index]
        self.timeout = timeout
        self._block_cache: Dict[int, Dict[str, Any]] = {}
        self._ts_cache: Dict[int, int] = {}
        self.max_block_span = max_block_span or self.MAX_BLOCK_SPAN

        # статистика
        self.stats_total_requests: int = 0
        self.stats_last_run_requests: int = 0

        self.logger.info(
            "BSC RPC client init: %s (urls=%d, max_block_span=%d, timeout=%s)",
            self.base_url,
            len(self.urls),
            self.max_block_span,
            self.timeout,
        )

    def _switch_url(self) -> None:
        if len(self.urls) <= 1:
            return
        self._url_index = (self._url_index + 1) % len(self.urls)
        self.base_url = self.urls[self._url_index]
        self.logger.warning("Switching BSC RPC endpoint to %s", self.base_url)

    def _post(self, payload: Dict[str, Any]) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        attempts = len(self.urls)
        for _ in range(attempts):
            url = self.base_url
            try:
                resp = requests.post(url, json=payload, timeout=self.timeout)
            except Exception as exc:
                self.logger.error("RPC %s failed: %s, trying next", url, exc)
                self._switch_url()
                continue

            if resp.status_code != 200:
                self.logger.error("RPC %s HTTP %s, body=%s", url, resp.status_code, resp.text[:200])
                self._switch_url()
                continue

            try:
                data = resp.json()
            except Exception as exc:
                self.logger.error("RPC %s invalid JSON: %s", url, exc)
                self._switch_url()
                continue

            # увеличиваем счётчики при успешном получении JSON
            self.stats_total_requests += 1
            self.stats_last_run_requests += 1

            err = data.get("error")
            if err and err.get("code") != -32005:
                self.logger.error("RPC %s error: %s", url, err)
                self._switch_url()
                continue

            return data, err

        return None, {"code": "all_failed", "message": "All RPC endpoints failed"}

    def _rpc_call(self, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        data, err = self._post(payload)
        if err:
            return None
        return data.get("result") if data else None

    def get_block(self, number: int) -> Optional[Dict[str, Any]]:
        if number in self._block_cache:
            return self._block_cache[number]
        hex_num = hex(number)
        result = self._rpc_call("eth_getBlockByNumber", [hex_num, False])
        if not isinstance(result, dict):
            return None
        try:
            ts_hex = result.get("timestamp", "0x0")
            ts_int = int(ts_hex, 16)
            result["timestamp_int"] = ts_int
            self._block_cache[number] = result
            self._ts_cache[number] = ts_int
            return result
        except Exception as exc:
            self.logger.error("Failed to parse block %s: %s", number, exc)
            return None

    def get_latest_block_number(self) -> Optional[int]:
        result = self._rpc_call("eth_blockNumber", [])
        try:
            return int(result, 16)
        except Exception:
            return None

    def find_block_by_timestamp(self, target_ts: int, search_back_hours: int = 24) -> Optional[int]:
        latest = self.get_latest_block_number()
        if latest is None:
            return None
        latest_block = self.get_block(latest)
        if not latest_block:
            return None
        latest_ts = latest_block.get("timestamp_int", 0)

        blocks_delta = max(1, int((latest_ts - target_ts) / 3))  # ~3s per block
        lo = max(1, latest - blocks_delta * 2)
        hi = latest

        closest = None
        while lo <= hi:
            mid = (lo + hi) // 2
            blk = self.get_block(mid)
            if not blk:
                break
            mid_ts = blk.get("timestamp_int", 0)
            closest = mid if closest is None or abs(mid_ts - target_ts) < abs(
                self.get_block(closest).get("timestamp_int", 0) - target_ts
            ) else closest

            if mid_ts > target_ts:
                hi = mid - 1
            else:
                lo = mid + 1
        return closest

    def _eth_get_logs(
        self,
        from_block: int,
        to_block: int,
        contract: str,
    ) -> tuple[Optional[List[Dict[str, Any]]], Optional[Dict[str, Any]]]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "address": contract,
                    "topics": [TRANSFER_TOPIC],
                }
            ],
        }
        data, err = self._post(payload)
        if err:
            return None, err
        result = data.get("result") if data else None
        if not isinstance(result, list):
            return None, {"message": f"unexpected result type {type(result)}"}
        return result, None

    def _parse_logs(self, raw_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logs = []
        for log in raw_logs:
            try:
                block_number = int(log.get("blockNumber"), 16)
                tx_hash = log.get("transactionHash")
                topics = log.get("topics") or []
                data = log.get("data") or "0x0"
                from_addr = topics[1][-40:] if len(topics) > 1 else ""
                to_addr = topics[2][-40:] if len(topics) > 2 else ""
                value_raw = int(data, 16) if data else 0
                ts = self._ts_cache.get(block_number)
                if ts is None:
                    blk = self.get_block(block_number)
                    ts = blk.get("timestamp_int", 0) if blk else 0
                logs.append(
                    {
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "from": f"0x{from_addr.lower()}" if from_addr else "",
                        "to": f"0x{to_addr.lower()}" if to_addr else "",
                        "value_raw": value_raw,
                        "timestamp": ts,
                    }
                )
            except Exception as exc:
                self.logger.debug("Failed to parse log: %s", exc)
                continue
        return logs

    def _get_logs_chunked(self, contract: str, from_block: int, to_block: int, depth: int = 0) -> List[Dict[str, Any]]:
        if from_block > to_block:
            return []

        span = to_block - from_block
        if span > self.max_block_span:
            mid = (from_block + to_block) // 2
            left = self._get_logs_chunked(contract, from_block, mid, depth + 1)
            right = self._get_logs_chunked(contract, mid + 1, to_block, depth + 1)
            return left + right

        self.logger.debug("eth_getLogs chunk depth=%d range=%s-%s", depth, from_block, to_block)
        raw_logs, error = self._eth_get_logs(from_block, to_block, contract)
        if error:
            code = error.get("code")
            if code == -32005 or depth < 5:
                self.logger.info("Splitting range %s-%s due to error %s", from_block, to_block, error)
                if from_block == to_block:
                    return []
                mid = (from_block + to_block) // 2
                left = self._get_logs_chunked(contract, from_block, mid, depth + 1)
                right = self._get_logs_chunked(contract, mid + 1, to_block, depth + 1)
                return left + right
            self.logger.error("RPC error eth_getLogs: %s", error)
            return []

        return self._parse_logs(raw_logs)

    def get_erc20_transfers(self, contract: str, from_block: int, to_block: int) -> Iterable[Dict[str, Any]]:
        """
        Возвращает логи Transfer для контракта за указанный диапазон блоков.
        Диапазон автоматически разбивается на чанки, чтобы избежать limit exceeded.
        """
        self.stats_last_run_requests = 0
        logs = self._get_logs_chunked(contract, from_block, to_block)
        self.logger.info("Fetched %d logs for %s blocks %s-%s", len(logs), contract, from_block, to_block)
        return logs

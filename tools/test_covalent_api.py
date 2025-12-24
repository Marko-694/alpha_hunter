import argparse
import json
import os
import sys
import time
from typing import Optional

import yaml

from alpha_hunter.covalent_client import CovalentClient
from alpha_hunter.pnl_tracer import _compute_direction


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def build_client(config: dict) -> CovalentClient:
    cov_cfg = config.get("covalent", {}) or {}
    if not cov_cfg.get("api_key"):
        print("Covalent api_key missing in config.yaml", file=sys.stderr)
        sys.exit(1)
    return CovalentClient(
        base_url=cov_cfg.get("base_url", "https://api.covalenthq.com"),
        api_key=cov_cfg.get("api_key", ""),
        logger=None,
        max_calls_per_minute=int(cov_cfg.get("max_calls_per_minute") or 0) or None,
    )


def run_probe(
    client: CovalentClient,
    args: argparse.Namespace,
    *,
    force_refresh: bool,
    cache_ttl_seconds: Optional[int],
) -> tuple[list, dict]:
    print(
        f"Fetching chain={args.chain} address={args.address} "
        f"window={args.start_ts}->{args.end_ts} force_refresh={force_refresh}"
    )
    txs, diag = client.iter_transactions_window(
        chain=args.chain,
        address=args.address,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        force_refresh=force_refresh,
        cache_ttl_seconds=cache_ttl_seconds,
    )
    diag = diag or {}
    print(
        "Stats: api_calls={api_calls} cache_hits={cache_hits} cache_misses={cache_misses} "
        "http_429={http_429_count} buckets_fetched={buckets_fetched}/{buckets_total} "
        "sleep={sleep_seconds_total:.2f}s failed_buckets={failed}".format(
            api_calls=diag.get("api_calls", 0),
            cache_hits=diag.get("cache_hits", 0),
            cache_misses=diag.get("cache_misses", 0),
            http_429_count=diag.get("http_429_count", 0),
            buckets_fetched=diag.get("buckets_fetched", 0),
            buckets_total=diag.get("buckets_total", 0),
            sleep_seconds_total=diag.get("sleep_seconds_total", 0.0),
            failed=diag.get("failed_buckets") or [],
        )
    )
    if txs:
        first_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(txs[0]["timestamp"]))
        last_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(txs[-1]["timestamp"]))
        tokens = sorted(
            {
                (tx.get("token_symbol") or "").upper()
                for tx in txs
                if tx.get("token_symbol")
            }
        )
        print(f"Tx count={len(txs)} first_ts={first_ts} last_ts={last_ts} tokens={', '.join(tokens[:10])}")
        invested = 0.0
        cashed = 0.0
        addr_l = args.address.lower()
        for tx in txs:
            usd_val = tx.get("usd_value")
            if usd_val is None:
                continue
            try:
                usd_abs = abs(float(usd_val) or 0.0)
            except Exception:
                continue
            from_addr = str(tx.get("from") or tx.get("from_address") or "").lower()
            to_addr = str(tx.get("to") or tx.get("to_address") or "").lower()
            direction = tx.get("direction")
            if direction not in {"in", "out"}:
                direction = _compute_direction(addr_l, from_addr, to_addr)
            if direction == "in":
                cashed += usd_abs
            elif direction == "out":
                invested += usd_abs
        pnl = cashed - invested
        print(f"Flows: invested≈${invested:,.2f} cashed≈${cashed:,.2f} realized≈${pnl:,.2f}")
    else:
        print("No transactions found in the window.")
    return txs, diag


def main() -> None:
    parser = argparse.ArgumentParser(description="Covalent API smoke test (bucket cache awareness)")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--chain", required=True, help="Chain name (bsc, eth, ...)")
    parser.add_argument("--address", required=True, help="Address to inspect")
    parser.add_argument("--start-ts", type=int, required=True, help="Window start (unix ts)")
    parser.add_argument("--end-ts", type=int, required=True, help="Window end (unix ts)")
    parser.add_argument("--force-refresh", action="store_true", help="Force bypass bucket cache")
    parser.add_argument("--save-sample", action="store_true", help="Save tx sample to tmp/covalent_sample.json")
    parser.add_argument(
        "--cache-ttl-seconds",
        type=int,
        default=600,
        help="Override cache TTL seconds (default 600)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    client = build_client(config)

    txs, _ = run_probe(
        client,
        args,
        force_refresh=args.force_refresh,
        cache_ttl_seconds=args.cache_ttl_seconds,
    )
    run_probe(
        client,
        args,
        force_refresh=False,
        cache_ttl_seconds=args.cache_ttl_seconds,
    )

    if args.save_sample and txs:
        os.makedirs("tmp", exist_ok=True)
        sample_path = os.path.join("tmp", "covalent_sample.json")
        with open(sample_path, "w", encoding="utf-8") as fh:
            json.dump(txs[:50], fh, ensure_ascii=False, indent=2)
        print(f"Sample saved to {sample_path}")


if __name__ == "__main__":
    main()

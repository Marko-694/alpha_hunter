import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from alpha_hunter.actor_activity_monitor import ActorActivityMonitor
from alpha_hunter.binance_client import BinanceClient
from alpha_hunter.binance_futures_client import BinanceFuturesClient
from alpha_hunter.birdeye_client import BirdEyeClient
from alpha_hunter.covalent_client import CovalentClient
from alpha_hunter.explorer_client import ExplorerClient
from alpha_hunter.features_consumer import evaluate_signals, load_features
from alpha_hunter.hunter_state import DEFAULT_STATE, load_state, save_state, remember_event, seen_event
from alpha_hunter.logger import get_logger
from alpha_hunter.nansen_client import NansenClient
from alpha_hunter.nansen_monitor import NansenSmartMoneyMonitor
from alpha_hunter.profile_artifacts import ArtifactsCache, build_initiator_index
from alpha_hunter.scanner import AlphaScanner
from alpha_hunter.stablecoin_inflow_monitor import StablecoinInflowMonitor
from alpha_hunter.telegram_alerts import TelegramNotifier
from alpha_hunter.config_utils import resolve_nansen_config


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def run_hunter() -> None:
    logger = get_logger("alpha_hunter")
    config = load_config("config.yaml")
    nansen_cfg, _ = resolve_nansen_config(config, logger)
    state = load_state(logger=logger)

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
    tele_alerts_cfg = config.get("alerts", {}) or {}
    dry_run_env = os.getenv("HUNTER_DRY_RUN") == "1"
    smoke_mode = os.getenv("HUNTER_SMOKE") == "1"
    dry_run = dry_run_env or bool(telegram_cfg.get("dry_run")) or smoke_mode

    if smoke_mode:
        config["alpha_tokens"] = [
            {
                "symbol": "ARTX",
                "chain": "bsc",
                "contract": "0x8105743e8a19c915a604d7d9e7aa3a060a4c2c32",
            },
            {
                "symbol": "TIMI",
                "chain": "bsc",
                "contract": "0xaafe1f781bc5e4d240c4b73f6748d76079678fa8",
            },
            {
                "symbol": "IRYS",
                "chain": "bsc",
                "contract": "0x91152b4ef635403efbae860edd0f8c321d7c035d",
            },
        ]
        state = {**DEFAULT_STATE}
        _prime_smoke_state(state)

    binance_client = FakeBinanceClient(logger=logger) if smoke_mode else BinanceClient(logger=logger)
    explorer_client = None if smoke_mode else ExplorerClient(config=config, logger=logger)
    notifier = TelegramNotifier(
        bot_token=bot_token,
        chat_ids=chat_ids,
        dry_run=dry_run,
        state=state,
        alerts_cfg=tele_alerts_cfg,
        logger=logger,
    )

    nansen_client = None
    nansen_monitor = None
    if smoke_mode:
        nansen_client = FakeNansenClient()
    else:
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
                nansen_cfg=nansen_cfg,
                logger=logger,
            )

    try:
        notifier.send_message("Alpha Hunter запущен и готов к сканированию.")
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to send startup test message: %s", exc)

    futures_client = FakeBinanceFuturesClient() if smoke_mode else BinanceFuturesClient(logger=logger)
    scanner = AlphaScanner(
        binance_client,
        notifier,
        config=config,
        logger=logger,
        futures_client=futures_client,
        nansen_client=nansen_client,
        state=state,
        nansen_cfg=nansen_cfg,
    )
    stablecoin_inflow_monitor = None
    if not smoke_mode:
        stablecoin_inflow_monitor = StablecoinInflowMonitor(
            notifier=notifier,
            config=config,
            logger=logger,
            explorer_client=explorer_client,
        )

    # optional features signals (from warehouse/features.json)
    features = load_features()
    feat_cfg = config.get("features", {}) if isinstance(config, dict) else {}
    signals = evaluate_signals(features, feat_cfg.get("thresholds", {}))
    for sig in signals[:5]:
        logger.info(sig)
        try:
            notifier.send_message(sig)
        except Exception:
            pass

    if smoke_mode:
        artifacts_cache = None
        snapshot = _build_smoke_artifacts()
    else:
        artifacts_cache = ArtifactsCache(logger=logger)
        snapshot = dict(artifacts_cache.snapshot)
    initiator_cfg = config.get("initiator_tracking", {}) or {}
    initiator_data = snapshot.get("initiators") or {}
    if not initiator_data.get("initiator_addrs") and not smoke_mode:
        initiator_data = build_initiator_index(
            snapshot.get("actors_raw") or [],
            snapshot.get("clusters_raw") or [],
            snapshot.get("features") or {},
            settings=initiator_cfg,
        )
        snapshot["initiators"] = initiator_data

    birdeye_client = None
    if smoke_mode:
        birdeye_client = FakeBirdEyeClient()
    elif initiator_cfg.get("enabled"):
        try:
            birdeye_client = BirdEyeClient(config=config, logger=logger)
        except Exception as exc:
            logger.warning("Failed to init BirdEye for initiator tracking: %s", exc)
            initiator_cfg["enabled"] = False

    enrichment_cfg = config.get("hunter_enrichment", {}) or {}
    actor_boost_cfg = enrichment_cfg.get("actor_boost", {}) or {}
    dedupe_cfg = enrichment_cfg.get("dedupe", {}) or {}
    fusion_cfg = config.get("fusion", {}) or {}

    scanner.actor_features = snapshot.get("features") or {}
    scanner.watchlist_addrs = set(snapshot.get("watchlist_addrs") or [])
    scanner.state = state
    scanner.actor_boost_cfg = actor_boost_cfg
    scanner.dedupe_cfg = dedupe_cfg
    scanner.initiator_data = initiator_data
    scanner.initiator_cfg = initiator_cfg
    scanner.birdeye_client = birdeye_client
    scanner.update_artifacts(snapshot)
    if smoke_mode:
        scanner.update_hot_tokens(snapshot.get("hot_tokens_seed") or [], time.time())

    actor_activity_cfg = config.get("actor_activity", {}) or {}
    alerts_cfg = config.get("alerts", {}) or {}
    covalent_cfg = config.get("covalent", {}) or {}
    actor_monitor: ActorActivityMonitor | None = None
    covalent_client = None
    if actor_activity_cfg.get("enabled"):
        if smoke_mode:
            covalent_client = FakeCovalentClient()
        elif covalent_cfg.get("enabled") and covalent_cfg.get("api_key"):
            covalent_client = CovalentClient(
                base_url=covalent_cfg.get("base_url", "https://api.covalenthq.com"),
                api_key=covalent_cfg.get("api_key"),
                logger=logger,
                max_calls_per_minute=covalent_cfg.get("max_calls_per_minute"),
                cache_dir="data/raw_cache/covalent",
            )
        else:
            logger.warning("Actor activity monitor enabled but Covalent configuration missing; disabling monitor")
        if covalent_client:
            actor_monitor = ActorActivityMonitor(
                covalent_client=covalent_client,
                notifier=notifier,
                config=config,
                state=state,
                logger=logger,
            )
            if smoke_mode:
                actor_monitor._internal_state["last_run_ts"] = 0
            actor_monitor.update_sources(
                watchlist_records=snapshot.get("watchlist_records"),
                initiator_snapshot=snapshot.get("initiators"),
                exit_signatures=snapshot.get("exit_signatures"),
            )
            actor_monitor.update_trust_map(snapshot.get("trust_map"))

    interval_seconds = scanner.interval_seconds

    logger.info("Alpha Hunter started with interval %ss", interval_seconds)
    try:
        while True:
            logger.info("Scan cycle start")
            if not smoke_mode and artifacts_cache and artifacts_cache.refresh_if_changed():
                snapshot = dict(artifacts_cache.snapshot)
                if not snapshot.get("initiators", {}).get("initiator_addrs"):
                    snapshot["initiators"] = build_initiator_index(
                        snapshot.get("actors_raw") or [],
                        snapshot.get("clusters_raw") or [],
                        snapshot.get("features") or {},
                        settings=initiator_cfg,
                    )
                scanner.update_artifacts(snapshot)
                scanner.initiator_data = snapshot.get("initiators") or scanner.initiator_data
                if actor_monitor:
                    actor_monitor.update_sources(
                        watchlist_records=snapshot.get("watchlist_records"),
                        initiator_snapshot=snapshot.get("initiators"),
                        exit_signatures=snapshot.get("exit_signatures"),
                    )
                    actor_monitor.update_trust_map(snapshot.get("trust_map"))

            scanner.scan_once()
            monitor_result = actor_monitor.run_once() if actor_monitor else None
            monitor_reason = (monitor_result or {}).get("reason")
            monitor_stats = (monitor_result or {}).get("stats") or {}
            setup_alerts_sent = 0
            activity_alerts_sent = monitor_stats.get("alerts_sent", 0)
            dump_alerts_sent = monitor_stats.get("dump_alerts", 0)
            covalent_stats: Dict[str, Any] = {
                "api_calls": monitor_stats.get("api_calls", 0),
                "cache_hits": monitor_stats.get("cache_hits", 0),
            }
            if monitor_result and monitor_result.get("ran"):
                hot_entries = monitor_result.get("hot_tokens") or []
                if hot_entries:
                    scanner.update_hot_tokens(hot_entries, time.time())
                if fusion_cfg.get("enabled") and fusion_cfg.get("emit_setup_alerts"):
                    setup_alerts_sent = _maybe_emit_setup_alerts(
                        notifier=notifier,
                        state=state,
                        fusion_cfg=fusion_cfg,
                        actor_cfg=actor_activity_cfg,
                        alerts_cfg=alerts_cfg,
                        hot_tokens=hot_entries,
                        new_token_counts=monitor_result.get("new_tokens_per_actor") or {},
                        logger=logger,
                    )
                logger.info(
                    (
                        "Actor monitor stats: processed=%s skipped=%s errors=%s alerts=%s "
                        "dumps=%s hot_tokens=%s new_tokens=%s setup_alerts=%s"
                    ),
                    monitor_stats.get("processed_addrs", 0),
                    monitor_stats.get("skipped", 0),
                    monitor_stats.get("errors", 0),
                    monitor_stats.get("alerts_sent", 0),
                    dump_alerts_sent,
                    monitor_stats.get("hot_tokens", 0),
                    monitor_stats.get("new_tokens", 0),
                    setup_alerts_sent,
                )
            elif monitor_result and monitor_reason:
                stats = monitor_result.get("stats") or {}
                logger.info(
                    "ActorActivityMonitor skipped: reason=%s candidates=%s cursor=%s",
                    monitor_reason,
                    stats.get("candidates_total"),
                    stats.get("cursor_before"),
                )
            if stablecoin_inflow_monitor:
                stablecoin_inflow_monitor.run_once()
            alpha_tokens = config.get("alpha_tokens", [])
            if nansen_monitor is not None and alpha_tokens:
                nansen_monitor.run_once(alpha_tokens)
            logger.info("Scan cycle end")
            telemetry_payload = {
                "ts": time.time(),
                "market_checked": scanner.last_cycle_stats.get("total_tickers", 0),
                "market_candidates": scanner.last_cycle_stats.get("candidates", 0),
                "messages_ping": scanner.last_cycle_stats.get("pings_sent", 0),
                "messages_alarm": scanner.last_cycle_stats.get("alarms_sent", 0),
                "setup_sent": setup_alerts_sent,
                "activity_sent": activity_alerts_sent,
                "dump_sent": dump_alerts_sent,
                "hot_tokens_count": len(scanner.hot_tokens),
                "covalent_stats": covalent_stats,
                "birdeye_stats": getattr(scanner.birdeye_client, "cache_stats", {}),
                "dry_run": dry_run,
                "smoke_mode": smoke_mode,
                "actor_activity_processed": monitor_stats.get("processed", 0),
                "actor_activity_errors": monitor_stats.get("errors", 0),
                "actor_activity_cursor": monitor_stats.get("cursor_after", monitor_stats.get("cursor_before", 0)),
                "actor_activity_candidates_total": monitor_stats.get("candidates_total", 0),
                "actor_activity_reason": monitor_reason,
            }
            _append_runtime_telemetry(
                Path("data/alpha_profiler/warehouse/hunter_runtime.jsonl"),
                telemetry_payload,
            )
            save_state(state)
            if os.environ.get("HUNTER_ONCE") == "1":
                logger.info("HUNTER_ONCE set, exiting after one cycle")
                break
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Stopped by user")


def _maybe_emit_setup_alerts(
    *,
    notifier: TelegramNotifier,
    state: Dict[str, Any],
    fusion_cfg: Dict[str, Any],
    actor_cfg: Dict[str, Any],
    alerts_cfg: Dict[str, Any],
    hot_tokens: List[Dict[str, Any]],
    new_token_counts: Dict[str, int],
    logger,
) -> int:
    if not hot_tokens:
        return 0
    ping_prefix = str(alerts_cfg.get("ping_prefix", "👀"))
    min_score = float(fusion_cfg.get("setup_alert_min_initiator_score", 0.7))
    min_new_tokens = int(fusion_cfg.get("setup_alert_min_new_tokens", 1))
    cooldown = int(fusion_cfg.get("setup_alert_cooldown_seconds", 1800))
    bucket_seconds = 1800
    lookback = int(actor_cfg.get("lookback_minutes", actor_cfg.get("window_minutes", 15)) or 15)
    setup_state = state.setdefault("last_setup_alert_ts", {})
    sent = 0
    now_ts = time.time()
    for entry in hot_tokens:
        if not entry.get("new_token"):
            continue
        score = float(entry.get("initiator_score") or 0.0)
        if score < min_score:
            continue
        addr = entry.get("addr")
        token_key = entry.get("token_key")
        if not addr or not token_key:
            continue
        if new_token_counts.get(addr, 0) < min_new_tokens:
            continue
        addr_key = addr.lower()
        last_ts = setup_state.get(f"{addr_key}:{token_key}")
        if last_ts and now_ts - last_ts < cooldown:
            continue
        event_bucket = int(now_ts) // bucket_seconds
        event_key = f"setup:{addr_key}:{token_key}:{event_bucket}"
        if seen_event(state, event_key):
            continue
        symbol = entry.get("symbol") or "?"
        top_symbols = entry.get("top_symbols") or [symbol]
        top_line = ", ".join(top_symbols)
        campaigns = entry.get("campaigns_count")
        campaigns_text = str(int(campaigns)) if isinstance(campaigns, (int, float)) else "n/a"
        volume = entry.get("usd_volume") or 0.0
        tx_count = entry.get("tx_count") or 0
        message = (
            f"{ping_prefix} Initiator setup (pre-pump)\n"
            f"Address: {_short_addr(addr)} | init_score={score:.2f} | campaigns={campaigns_text}\n"
            f"New token: {symbol} ({token_key}) | Tx: {tx_count} | Volume≈${volume:,.0f}\n"
            f"Top tokens ({lookback}m): {top_line}\n"
            f"Статус: SETUP\n"
            f"Режим: PING"
        )
        try:
            notifier.send_message(message)
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to send setup alert for %s: %s", addr, exc)
            continue
        setup_state[f"{addr_key}:{token_key}"] = now_ts
        remember_event(state, event_key, int(now_ts))
        sent += 1
    return sent


def _short_addr(addr: str) -> str:
    if len(addr) <= 10:
        return addr
    return f"{addr[:6]}..{addr[-4:]}"


def _append_runtime_telemetry(path: Path, payload: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=False)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as exc:  # pragma: no cover
        get_logger("telemetry").debug("Telemetry write failed: %s", exc)


def _prime_smoke_state(state: Dict[str, Any]) -> None:
    state.setdefault("last_symbol_alert_ts", {})
    state.setdefault("last_symbol_ping_ts", {})
    state.setdefault("last_symbol_alarm_ts", {})
    actor_tokens = state.setdefault("actor_recent_tokens", {})
    demo_entries = {
        "0xf3e70b7030abcdeffeedcafe0000000000000001": "bsc:0x8105743e8a19c915a604d7d9e7aa3a060a4c2c32",
        "0xf3e70b7030abcdeffeedcafe0000000000000002": "bsc:0xaafe1f781bc5e4d240c4b73f6748d76079678fa8",
    }
    now_ts = int(time.time()) - 1800
    for addr, token_key in demo_entries.items():
        bucket = actor_tokens.setdefault(addr, {})
        bucket[token_key] = now_ts
    stage_map = state.setdefault("actor_token_stage", {})
    for addr, token_key in demo_entries.items():
        stage_map.setdefault(addr, {})[token_key] = {
            "stage": "PUMP",
            "ts": now_ts,
            "last": {"sell_ratio": 0.15, "exit_wave_score": 0.35, "updated_at": now_ts},
        }


def _build_smoke_artifacts() -> Dict[str, Any]:
    watchlist_records = [
        {
            "address": "0xf3e70b7030abcdeffeedcafe0000000000000001",
            "chain": "bsc",
            "actor_score": 32.5,
            "label": "core watch",
        },
        {
            "address": "0xf3e70b7030abcdeffeedcafe0000000000000002",
            "chain": "bsc",
            "actor_score": 21.0,
            "label": "support",
        },
    ]
    watchlist_addrs = {entry["address"].lower() for entry in watchlist_records}
    features = {
        "actors": [
            {
                "address": entry["address"],
                "actor_score": entry["actor_score"],
                "cov_realized_pnl_usd": 4200 if idx == 0 else 1600,
            }
            for idx, entry in enumerate(watchlist_records)
        ]
    }
    initiator_entries = [
        {
            "address": watchlist_records[0]["address"],
            "initiator_score": 0.92,
            "campaigns_count": 4,
            "avg_pre_buy_usd": 3200,
            "median_exit_speed_seconds": 1800,
            "tokens": ["ARTX", "TIMI", "IRYS"],
            "cluster_id": "cluster:smoke-core",
        },
        {
            "address": watchlist_records[1]["address"],
            "initiator_score": 0.71,
            "campaigns_count": 2,
            "avg_pre_buy_usd": 2200,
            "median_exit_speed_seconds": 2400,
            "tokens": ["TIMI"],
            "cluster_id": "cluster:smoke-wing",
        },
    ]
    initiators = {
        "initiator_addrs": {entry["address"].lower() for entry in initiator_entries},
        "addr_score": {entry["address"].lower(): entry["initiator_score"] for entry in initiator_entries},
        "addr_campaigns": {entry["address"].lower(): entry["campaigns_count"] for entry in initiator_entries},
        "addr_tokens": {entry["address"].lower(): entry["tokens"] for entry in initiator_entries},
        "addr_cluster_id": {
            entry["address"].lower(): entry.get("cluster_id") for entry in initiator_entries
        },
        "addr_details": {entry["address"].lower(): entry for entry in initiator_entries},
    }
    actors_raw = [
        {
            "address": watchlist_records[0]["address"],
            "pumps_count": 4,
            "best_pamper_level": "core_operator",
            "actor_score": 30,
            "pumps": [{"symbol": "ARTX"}, {"symbol": "TIMI"}],
        },
        {
            "address": watchlist_records[1]["address"],
            "pumps_count": 2,
            "best_pamper_level": "outer_circle",
            "actor_score": 22,
            "pumps": [{"symbol": "TIMI"}],
        },
    ]
    clusters_raw = [
        {"address": watchlist_records[0]["address"], "pumps_participated": 4},
        {"address": watchlist_records[1]["address"], "pumps_participated": 2},
    ]
    hot_seed = [
        {
            "token_key": "bsc:0xaafe1f781bc5e4d240c4b73f6748d76079678fa8",
            "weight": 0.8,
            "addr": watchlist_records[0]["address"],
            "initiator_score": 0.92,
            "new_token": True,
        },
        {
            "token_key": "bsc:0x91152b4ef635403efbae860edd0f8c321d7c035d",
            "weight": 0.6,
            "addr": watchlist_records[1]["address"],
            "initiator_score": 0.71,
            "new_token": False,
        },
    ]
    exit_signatures = {
        watchlist_records[0]["address"].lower(): {
            "address": watchlist_records[0]["address"],
            "chain": "bsc",
            "campaigns": 4,
            "exit_speed_p50_seconds": 1800,
            "dump_ratio_p50": 1.12,
        },
        watchlist_records[1]["address"].lower(): {
            "address": watchlist_records[1]["address"],
            "chain": "bsc",
            "campaigns": 2,
            "exit_speed_p50_seconds": 2400,
            "dump_ratio_p50": 0.95,
        },
    }
    return {
        "features": features,
        "features_watchlist": features,
        "features_actors": features,
        "watchlist_records": watchlist_records,
        "watchlist_addrs": watchlist_addrs,
        "actors_raw": actors_raw,
        "clusters_raw": clusters_raw,
        "cache_health": {"mode": "smoke"},
        "initiators": initiators,
        "hot_tokens_seed": hot_seed,
        "exit_signatures": exit_signatures,
        "trust_map": {
            entry["address"].lower(): {
                "trust": 0.8 if idx == 0 else 0.6,
                "last_summary": {
                    "realized_pnl_usd": 4200 if idx == 0 else 1600,
                    "buy_pressure_usd": 3000 if idx == 0 else 1200,
                    "trust_reason": "ok",
                },
            }
            for idx, entry in enumerate(watchlist_records)
        },
    }


class FakeBinanceClient:
    def __init__(self, logger=None) -> None:
        self.logger = logger or get_logger("FakeBinanceClient")
        self._tickers = [
            {"symbol": "ARTXUSDT", "lastPrice": "0.125", "quoteVolume": "21000000"},
            {"symbol": "TIMIUSDT", "lastPrice": "0.042", "quoteVolume": "7500000"},
            {"symbol": "IRYSUSDT", "lastPrice": "0.028", "quoteVolume": "5200000"},
        ]
        self._klines = {
            "ARTXUSDT": self._build_klines(last_close=0.125, prev_close=0.098, avg_volume=8000, spike=58000),
            "TIMIUSDT": self._build_klines(last_close=0.042, prev_close=0.0418, avg_volume=9000, spike=18000),
            "IRYSUSDT": self._build_klines(
                last_close=0.028,
                prev_close=0.034,
                avg_volume=6000,
                spike=52000,
                falling=True,
            ),
        }

    @staticmethod
    def _build_klines(
        last_close: float,
        prev_close: float,
        avg_volume: float,
        spike: float,
        falling: bool = False,
    ) -> List[List[Any]]:
        klines: List[List[Any]] = []
        for _ in range(8):
            klines.append(
                [0, f"{prev_close:.6f}", "0", "0", f"{prev_close:.6f}", "0", "0", f"{avg_volume:.0f}", "0", "0", "0", "0"]
            )
        if falling:
            open1 = prev_close * 1.01
            close1 = prev_close * 0.99
            klines.append(
                [0, f"{open1:.6f}", "0", "0", f"{close1:.6f}", "0", "0", f"{avg_volume * 1.2:.0f}", "0", "0", "0", "0"]
            )
            open2 = close1 * 1.005
            close2 = close1 * 0.97
            klines.append(
                [0, f"{open2:.6f}", "0", "0", f"{close2:.6f}", "0", "0", f"{avg_volume * 1.4:.0f}", "0", "0", "0", "0"]
            )
        else:
            open1 = prev_close * 0.99
            close1 = prev_close * 1.015
            klines.append(
                [0, f"{open1:.6f}", "0", "0", f"{close1:.6f}", "0", "0", f"{avg_volume * 1.1:.0f}", "0", "0", "0", "0"]
            )
            open2 = close1 * 0.995
            close2 = last_close * 0.98
            klines.append(
                [0, f"{open2:.6f}", "0", "0", f"{close2:.6f}", "0", "0", f"{avg_volume * 1.2:.0f}", "0", "0", "0", "0"]
            )
        klines.append(
            [0, f"{last_close:.6f}", "0", "0", f"{last_close:.6f}", "0", "0", f"{spike:.0f}", "0", "0", "0", "0"]
        )
        return klines

    def get_24h_tickers(self) -> List[Dict[str, Any]]:
        self.logger.info("[SMOKE] returning %d fake tickers", len(self._tickers))
        return self._tickers

    def get_recent_klines(self, symbol: str, interval: str = "1m", limit: int = 11) -> List[List[Any]]:
        return self._klines.get(symbol, [])


class FakeBinanceFuturesClient:
    def get_futures_snapshot(self, symbol: str) -> Dict[str, Any]:
        return {
            "open_interest": 1_200_000,
            "funding_rate": 0.0005,
            "mark_price": 1.23,
        }


class FakeBirdEyeClient:
    def __init__(self) -> None:
        self.cache_stats: Dict[str, Any] = {"api_calls": 0, "cache_hits": 0}
        self._responses = {
            "0x8105743e8a19c915a604d7d9e7aa3a060a4c2c32": [
                {"owner": "0xf3e70b7030abcdeffeedcafe0000000000000001"},
                {"owner": "0xf3e70b7030abcdeffeedcafe0000000000000002"},
            ],
            "0xaafe1f781bc5e4d240c4b73f6748d76079678fa8": [
                {"owner": "0xf3e70b7030abcdeffeedcafe0000000000000002"},
            ],
        }

    def fetch_for_window(
        self,
        *,
        token_address: str,
        start_ts: int,
        end_ts: int,
        label: str,
        chain: str,
        force_refresh: bool = False,
        return_meta: bool = False,
    ) -> Any:
        self.cache_stats["api_calls"] += 1
        items = self._responses.get(token_address.lower(), [])
        meta = {
            "status": "success",
            "reason": None,
            "force": bool(force_refresh),
            "items_total": len(items),
        }
        return (items, meta) if return_meta else items


class FakeCovalentClient:
    def __init__(self, logger=None) -> None:
        self.logger = logger or get_logger("FakeCovalentClient")
        addr1 = "0xf3e70b7030abcdeffeedcafe0000000000000001"
        addr2 = "0xf3e70b7030abcdeffeedcafe0000000000000002"
        self._txs: Dict[str, List[Dict[str, Any]]] = {
            addr1: self._build_token_flow(
                contract="0x8105743e8a19c915a604d7d9e7aa3a060a4c2c32",
                symbol="ARTX",
                usd_value=6000,
                count=14,
                wallet=addr1,
                dump=False,
            ),
            addr2: self._build_token_flow(
                contract="0xaafe1f781bc5e4d240c4b73f6748d76079678fa8",
                symbol="TIMI",
                usd_value=4200,
                count=12,
                wallet=addr2,
                dump=True,
            ),
        }

    @staticmethod
    def _build_token_flow(
        contract: str,
        symbol: str,
        usd_value: float,
        count: int,
        wallet: str,
        dump: bool,
    ) -> List[Dict[str, Any]]:
        per_tx = max(1.0, usd_value / max(1, count))
        wallet_norm = wallet.lower()
        entries: List[Dict[str, Any]] = []
        for idx in range(count):
            if dump:
                threshold = max(1, count // 5)
                direction = "sell" if idx >= threshold else "buy"
            else:
                direction = "buy" if idx % 2 == 0 else "sell"
            entries.append(
                {
                    "token_address": contract.lower(),
                    "token_symbol": symbol,
                    "usd_value": per_tx,
                    "direction": direction,
                    "from_address": wallet_norm if direction == "sell" else "0xfeedcafe00000000000000000000000000000000",
                    "to_address": wallet_norm if direction == "buy" else "0xfeedcafe00000000000000000000000000000000",
                }
            )
        return entries

    def iter_transactions_window(
        self,
        *,
        chain: str,
        address: str,
        start_ts: int,
        end_ts: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        addr = address.lower()
        txs = self._txs.get(addr, [])
        diag = {"api_calls": 1, "cache_hits": 1 if txs else 0}
        if not txs:
            self.logger.info("[SMOKE] no fake txs for %s", addr)
        return txs, diag


class FakeNansenClient:
    def smart_money_netflows(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "netflow_usd": 125_000,
            "smart_wallets": 6,
        }

    def smart_money_dex_trades(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "largest_buy_usd": 54_000,
            "trades": 9,
        }

if __name__ == "__main__":
    run_hunter()

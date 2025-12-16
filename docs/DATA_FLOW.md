# Data Flow (Alpha Hunter)

## 1) Online pipeline

```
config.yaml (scanner/signals/telegram/nansen/whales/stablecoin_inflow)
        |
        v
main.py -> AlphaScanner.scan_once (Binance spot klines + futures + Nansen enrich)
        |--> TelegramNotifier (alerts)
        |--> WhaleMonitor (DeBank actions -> alerts)
        |--> StablecoinInflowMonitor (explorer tokentx -> alerts)
        |--> NansenSmartMoneyMonitor (token_screener -> alerts)
```

## 2) Offline pipeline

```
config.yaml (alpha_profiler.pumps, birdeye, covalent, explorers)
        |
        v
alpha_profiler.py menu
 1/2) profiler_core.analyze_single_pump (BirdEye token tx) -> data/alpha_profiler/*_profile.json
 3/4) actors_builder.build_clusters/build_actors_and_watchlist -> clusters.json, actors.json, watchlist.json
 6) pnl_tracer.recompute_watchlist_pnl (Covalent buckets + cache)
       - читает profiles/actors/watchlist
       - использует covalent_client bucket-cache (data/raw_cache/covalent)
       - пишет pnl_watchlist.json, timeline_watchlist.json, holdings_overlap.json, network_watchlist.json
       - warehouse/* (tx_normalized.csv, summary_wallets/tokens)
```

## 3) UI pipeline

```
Files: actors.json, watchlist.json, pnl_watchlist.json,
       timeline_watchlist.json, holdings_overlap.json, network_watchlist.json
        |
        v
watchlist_ui.py (Flask): /api/actors,/api/watchlist,/api/timeline,/api/overlap,/api/network,/api/cache_meta
        |
        v
Browser UI: таблица акторов + вкладки Summary/PnL/Holdings/Linked/Timeline/Overlap/Network
```

## 4) Caching/Artifacts

```
Covalent bucket cache: data/raw_cache/covalent/{chain}/{addr}/bucket_*.json
BirdEye page cache:    data/raw_cache/birdeye/{chain}/{contract}/window_*/page_*.json
Legacy tx cache:       data/alpha_profiler/cache_tx/{chain}_{addr}_{from}_{to}.json
Progress:              data/alpha_profiler/pnl_progress.json
Artifacts:             pnl_watchlist.json, timeline_watchlist.json, holdings_overlap.json, network_watchlist.json, warehouse/*
```

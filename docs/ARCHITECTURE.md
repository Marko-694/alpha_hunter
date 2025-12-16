# Alpha Hunter — Архитектура

## A. Overview
Alpha Hunter — тулкит для ловли пампов:
- **Онлайн-хантер**: Binance spot 1m сканер + фьючерсные данные, Telegram-алерты.
- **Мониторы**: WhaleMonitor (DeBank actions), StablecoinInflow (Etherscan V2 / RPC fallback), Nansen Smart Money (enrichment), BirdEye пока не затронут в онлайне.
- **Оффлайн-профайлер**: построение профилей пампов по BirdEye token tx, агрегация actors/watchlist, PnL по Covalent/GoldRush с кешом.
- **UI**: локальный Flask (`watchlist_ui.py`) для просмотра actors/watchlist/pnl + таймлайнов/overlap/network.

## B. Entry points
- `main.py` — бесконечный цикл онлайн-хантера (scanner + whale_monitor + stablecoin_inflow + nansen_monitor).
- `manager.py` — консольное меню: запуск онлайн-хантера или оффлайн-профайлера.
- `alpha_profiler.py` — CLI для оффлайн режима: пункты 1–7 (профайлер пампов, кластеры, actors/watchlist, UI, PnL [-3d,+2h]).
- `alpha_hunter/watchlist_ui.py` — Flask сервер UI, открывает браузер на 127.0.0.1:8050.
- Тесты/утилиты: `test_keys.py`, `test_bsc_rpc_span.py`, `test_nodereal_usage.py`.

## C. Core subsystems
- **Scanner (alpha_hunter/scanner.py)**: 1m Binance tickers, фильтры по 24h объёму/цене, запрос 1m klines, сигналы по volume_ratio/price_delta; алерты в Telegram, опционально Nansen/Futures обогащение, новый MM-блок из кластеров.
- **Monitors**: `whale_monitor.py` (DeBank actions), `stablecoin_inflow_monitor.py` (Etherscan V2 tokentx), `nansen_monitor.py` (smart-money screener), вызываются в `main.py` циклом.
- **BirdEye профилировщик** (alpha_hunter/birdeye_client.py + profiler_core): собирает swap-данные по контракту, строит *_profile.json.
- **Actors/Watchlist builder** (alpha_hunter/actors_builder.py): агрегирует профили → actors.json + watchlist.json (плюс max_pamper_score).
- **PnL pipeline** (pnl_tracer.py + covalent_client.py): GoldRush/Covalent bucket API, кеш бакетов, PnL [-3d,+2h], артефакты timeline/overlap/network/warehouse, роль/бейджи/кандидаты.
- **UI** (watchlist_ui.py): отдаёт JSON из файлов + вкладки Summary/PnL/Holdings/Linked/Timeline/Overlap/Network.

## D. External integrations
- **Binance**: spot /api/v3/ticker/24hr, klines 1m; futures premiumIndex/openInterest. Без ключей.
- **Telegram**: Bot API sendMessage.
- **BirdEye**: public-api.birdeye.so/defi/v3/token/txs (swap), ключи с ротацией/прокси (birdeye_client), кэш страниц (ещё не считает метрики cache_meta).
- **Covalent/GoldRush**: bulk/transactions/{bucket}, balances_v2. Авторизация Bearer. Кэш бакетов в data/raw_cache/covalent.
- **Explorers**: Etherscan V2 (eth/arb/polygon/optimism/base) и прямой bscscan для BSC (в explorer_client / pnl_tracer legacy). Не трогались в последнем раунде.
- **Nansen**: nansen_client (netflows/dex-trades/token-screener), опциональное обогащение.
- **DeBank**: whale_monitor (actions), сейчас зависит от access key.

Ключи/лимиты: config.yaml секции telegram/binance/telegram/nansen/explorers/bird-eye/covalent; covalent max_calls_per_minute применяется в covalent_client, 429 бэкофф реализован.

## E. Persistence / data artifacts
- `data/alpha_profiler/*_profile.json` — профили пампов (BirdEye). Источник: profiler_core.
- `data/alpha_profiler/clusters.json` — агрегат кошельков по профилям.
- `data/alpha_profiler/actors.json`, `watchlist.json` — акторы и фильтр (actors_builder). watchlist содержит max_pamper_score.
- `data/alpha_profiler/pnl_watchlist.json` — PnL (Covalent) по watchlist, содержит pumps[], current_holdings, diag.
- `data/alpha_profiler/timeline_watchlist.json` — таймлайны net_usd/price/tx_count per pump.
- `data/alpha_profiler/holdings_overlap.json` — матрица actors×tokens с rarity.
- `data/alpha_profiler/network_watchlist.json` — граф связей и edges.
- `data/alpha_profiler/warehouse/*` — нормализованные tx (csv) и summary по токенам/акторам.
- Кэши: `data/raw_cache/covalent/{chain}/{addr}/bucket_*.json`; `data/raw_cache/birdeye/{chain}/{contract}/window_*/page_*.json`; `data/cache_tx/{chain}_{addr}_{from}_{to}.json` (pnl_tracer legacy).
- Прогресс: `data/alpha_profiler/pnl_progress.json` (последний bucket).
- Spent BirdEye keys: `data/birdeye_api_keys_spent.json`; proxies `data/birdeye_proxies.txt`.

Правила обновления: финальные JSON перезаписываются целиком, но теперь cache_meta мержится в pnl_tracer (данные не сохраняются при fail). Профили/actors/watchlist — перезапись. Кэш не трогается при ошибках.

## F. Config (config.yaml)
Секции и потребители:
- telegram: bot_token, chat_id — используется в main.py/telegram_alerts.
- scanner/signals: фильтры 24h объёма/цены, пороги 1m — scanner.py.
- futures: нет отдельной секции, BinanceFuturesClient без ключей.
- nansen: base_url, api_key, flags enrich_binance_signals/enrich_profiler — scanner, profiler_core.
- explorers: etherscan_api_key, bscscan_api_key, arbiscan_api_key, polygonscan_api_key, optimistic_api_key, basescan_api_key — explorer_client, stablecoin monitor, legacy PnL fallback.
- whales/debank/stablecoin_inflow: whale_monitor, stablecoin_inflow_monitor.
- birdeye: enabled, api_keys list, rotate_on_http_status (опц), chain — birdeye_client; proxy файл.
- covalent: enabled, api_key, base_url, max_calls_per_minute — covalent_client/pnl_tracer.
- alpha_profiler: pumps[] описание, retention_days.
- bsc_rpc: (устар.) url — bsc_rpc_client (в PnL не используется).

## G. Observability
- Логгеры создаются через alpha_hunter/logger.get_logger, консольный формат.
- scanner/main: info per cycle; pnl_tracer — подробный прогресс (actor i/n, buckets, 429, sleep); covalent_client — throttling/backoff логи; birdeye_client — ротация ключей/прокси.
- Риски зависания: большой диапазон бакетов, 429 без кэша, медленные Covalent ответы (timeout 40s), BirdEye rate limits.

## H. Known issues / риски
1) PnL merge не сохраняет старые данные при провале fetch (частично решено, но нет полного merge per pump).
2) cache_meta не добавлено в BirdEye/actors/watchlist; UI показывает только covalent cache_meta.
3) Timeline/overlap/network строятся из последнего прогона, без merge с предыдущим кешем.
4) bird-eye cache_meta/страницы не учитываются в итоговых метриках.
5) Роли/бейджи вычисляются в pnl_tracer, но actors_builder их не ставит; без прогона пункта 6 роли в actors.json не обновятся.
6) UI отображает граф/overlap как таблицы, без графических heatmap; нет быстрого фильтра по badges/role кроме таблицы.
7) В explorer_client/pnl_tracer ещё есть BscScan fallback без кэша.
8) Нет force-refresh/skip из CLI; PnL перезаписывает файлы, cache_tx/warehouse могут расти.
9) Nansen интеграция в onchain профайлер может давать 401; нет graceful degrade в онлайн-hunter enrichment.
10) BirdEye key removal из конфига делает постоянное изменение config.yaml (не всегда ожидаемо).

## I. Extensibility plan
- Новые эвристики роли/бейджей: дописать функции в pnl_tracer (role_guess/badges) и вызывать из actors_builder для раннего заполнения.
- Онлайн мониторинг watchlist: использовать artifacts (network/overlap/timeline) как источник; строить watcher на основе scanner/monitors.
- Источники цен: добавить отдельный price cache + фолбэк в timeline_builder.
- Оптимизация лимитов: тоньше token-bucket в covalent_client; force-refresh флаг в alpha_profiler пункте 6.
- BirdEye page-cache: считать метрики cache_meta, merge с PnL.


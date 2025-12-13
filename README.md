# Alpha Hunter

Онлайн‑хантер и оффлайн‑профайлер для быстрого отслеживания пампов на Binance и анализа on-chain активности.

## Структура проекта
- `main.py` — онлайн‑хантер: 1m спот‑сигналы, фьюч‑обогащение, smart‑money (Nansen), стейбл‑инфлоу.
- `manager.py` — консольное меню для запуска онлайн‑хантер/оффлайн‑профайлер.
- `alpha_profiler.py` — CLI профайлера: анализ заранее описанных пампов из `config.yaml`, сбор кластеров.
- `alpha_hunter/` — модули:
  - `scanner.py` — ядро спот‑сканера (1m свечи, фильтры, cooldown, алерты).
  - `binance_client.py`, `binance_futures_client.py` — публичные REST клиенты Binance.
  - `price_client.py` — цены: Binance → DexScreener.
  - `explorer_client.py` — Etherscan V2 токен‑транзакции.
  - `bsc_rpc_client.py` — BSC RPC (чанкование диапазонов, счётчики запросов).
  - `nansen_client.py`, `nansen_monitor.py` — smart‑money данные Nansen.
  - `stablecoin_inflow_monitor.py`, `whale_monitor.py` — ончейн‑мониторы.
  - `profiler_core.py` — логика оффлайн‑профайлера и кластеров.
  - `telegram_alerts.py` — отправка алертов в несколько Telegram‑чатов.
  - `logger.py` — базовый логгер.
- `data/` — шаблоны и результаты:
  - `mm_wallets.json` — адреса для whale‑монитора.
  - `alpha_profiler/` — сохранённые профили *_profile.json и `clusters.json`.
  - файлы состояния (например `stablecoin_inflow_state.json`).
- Тестовые утилиты:
  - `test_keys.py` — проверка ключей (Etherscan V2, Nansen).
  - `test_nodereal_usage.py` — оценка числа RPC запросов (NodeReal).

## Быстрый старт онлайн‑хантера
1. Установите зависимости: `pip install -r requirements.txt`
2. Заполните `config.yaml`:
   - `telegram.bot_token`
   - `telegram.chat_ids` — список chat_id, алерты уйдут во все чаты.
   - `explorers.etherscan_api_key` — используется для всех Etherscan‑совместимых сетей (V2).
   - `nansen` — при наличии ключа включит обогащение smart‑money.
   - `bsc_rpc.url` — NodeReal RPC для профайлера.
   - остальные секции оставьте по необходимости (scanner, signals, whales, stablecoin_inflow, alpha_profiler).
3. Запуск: `python main.py` (онлайн‑хантер) или `python manager.py` (меню).

## Telegram алерты в несколько чатов
В `config.yaml`:
```yaml
telegram:
  bot_token: "YOUR_BOT_TOKEN"
  chat_ids:
    - 123456789      # первый чат
    - -1001234567890 # пример супергруппы/канала
```
Если хотите использовать старый ключ `chat_id`, оставьте его — он автоматически превратится в список.

## Оффлайн‑профайлер
- Список пампов задаётся в `config.yaml` → `alpha_profiler.pumps` (symbol, chain, contract, start/end, окна, пороги).
- Запуск: `python alpha_profiler.py` или через `manager.py` → пункты меню «Прогнать один» / «Прогнать все».
- Источник транзакций:
  - BSC: `bsc_rpc_client` (chunked `eth_getLogs`, обход лимитов, счётчики запросов).
  - Другие сети: `explorer_client` (Etherscan V2).
- Результаты кладутся в `data/alpha_profiler/`, кластеризация — `clusters.json`.
- Авто‑очистка старых профилей управляется `alpha_profiler.retention_days` (или `profiler.retention_days`).

## Потоки данных (высокоуровневый флоу)
Онлайн‑хантер:
```
Binance 24h tickers --> scanner (фильтры USDT, объем/цена) --> top symbols
                                               |
                                               v
                             Binance 1m klines (11 штук на символ)
                                               |
                                               v
                volume_ratio / price_delta --> сигнальный порог & cooldown
                                               |
                                               +--> Futures snapshot (OI/funding)
                                               +--> Nansen smart-money (если включено)
                                               v
                                   TelegramNotifier -> chat_ids[]
```
Оффлайн‑профайлер:
```
config.alpha_profiler.pumps --> iterate pumps
    if chain=bsc: BscRpcClient (find blocks, chunked eth_getLogs)
    else: ExplorerClient (Etherscan V2 tokentx)
    -> агрегируем net-buy по кошелькам
    -> опционально Nansen метки
    -> PriceClient (Binance/DexScreener)
    -> сохраняем *_profile.json
    -> build_clusters() по профилям -> clusters.json
```

## Пример структуры кластеров (clusters.json)
```json
[
  {
    "address": "0xabc...def",
    "tokens": ["ARTX", "BOB"],
    "chains": ["bsc", "eth"],
    "pumps_participated": 3,
    "total_net_usd": 125000.0
  },
  {
    "address": "0x123...789",
    "tokens": ["ARTX"],
    "chains": ["bsc"],
    "pumps_participated": 1,
    "total_net_usd": 42000.0
  }
]
```
Файл лежит в `data/alpha_profiler/`, обновляется функцией `build_clusters()`.

## Nansen: где и как используется
- Конфиг: `nansen.enabled`, `api_key`, пороги `min_token_netflow_usd_24h`, `min_smart_wallets_count`.
- Онлайн‑хантер (`scanner.py`):
  - По символу ищет chain/contract в `alpha_tokens`.
  - Дёргает smart‑money netflows/dex‑trades через `nansen_client`.
  - Добавляет блок *MM / Smart Money контекст* в алерт (netflow, active wallets, совпадения с кластерами, эвристический score).
- Онлайн‑монитор (`nansen_monitor.py`):
  - По списку `alpha_tokens` смотрит smart‑money activity (token_screener) и шлёт отдельные алерты.
- Оффлайн‑профайлер (`profiler_core.py`):
  - При `use_nansen=True` для пампа добавляет smart‑money сведения в профиль/кластеры.

## Мониторы
- **Smart‑money (Nansen)**: `nansen_monitor.py`, использует `alpha_tokens` из конфига для сигналов.
- **Whale monitor**: читает адреса из `data/mm_wallets.json`, опрашивает DeBank‑совместимые источники (если включено).
- **Stablecoin inflow monitor**: следит за крупными входами стейблов на биржевые адреса (Etherscan V2).

## Важные параметры `config.yaml`
- `scanner` / `signals` — пороги 1m‑сканера.
- `nansen.enabled`, `nansen.api_key` — включение smart‑money обогащения.
- `alpha_tokens` — справочник токенов (symbol/chain/contract) для Nansen мониторинга и обогащения сигналов.
- `alpha_profiler.retention_days` — хранение профилей; `alpha_profiler.pumps` — описания пампов.
- `bsc_rpc.url` — RPC NodeReal для BSC (профайлер).

## Проверки
- Сборка: `python -m compileall .`
- Ключи: `python test_keys.py`
- RPC нагрузка (NodeReal): `python test_nodereal_usage.py`

## Расширение
- Добавление новых сетей в `explorer_client` (через Etherscan V2 chainid).
- Расширение алертов/мониторов за счёт новых API‑клиентов по аналогии с `nansen_client`.
- Дополнительные источники цен можно внедрить в `price_client`.

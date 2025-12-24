import inspect
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import yaml

from alpha_hunter.logger import get_logger
from alpha_hunter.explorer_client import ExplorerClient
from alpha_hunter.binance_client import BinanceClient
from alpha_hunter.price_client import PriceClient
from alpha_hunter.nansen_client import NansenClient
from alpha_hunter.profiler_core import (
    PumpWindowConfig,
    analyze_single_pump,
    build_clusters,
    build_actors_and_watchlist,
)
from alpha_hunter.birdeye_client import fetch_for_window as fetch_for_window_birdeye
from alpha_hunter.watchlist_ui import run_watchlist_ui
from alpha_hunter.pnl_tracer import recompute_watchlist_pnl, recompute_actors_pnl
from alpha_hunter.actors_builder import build_actors_watchlist
from alpha_hunter.initiators_builder import build_initiators_index
from alpha_hunter.exit_signatures_builder import build_exit_signatures
from alpha_hunter.config_utils import resolve_nansen_config


def _ensure_utf8_std_streams() -> None:
    try:
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # pragma: no cover
        pass


_ensure_utf8_std_streams()

FORCE_REFRESH_ENV = os.getenv("ALPHA_PROFILER_FORCE_REFRESH", "0") == "1"


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


try:
    from zoneinfo import ZoneInfo  # type: ignore
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


LOCAL_TZ_NAME = "Europe/Kyiv"


def _get_local_tz():
    """
    Возвращает таймзону Киева.
    Если zoneinfo недоступен, fallback: UTC+2 (без учёта перехода на летнее время).
    """
    if ZoneInfo is not None:
        try:
            return ZoneInfo(LOCAL_TZ_NAME)
        except Exception:
            pass
    return timezone(timedelta(hours=2))


def _parse_utc(dt_str: str) -> int:
    """
    Универсальный парсер дат.

    Поддерживаем форматы вида:
      - '2025-12-05 10:40'
      - '2025_12_05 10:40'
      - '2025/12/05 10:40'
      - '2025.12.05 10:40'
      - '2025-12-05T10:40'
      - '05-12-2025 10:40'
      - '05.12.2025 10:40'
      - '05/12/2025 10:40'

    По умолчанию считаем локальное время Киева и конвертируем в UTC.
    Если строка заканчивается на 'UTC' или 'Z', трактуем как уже UTC.
    """
    dt_str = dt_str.strip()
    tz = None
    upper = dt_str.upper()
    if upper.endswith("UTC"):
        dt_str = dt_str[:-3].strip()
        tz = timezone.utc
    elif dt_str.endswith("Z"):
        dt_str = dt_str[:-1].strip()
        tz = timezone.utc

    candidates = [
        "%Y-%m-%d %H:%M",
        "%Y_%m_%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y.%m.%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%d-%m-%Y %H:%M",
        "%d.%m.%Y %H:%M",
        "%d/%m/%Y %H:%M",
    ]

    last_error: Exception | None = None
    for fmt in candidates:
        try:
            naive = datetime.strptime(dt_str, fmt)
            tzinfo = tz or _get_local_tz()
            dt = naive.replace(tzinfo=tzinfo)
            dt_utc = dt.astimezone(timezone.utc)
            return int(dt_utc.timestamp())
        except Exception as exc:
            last_error = exc
            continue

    raise ValueError(f"Не удалось распарсить дату '{dt_str}': {last_error}")


def _build_pump_config(pump_cfg: Dict[str, Any]) -> PumpWindowConfig:
    start_ts = _parse_utc(pump_cfg["start_utc"])
    end_ts = _parse_utc(pump_cfg["end_utc"])
    pre_minutes = int(pump_cfg.get("pre_pump_minutes", 180))
    min_whale_net_usd = float(pump_cfg.get("min_whale_net_usd", 30000))
    use_nansen = bool(pump_cfg.get("use_nansen", True))
    cfg_name = pump_cfg.get("name") or f"{pump_cfg.get('symbol','')}_{pump_cfg.get('chain','')}_{start_ts}"
    return PumpWindowConfig(
        name=str(cfg_name),
        symbol=pump_cfg["symbol"],
        chain=pump_cfg["chain"],
        contract=pump_cfg["contract"],
        start_ts=start_ts,
        end_ts=end_ts,
        pre_window_minutes=pre_minutes,
        min_whale_net_usd=min_whale_net_usd,
        use_nansen=use_nansen,
    )


def _run_pump(
    pump_cfg: Dict[str, Any],
    explorer: ExplorerClient | None,
    price_client: PriceClient,
    nansen_client: NansenClient | None,
    retention_days: int,
    logger,
) -> None:
    name = pump_cfg.get("name", f"{pump_cfg.get('symbol','')}_{pump_cfg.get('chain','')}")
    logger.info("Starting pump profile: %s", name)
    cfg = _build_pump_config(pump_cfg)
    force_refresh = FORCE_REFRESH_ENV or bool(pump_cfg.get("force_refresh"))
    path = analyze_single_pump(
        cfg=cfg,
        explorer_client=explorer,
        price_client=price_client,
        nansen_client=nansen_client if cfg.use_nansen else None,
        logger=logger,
        retention_days=retention_days,
        force_refresh=force_refresh,
    )
    if path:
        print(f"[OK] Профиль {name} сохранён: {path}")
    else:
        print(f"[FAIL] Профиль {name} не построен (см. логи)")


def run_profiler_menu() -> None:
    logger = get_logger("alpha_profiler_menu")
    logger.info(
        "BirdEye fetch_for_window resolved to %s.%s sig=%s",
        fetch_for_window_birdeye.__module__,
        fetch_for_window_birdeye.__name__,
        inspect.signature(fetch_for_window_birdeye),
    )
    logger.info(
        "BirdEye fetch_for_window implementation: %s.%s",
        fetch_for_window_birdeye.__module__,
        fetch_for_window_birdeye.__name__,
    )
    config = load_config()

    explorer = ExplorerClient(config=config, logger=logger)
    binance = BinanceClient(logger=logger)
    price_client = PriceClient(binance_client=binance, config=config, logger=logger)

    nansen_cfg, _ = resolve_nansen_config(config, logger)
    nansen_client = None
    if nansen_cfg.get("enabled") and nansen_cfg.get("api_key"):
        nansen_client = NansenClient(
            api_key=nansen_cfg["api_key"],
            base_url=nansen_cfg.get("base_url", "https://api.nansen.ai"),
            logger=logger,
        )

    profiler_cfg = config.get("alpha_profiler", {}) or {}
    pumps_cfg: List[Dict[str, Any]] = profiler_cfg.get("pumps", []) or []
    retention_days = int(
        profiler_cfg.get("retention_days")
        or config.get("profiler", {}).get("retention_days", 30)
    )

    while True:
        print("\n=== Alpha Offline Profiler ===")
        print("1) Запустить профайлер для выбранного pump из config.yaml")
        print("2) Запустить профайлер для всех pumps из config.yaml")
        print("3) Собрать кластеры (clusters.json)")
        print("4) Собрать actors/watchlist")
        print("5) Запустить Watchlist UI (dashboard)")
        print("6) Пересчитать PnL для адресов watchlist")
        print("7) Построить initiators (warehouse/initiators.json)")
        print("8) Выйти из меню")
        print("9) Построить exit signatures (warehouse/exit_signatures.json)")
        print("10) Пересчитать PnL для всех акторов (system/actors.json)")
        choice = input("Выберите пункт (1-10): ").strip()

        if choice == "1":
            if not pumps_cfg:
                print("В config.yaml не найдено alpha_profiler.pumps, добавь хотя бы один памп.")
                continue
            print("Доступные пампы:")
            for idx, p in enumerate(pumps_cfg, start=1):
                print(
                    f"[{idx}] {p.get('name','')} ({p.get('symbol','')} / {p.get('chain','')} "
                    f"{p.get('start_utc','')} → {p.get('end_utc','')})"
                )
            sel = input("Введите номер пампа или q для отмены: ").strip().lower()
            if sel == "q":
                continue
            try:
                sel_idx = int(sel) - 1
            except ValueError:
                print("Некорректный ввод.")
                continue
            if sel_idx < 0 or sel_idx >= len(pumps_cfg):
                print("Неверный номер.")
                continue
            _run_pump(
                pumps_cfg[sel_idx],
                explorer,
                price_client,
                None,  # временно отключаем Nansen для ручного запуска
                retention_days,
                logger,
            )

        elif choice == "2":
            if not pumps_cfg:
                print("В config.yaml не найдено alpha_profiler.pumps, добавь хотя бы один памп.")
                continue
            for pump in pumps_cfg:
                _run_pump(
                    pump,
                    explorer,
                    price_client,
                    None,  # временно отключаем Nansen для массового запуска
                    retention_days,
                    logger,
                )

        elif choice == "3":
            path = build_clusters(logger=logger)
            print(f"Кластеры пересчитаны и сохранены в: {path}")
        elif choice == "4":
            paths = build_actors_and_watchlist(logger=logger)
            print(f"Actors/watchlist пересобраны: {paths}")
        elif choice == "5":
            logger.info("Starting Watchlist UI dashboard on http://127.0.0.1:8050/")
            run_watchlist_ui()
        elif choice == "6":
            logger.info("Recomputing PnL for watchlist actors with extended window [-3d, +2h]...")
            output_path = recompute_watchlist_pnl(
                config=config,
                logger=logger,
                profiles_dir="data/alpha_profiler",
            )
            logger.info("PnL for watchlist recomputed via Covalent and saved to %s", output_path)
            print(f"Pnl для watchlist пересчитан и сохранён в: {output_path}")
        elif choice == "7":
            logger.info("Building initiators artifact...")
            out_path = build_initiators_index(logger=logger)
            print(f"Initiators ????????? ?: {out_path}")
        elif choice == "9":
            logger.info("Building exit signatures artifact...")
            out_path = build_exit_signatures(logger=logger)
            print(f"Exit signatures saved to: {out_path}")
        elif choice == "10":
            logger.info("Recomputing PnL for all actors (system/actors.json)...")
            output_path = recompute_actors_pnl(
                config=config,
                logger=logger,
                profiles_dir="data/alpha_profiler",
            )
            logger.info("Actors PnL saved to %s", output_path)
            print(f"Pnl для system/actors.json пересчитан и сохранён в: {output_path}")
        elif choice == "8":
            logger.info("Exiting profiler menu")
            break
        else:
            print("Неверный выбор, попробуйте ещё раз.")


if __name__ == "__main__":
    run_profiler_menu()

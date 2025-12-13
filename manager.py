import sys
from typing import NoReturn

from alpha_hunter.logger import get_logger
from main import run_hunter
from alpha_profiler import run_profiler_menu


def run_menu() -> NoReturn:
    logger = get_logger("alpha_manager")

    while True:
        print("\n=== Alpha Hunter Manager ===")
        print("1) Запустить онлайн-хантера")
        print("2) Запустить оффлайн-анализатор (профайлер)")
        print("3) Выход")
        choice = input("Выберите пункт меню (1-3): ").strip()

        if choice == "1":
            logger.info("Starting online hunter...")
            try:
                run_hunter()
            except KeyboardInterrupt:
                logger.info("Online hunter stopped by user")
        elif choice == "2":
            logger.info("Starting offline profiler menu...")
            try:
                run_profiler_menu()
            except KeyboardInterrupt:
                logger.info("Offline profiler terminated by user")
        elif choice == "3":
            logger.info("Exiting manager")
            sys.exit(0)
        else:
            print("Неверный выбор, попробуйте ещё раз.")


if __name__ == "__main__":
    run_menu()

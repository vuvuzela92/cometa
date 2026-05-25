from services.autopilot_manager import AutopilotManager


def run_autopilot() -> None:
    """Запускает основной pipeline отправки настроек.

    Side Effects:
        Инициализирует сервисы, выполняет сетевые запросы к Google Sheets и API Cometa,
        пишет логи в консоль и файлы.
    """
    manager = AutopilotManager()
    manager.run()


if __name__ == "__main__":
    run_autopilot()

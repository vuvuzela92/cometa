from services.autopilot_manager import AutopilotManager
from services.cometa_settings.current_settings_exporter import (
    CurrentAutopilotSettingsExporter,
)
from services.logging_setup import get_logger

log = get_logger("CometaApp.processing")


def run_autopilot() -> None:
    """Запускает основной pipeline отправки настроек.

    Side Effects:
        Инициализирует сервисы, выполняет сетевые запросы к Google Sheets и API Cometa,
        пишет логи в консоль и файлы.
    """
    manager = AutopilotManager()
    manager.run()

    # После применения настроек сразу обновляем лист с текущими настройками.
    # Это отдельный шаг, поэтому его ошибка не должна откатывать уже выполненную отправку.
    try:
        exporter = CurrentAutopilotSettingsExporter()
        exporter.run(export_yesterday=False)
    except Exception as error:
        log.error(f"❌ Не удалось выгрузить текущие настройки в Google Sheets: {error}")


if __name__ == "__main__":
    run_autopilot()

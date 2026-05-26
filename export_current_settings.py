import os

from services.cometa_settings.current_settings_exporter import (
    CurrentAutopilotSettingsExporter,
)


def export_current_settings() -> None:
    """Entrypoint для выгрузки текущих настроек Cometa в Google Sheets."""
    exporter = CurrentAutopilotSettingsExporter()
    export_yesterday = os.getenv("EXPORT_YESTERDAY_SETTINGS", "0") == "1"
    exporter.run(export_yesterday=export_yesterday)


if __name__ == "__main__":
    export_current_settings()

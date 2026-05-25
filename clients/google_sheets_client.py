import gspread
import pandas as pd

from services.logging_setup import get_logger

log = get_logger("CometaApp.processing")


class GoogleSheetClient:
    """Клиент чтения данных из Google Sheets."""

    def __init__(self, creds_path: str, sheet_title: str):
        """Сохраняет параметры подключения к таблице."""
        self.gc = gspread.service_account(filename=creds_path)
        self.title = sheet_title

    def get_data(self, worksheet_name: str) -> pd.DataFrame:
        """Читает лист Google Sheets в DataFrame.

        Args:
            worksheet_name: Имя вкладки в Google Sheets.

        Returns:
            pd.DataFrame: Табличные данные без изменения исходного формата.

        Raises:
            Exception: Пробрасывает исключение чтения после логирования ошибки.
        """
        try:
            spreadsheet = self.gc.open(self.title)
            worksheet = spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_values()
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as error:
            log.error(f"Ошибка при чтении таблицы: {error}")
            raise

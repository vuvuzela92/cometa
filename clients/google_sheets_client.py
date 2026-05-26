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

    def update_worksheet(self, worksheet_name: str, rows: list[list[object]]) -> None:
        """Перезаписывает лист данными.

        Args:
            worksheet_name: Имя вкладки Google Sheets.
            rows: Данные в формате `[[header...], [row1...], ...]`.

        Raises:
            Exception: Ошибки доступа или обновления листа.
        """
        try:
            spreadsheet = self.gc.open(self.title)
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.update(rows)
        except Exception as error:
            log.error(f"Ошибка при записи в таблицу: {error}")
            raise

    def get_worksheet_col_count(self, worksheet_name: str) -> int:
        """Возвращает текущее количество колонок на листе."""
        try:
            spreadsheet = self.gc.open(self.title)
            worksheet = spreadsheet.worksheet(worksheet_name)
            return worksheet.col_count
        except Exception as error:
            log.error(f"Ошибка при чтении размеров листа: {error}")
            raise

    def update_cell(self, worksheet_name: str, row: int, col: int, value: object) -> None:
        """Обновляет одну ячейку на листе."""
        try:
            spreadsheet = self.gc.open(self.title)
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.update_cell(row, col, value)
        except Exception as error:
            log.error(f"Ошибка при записи ячейки: {error}")
            raise

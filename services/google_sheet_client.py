import gspread
import pandas as pd
from cometa.services.logger import setup_logger

log = setup_logger()

# --- Клиент для Google Таблиц ---
class GoogleSheetClient:
    def __init__(self, creds_path: str, sheet_title: str):
        self.gc = gspread.service_account(filename=creds_path)
        self.title = sheet_title

    def get_data(self, worksheet_name: str) -> pd.DataFrame:
        try:
            spreadsheet = self.gc.open(self.title)
            worksheet = spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_values()
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            log.error(f"Ошибка при чтении таблицы: {e}")
            raise
import os
import time
import logging
import json
import requests
import pandas as pd
import gspread
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional
from dotenv import load_dotenv
from colorlog import ColoredFormatter

# --- Настройка логирования ---
def setup_logger():
    logger = logging.getLogger("CometaApp")
    logger.setLevel(logging.INFO)
    color_formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(message)s",
        log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)
    if not logger.handlers:
        logger.addHandler(console_handler)
    return logger

log = setup_logger()

# --- Модель данных ---
@dataclass
class AutopilotSettings:
    api_key_id: int
    product_id: int
    active: Optional[bool] = None
    target_drr: Optional[List[dict]] = None
    target_cost_override: Optional[List[dict]] = None
    min_rem: Optional[List[dict]] = None
    deposit_type: Optional[List[str]] = None
    min_daily_cost: Optional[List[dict]] = None
    max_daily_cost: Optional[int] = None

    def to_api_dict(self) -> dict:
        """Очищает объект от None-полей перед отправкой в API"""
        data = asdict(self)
        # Удаляем ключи, значения которых None или пустые списки (если это допустимо по API)
        return {k: v for k, v in data.items() if v is not None}

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

# --- Клиент для API Кометы ---
class CometaClient:
    def __init__(self, api_key: str):
        self.url = 'https://api.e-comet.io/v1/autopilots'
        self.headers = {'Authorization': api_key, 'Content-Type': 'application/json'}

    def send_batch(self, batch: List[dict]) -> bool:
        for attempt in range(5):
            try:
                response = requests.post(self.url, headers=self.headers, json=batch, timeout=30)
                if response.status_code == 200:
                    log.info(f"✅ Батч успешно отправлен ({len(batch)} шт.)")
                    return True
                elif response.status_code == 429:
                    wait = (attempt + 1) * 2
                    log.warning(f"⚠️ 429 Too Many Requests. Ждем {wait} сек.")
                    time.sleep(wait)
                else:
                    log.error(f"❌ Ошибка {response.status_code}: {response.text}")
                    return False
            except requests.RequestException as e:
                log.error(f"🌐 Ошибка сети: {e}")
                time.sleep(2)
        return False

class AutopilotManager:
    def __init__(self):
        load_dotenv()
        # Загружаем ключ и проверяем его наличие
        api_key = os.getenv('COMETA_API_KEY')
        if not api_key:
            log.error("API ключ COMETA_API_KEY не найден в .env")
            raise ValueError("Missing API Key")

        # Инициализируем клиенты
        self.gs_client = GoogleSheetClient('creds/creds.json', "Панель управления продажами Вектор")
        self.cometa_client = CometaClient(api_key)
        
        # Настраиваем файлы логов
        self.setup_detailed_logging()

    def setup_detailed_logging(self):
        """Настройка записи логов в файлы"""
        # 1. Основной технический лог (app.log) - режим 'a' (дозапись)
        file_handler = logging.FileHandler('app.log', mode='a', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        log.addHandler(file_handler)

        # 2. Файл исключенных артикулов (excluded_rows.log) - режим 'w' (перезапись)
        self.excluded_log_path = 'excluded_rows.log'
        with open(self.excluded_log_path, 'w', encoding='utf-8') as f:
            f.write(f"--- Отчет об исключенных артикулах от {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")

    def log_exclusion(self, row_index: int, product_id: any, reason: str):
        """Метод для записи исключенного артикула в отдельный файл"""
        with open(self.excluded_log_path, 'a', encoding='utf-8') as f:
            f.write(f"Строка {row_index + 2}: Артикул [{product_id}] - Причина: {reason}\n")

    def _parse_row(self, row: pd.Series) -> Optional[AutopilotSettings]:
        """Внутренний метод парсинга строки (логика из предыдущего шага)"""
        try:
            def to_int(val):
                v = str(val).strip().replace('\xa0', '')
                return int(float(v)) if v and v.lower() != 'nan' and v != '' else None

            def to_float(val):
                v = str(val).strip().replace(',', '.').replace('\xa0', '')
                return float(v) if v and v.lower() != 'nan' and v != '' else None

            prod_id = to_int(row.get('Артикул'))
            api_id = to_int(row.get('Идентификатор юрлица'))
            
            if not prod_id or not api_id:
                return None

            settings = AutopilotSettings(api_key_id=api_id, product_id=prod_id)
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Активность
            act = str(row.get('Активность')).strip()
            if act == '1': settings.active = True
            elif act == '0': settings.active = False

            # Расходы
            min_c = to_float(row.get('Минимальный расход'))
            if min_c is not None:
                settings.min_daily_cost = [{"date": today, "cost": int(min_c)}]
            
            max_c = to_int(row.get('Максимальный расход'))
            if max_c is not None:
                settings.max_daily_cost = max_c

            # ДРР
            drr = to_float(row.get('Целевой ДРР'))
            drr_date = str(row.get('Дата, начиная с которой будет действовать целевой ДРР')).strip()
            if drr is not None:
                valid_date = drr_date if drr_date and drr_date != 'nan' else today
                settings.target_drr = [{"date": valid_date, "drr": drr}]

            return settings
        except Exception as e:
            # Ошибки парсинга ловим в run через if not item
            return None

    def run(self):
        df = self.gs_client.get_data("Настройки автопилота")
        log.info(f"Прочитано строк из Google Таблицы: {len(df)}")

        final_payload = []
        stats = {"errors": 0, "empty": 0}

        for index, row in df.iterrows():
            raw_prod_id = row.get('Артикул', 'Неизвестно')
            item = self._parse_row(row)
            
            if not item:
                self.log_exclusion(index, raw_prod_id, "Ошибка формата данных или отсутствуют ID")
                stats["errors"] += 1
                continue

            payload = item.to_api_dict()
            
            # Проверка: если кроме ID ничего не заполнено (длина словаря 2)
            if len(payload) <= 2:
                self.log_exclusion(index, item.product_id, "Нет данных для обновления (все поля пустые)")
                stats["empty"] += 1
                continue

            final_payload.append(payload)

        # Резюме
        summary = (
            f"\n--- РЕЗУЛЬТАТ ОБРАБОТКИ ---\n"
            f"✅ К отправке: {len(final_payload)}\n"
            f"❌ Ошибки данных: {stats['errors']}\n"
            f"⚠️ Пустые записи: {stats['empty']}\n"
            f"Подробности в: {self.excluded_log_path}\n"
            f"--------------------------"
        )
        log.info(summary)

        # Отправка
        batch_size = 1000
        for i in range(0, len(final_payload), batch_size):
            batch = final_payload[i : i + batch_size]
            self.cometa_client.send_batch(batch)

if __name__ == "__main__":
    manager = AutopilotManager()
    manager.run()
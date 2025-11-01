import gspread
from time import time
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import json
import os
import requests
from colorlog import ColoredFormatter

def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename='creds.json')
    for attempt in range(1, retries + 1):
        print(f"[Попытка {attempt} октрыть доступ к таблице")
        try:
            return gc.open(title)
        except gspread.exceptions as e:
            if "503" in str(e):
                print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                time.sleep(delay)
            else:
                raise  # если ошибка не 503 — пробрасываем дальше
    raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")


# Настройка логирования
# Настройка логирования
logger = logging.getLogger("cometa_logger")
logger.setLevel(logging.INFO)

# Форматтеры
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
color_formatter = ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
)

# Хендлеры
file_handler = logging.FileHandler('cometa_change_settings_dashboard.log', encoding='utf-8')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


def main():

    # Открываем таблицу
    table = safe_open_spreadsheet("Панель управления продажами Вектор")
    sheet = table.worksheet("Настройки автопилота").get_all_values()
    df_settings = pd.DataFrame(sheet[1:], columns=sheet[0])
    logger.info(f"Получено {len(df_settings)} записей из гугл-таблицы")

    # Переименовываем колонки
    df_settings = df_settings.rename(columns={
        'Идентификатор юрлица': 'api_key_id',
        'Артикул': 'product_id',
        'Активность': 'active',
        'Дата, начиная с которой будет действовать целевой ДРР': 'target_drr_date',
        'Целевой ДРР': 'target_drr',
        'Дата, начиная с которой будет действовать целевой расход': 'target_cost_date',
        'Целевой расход': 'target_cost_override',
        'Размер': 'size',
        'Количество': 'quantity',
        'Счет автопополнения': 'deposit_type',
        'Минимальный расход': 'min_daily_cost',
        'Максимальный расход': 'max_daily_cost'
    })

    # Функции для преобразования
    def to_int_or_none(x):
        x = str(x).strip()
        return int(x) if x and x != 'nan' else None

    def to_float_or_none(x):
        x = str(x).replace(',', '.').strip()
        return float(x) if x and x != 'nan' else None

    def to_bool_or_none(x):
        return True if str(x).strip() == '1' else (False if str(x).strip() == '0' else None)

    def to_iso_date(date_str):
        if not date_str or pd.isna(date_str):
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                return None

    # Сбор параметров
    params = []
    for _, row in df_settings.iterrows():
        obj = {
            "api_key_id": to_int_or_none(row['api_key_id']),
            "product_id": to_int_or_none(row['product_id']),
            "active": to_bool_or_none(row['active']),
            "target_drr": (
                [{"date": to_iso_date(row['target_drr_date']), "drr": to_float_or_none(row['target_drr'])}]
                if pd.notna(row['target_drr']) and pd.notna(row['target_drr_date']) else []
            ),
            "target_cost_override": (
                [{"date": to_iso_date(row['target_cost_date']), "cost": to_float_or_none(row['target_cost_override'])}]
                if pd.notna(row['target_cost_override']) and pd.notna(row['target_cost_date']) else []
            ),
            "min_rem": (
                [{"quantity": to_int_or_none(row['quantity']), "size": str(row['size'])}]
                if pd.notna(row['quantity']) and pd.notna(row['size']) else []
            ),
            "deposit_type": ([row['deposit_type']] if pd.notna(row['deposit_type']) else []),
            "min_daily_cost": to_int_or_none(row['min_daily_cost']),
            "max_daily_cost": to_int_or_none(row['max_daily_cost'])
        }
        params.append(obj)

    logger.info("Сформирован словарь параметров")

    # Фильтруем пустые записи
    final_params = []
    for param in params:
        target_cost = param['target_cost_override'][0]['cost'] if param['target_cost_override'] else None
        target_drr = param['target_drr'][0]['drr'] if param['target_drr'] else None

        if (param['max_daily_cost'] is None
                and param['min_daily_cost'] is None
                and target_cost is None
                and target_drr is None
                and param['active'] is not False):
            logger.info(f"Удалён product_id: {param['product_id']}")
        else:
            final_params.append(param)

    # Чистим данные
    for p in final_params:
        if not p['target_drr'] or all((not i['date'] or i['drr'] is None) for i in p['target_drr']):
            p['target_drr'] = None
        if not p['target_cost_override'] or all((not i['date'] or i['cost'] is None) for i in p['target_cost_override']):
            p['target_cost_override'] = None
        if not p['min_rem'] or not isinstance(p['min_rem'], list) or p['min_rem'][0].get('quantity') is None:
            p['min_rem'] = None
        if not p['deposit_type'] or all(d not in ['account', 'net', 'bonus'] for d in p['deposit_type']):
            p['deposit_type'] = None

    logger.info(f"Сформированы параметры для передачи настроек в Комету")

    # Загружаем ключ
    load_dotenv()
    cometa_api_key = os.getenv('COMETA_API_KEY') 
    url_change_settings = 'https://api.e-comet.io/v1/autopilots'
    headers = {'Authorization': cometa_api_key}

    # Отправка запроса
    max_attempts = 10
    attempts = 0
    success = False
    while attempts != max_attempts and not success:
        try:
            logger.info("Отправляем POST запрос в Комету")
            response = requests.post(url_change_settings, headers=headers, json=final_params)
            if response.status_code == 200:
                logger.info(f"Настройки автопилота успешно применены:\n{json.dumps(response.json(), indent=2, ensure_ascii=False)}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                success = True
            elif response.status_code == 422:
                logger.warning(f"Ошибка 422. Неверный формат данных. {response.text}")
                attempts += 1
            elif response.status_code == 401:
                logger.warning(f"Ошибка 401. Неверный API ключ. {response.text}")
            elif response.status_code == 403:
                logger.warning(f"Ошибка 403. Недостаточно прав. {response.text}")
            elif response.status_code >= 500:
                logger.warning(f"Ошибка 500. Проблема на сервере. {response.text}")
                attempts += 1
            elif response.status_code == 400:
                logger.warning(f"Ошибка 400. {response.text}")
                try:
                    error_data = response.json()
                    not_found_article = int(error_data['detail'].split(': ')[1])
                    final_params = [item for item in final_params if item.get('product_id') != not_found_article]
                except Exception:
                    pass
            else:
                logger.warning(f"Неожиданный статус ответа {response.status_code}. Ответ сервера: {response.text}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Ошибка запроса к серверу: {e}")

    logger.info(f"Отработка завершена {datetime.now().strftime('%Y-%m-%d-%H-%M')}")
import gspread
from time import time
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import json

def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename='creds.json')
    for attempt in range(1, retries + 1):
        print(f"[Попытка {attempt} октрыть доступ к таблице")
        try:
            return gc.open(title)
        except APIError as e:
            if "503" in str(e):
                print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                time.sleep(delay)
            else:
                raise  # если ошибка не 503 — пробрасываем дальше
    raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cometa_change_settings_dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Начало выполнения скрипта")
    print('Запуск скрипта 🌌')

    # Открываем таблицу
    table = safe_open_spreadsheet("Панель управления продажами Вектор")
    sheet = table.worksheet("Настройки автопилота").get_all_values()
    df_settings = pd.DataFrame(sheet[1:], columns=sheet[0])
    logger.info(f"Получено {len(df_settings)} записей из гугл-таблицы")
    print(f'Таблица открыта')

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

    # Преобразуем в нужные типы
    def to_int_or_none(x):
        x = str(x).strip()
        return int(x) if x and x != 'nan' else None

    def to_float_or_none(x):
        x = str(x).replace(',', '.').strip()
        return float(x) if x and x != 'nan' else None

    def to_bool_or_none(x):
        return True if str(x).strip() == '1' else (False if str(x).strip() == '0' else None)

    def to_iso_date(date_str):
        # Преобразует '24.06.2025' или '2025-06-24' в '2025-06-24'
        if not date_str or pd.isna(date_str):
            return None
        try:
            # Если уже правильный формат
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                return None

    # Собираем список параметров
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
            "deposit_type": (
                [row['deposit_type']] if pd.notna(row['deposit_type']) else []
            ),
            "min_daily_cost": to_int_or_none(row['min_daily_cost']),
            "max_daily_cost": to_int_or_none(row['max_daily_cost'])
        }
        params.append(obj)
    logger.info("Сформирован словарь параметров")

    final_params = []
    for param in params:
        target_cost = None
        if param['target_cost_override'] and len(param['target_cost_override']) > 0:
            target_cost = param['target_cost_override'][0].get('cost')

        target_drr = None
        if param['target_drr'] and len(param['target_drr']) > 0:
            target_drr = param['target_drr'][0].get('drr')

        # Условие для удаления пустых
        if (
            param['max_daily_cost'] is None
            and param['min_daily_cost'] is None
            and target_cost is None
            and target_drr is None
            and param['active'] is not False
        ):
            print(f"Удалён product_id: {param['product_id']}")
        else:
            final_params.append(param)

    for p in final_params:
        # Чистим target_drr
        if not p['target_drr'] or all((not i['date'] or i['drr'] is None) for i in p['target_drr']):
            p['target_drr'] = None

        # Чистим target_cost_override
        if not p['target_cost_override'] or all((not i['date'] or i['cost'] is None) for i in p['target_cost_override']):
            p['target_cost_override'] = None

        # Чистим min_rem
        if not p['min_rem'] or not isinstance(p['min_rem'], list) or p['min_rem'][0].get('quantity') is None:
            p['min_rem'] = None

        # Чистим deposit_type
        if not p['deposit_type'] or all(d not in ['account', 'net', 'bonus'] for d in p['deposit_type']):
            p['deposit_type'] = None
    logger.info(f"Сформированы параметры для передачи настроек в Комету {final_params}")

    load_dotenv()
    cometa_api_key = os.getenv('COMETA_API_KEY') 
    # Отправка запроса
    url_change_settings = 'https://api.e-comet.io/v1/autopilots'
    headers = {'Authorization': cometa_api_key}

    print('Отправляем данные в Комету')
    max_attempts = 10
    attempts = 0
    success = False
    while attempts != max_attempts and not success:
        try:
            logger.info("Отправляем POST запрос в Комету")
            response = requests.post(url_change_settings, headers=headers, json=final_params)
            if response.status_code == 200:
                print(f"Настройки автопилота успешно применены:", response.json())
                logger.info(f"Настройки автопилота успешно применены:{response.json()} {(datetime.now()).strftime('%Y-%m-%d-%H-%M')}")
                success = True
            elif response.status_code == 422:
                print("Ошибка: Неверный формат данных. Проверьте логи для деталей.")
                logger.warning(f"Ошибка 422. Неверный формат данных. Проверьте логи для деталей. {response.text}")
                attempts += 1
            elif response.status_code == 401:
                print("Ошибка: Неверный API ключ.")
                logger.warning(f"Ошибка 401. Неверный API ключ.{response.text}")
            elif response.status_code == 403:
                print("Ошибка: Недостаточно прав для выполнения операции.")
                logger.warning(f"Ошибка 403. Недостаточно прав для выполнения операции.{response.text}")
            elif response.status_code >= 500:
                print("Ошибка: Проблема на стороне сервера. Попробуйте позже.")
                logger.warning(f"Ошибка 500. Проблема на стороне сервера. Попробуйте позже.{response.text}")
                attempts += 1
            elif response.status_code == 400:
                print("Ошибка обработки запроса.")
                error_data = response.json()
                if error_data:
                    # Получаем артикул из строки ошибки
                    not_found_article = int(error_data['detail'].split(': ')[1])
                    # Удаляем запись с этим артикулом из final_params
                    final_params = [item for item in final_params if item.get('product_id') != not_found_article]
                logger.warning(f"Ошибка 400. {response.text}")
            else:
                print(f"Ошибка: Получен неожиданный статус ответа {response.status_code}. Проверьте логи для деталей.")
                logger.warning(f"Ошибка: Получен неожиданный статус ответа {response.status_code}. Ответ сервера: {response.text}")
                
        except requests.exceptions.ConnectionError:
            print("Ошибка: Не удалось подключиться к серверу. Проверьте соединение с интернетом.")
            logger.warning(f"Ошибка: Не удалось подключиться к серверу. Проверьте соединение с интернетом. Ответ сервера: {response.text}")
        except requests.exceptions.Timeout:
            print("Ошибка: Сервер не отвечает. Попробуйте позже.")
            logger.warning(f"Ошибка: Сервер не отвечает. Попробуйте позже. Ответ сервера: {response.text}")
        except requests.exceptions.RequestException as e:
            print("Ошибка: Не удалось выполнить запрос. Проверьте логи для деталей.")
            logger.warning(f"Ошибка: Не удалось выполнить запрос. Проверьте логи для деталей. Ответ сервера: {response.text}")
        except json.JSONDecodeError:
            print("Ошибка: Сервер вернул некорректный ответ. Проверьте логи для деталей.")
            logger.warning(f"Ошибка: Сервер вернул некорректный ответ. Проверьте логи для деталей. Ответ сервера: {response.text}")
        except Exception as e:
            print("Ошибка: Произошла непредвиденная ошибка. Проверьте логи для деталей. Ответ сервера: {response.text}")
            logger.warning(f"Ошибка: Получен неожиданный статус ответа {response.status_code}. Ответ сервера: {response.text}")
    print(f"Отработка завершена {(datetime.now()).strftime('%Y-%m-%d-%H-%M')}")
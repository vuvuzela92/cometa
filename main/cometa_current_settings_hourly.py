import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
import numpy as np
from dotenv import load_dotenv
import os
from utils_sql import create_connection, get_db_table

# Загружаем переменные окружения
load_dotenv()

cometa_api_key = os.getenv('COMETA_API_KEY')
# Получаем текущую дату 
today = datetime.now().strftime('%Y-%m-%d')
# Форматируем дату и время
formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Выгружаем данные текущих настроек автопилотов
url_autopilots = 'https://api.e-comet.io/v1/autopilots'
headers = {'Authorization': cometa_api_key}
response_autopilots = requests.get(url_autopilots, headers=headers)
df_autopilots = pd.DataFrame(response_autopilots.json())

# Создаем новую колонку 'cost' из значений в 'target_cost'
df_autopilots['target_cost'] = df_autopilots['target_cost_override'].apply(
    lambda x:float(x[0]['cost']) if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'cost' in x[0] else np.nan
)
df_autopilots['target_cost_date'] = df_autopilots['target_cost_override'].apply(
    lambda x: x[0]['date'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'date' in x[0] else np.nan
)

# status_filter = df_autopilots['status'].isin(['calibrating_phrases', 'underfunded', 'working', 'target_reached', 'cost_control_product'])
status_filter = ~df_autopilots['status'].isin(['stopped'])
df_autopilots = df_autopilots[status_filter]


# Обрабатываем данные перед загрузкой в БД
def process_dict_columns(df):
    # target_drr
    df['target_drr_date'] = df['target_drr'].apply(
        lambda x: x[0]['date'] if isinstance(x, list) and len(x) > 0 else None
    )

    df['target_drr'] = df['target_drr'].apply(
        lambda x: float(x[0]['drr']) if isinstance(x, list) and len(x) > 0 else None
    )

    # dict → JSON
    dict_columns = ['target_cost_override', 'min_rem']

    for col in dict_columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else None
        )

    return df

df_autopilots = process_dict_columns(df_autopilots)
df_autopilots['date'] = today
df_autopilots = df_autopilots.sort_values(by='max_daily_cost', ascending=False)

# Убедимся, что числовые колонки имеют правильный формат
numbers_columns = ['target_drr', 'min_daily_cost', 'max_daily_cost', 'target_cost']

# Конвертируем в числа (используем точку как разделитель)
for col in numbers_columns:
    df_autopilots[col] = pd.to_numeric(
        df_autopilots[col], 
        errors='coerce'
    )

# Преобразуем остальные колонки в строки
str_columns = ['date', 'target_cost_date', 'target_drr_date', 'deposit_type']
for col in str_columns:
    df_autopilots[col] = df_autopilots[col].astype(str)

df_autopilots = df_autopilots.fillna('')

# Выгружаем данные в гугл-таблицы
# Дает права на взаимодействие с гугл-таблицами
gc = gspread.service_account(filename='creds.json')
# Открывает доступ к гугл-таблице
table = gc.open("Панель управления продажами Вектор")
# Доступ к конкретному листу гугл таблицы
autopilots_sheet = table.worksheet('Текущие настройки автопилота')
df_autopilots['date'] = df_autopilots['date'].astype(str)
df_autopilots['target_cost_date'] = df_autopilots['target_cost_date'].astype(str)
df_autopilots['target_drr_date'] = df_autopilots['target_drr_date'].astype(str)
df_autopilots['deposit_type'] = df_autopilots['deposit_type'].astype(str)


autopilots_sheet.update([df_autopilots.columns.values.tolist()] + df_autopilots.values.tolist())
print('Данные загружены в гугл-таблицу')

# Получаем количество колонок на листе
max_columns = autopilots_sheet.col_count

# Записываем дату и время в первую строку последней колонки
autopilots_sheet.update_cell(1, max_columns, formatted_time)
print(f"Дата и время последнего обновления: {formatted_time}")


# Подключение к базе данных
load_dotenv()
user = os.getenv('USER_2')
name = os.getenv('NAME_2')
password = (os.getenv('PASSWORD_2'))
host = os.getenv('HOST_2')
port = os.getenv('PORT_2')
dialect = 'postgresql'

table_db = 'cometa_current_settings_one'
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# Получение данных из базы данных
connection = create_connection(name, user, password, host, port)
query_1 = f"""SELECT * FROM {table_db}
WHERE date = '{yesterday}' AND status IS NOT NULL"""
df_cometa_yesterday_settings = get_db_table(query_1, connection)
df_cometa_yesterday_settings['date'] = df_cometa_yesterday_settings['date'].astype(str)
df_cometa_yesterday_settings['target_drr_date'] = df_cometa_yesterday_settings['target_drr_date'].astype(str)


df_cometa_yesterday_settings = df_cometa_yesterday_settings.astype(str)
# Убедимся, что числовые колонки имеют правильный формат
numbers_columns = ['target_drr', 'min_daily_cost', 'max_daily_cost', 'target_cost', 'product_id']

# Конвертируем в числа (используем точку как разделитель)
for col in numbers_columns:
    df_cometa_yesterday_settings[col] = pd.to_numeric(
        df_cometa_yesterday_settings[col], 
        errors='coerce'
    )

autopilots_yesterday_sheet = table.worksheet('Вчерашние настройки автопилота')
autopilots_yesterday_sheet.update([df_cometa_yesterday_settings.columns.values.tolist()] + df_cometa_yesterday_settings.values.tolist())

max_columns_yesterday = autopilots_yesterday_sheet.col_count
autopilots_yesterday_sheet.update_cell(1, max_columns_yesterday, formatted_time)
print(f"Дата и время последнего обновления: {formatted_time}")
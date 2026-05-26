from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from clients.cometa_api_client import CometaClient
from clients.google_sheets_client import GoogleSheetClient
from services.cometa_settings.serializer import serialize_sheet_cell
from services.logging_setup import configure_logging, get_logger

log = get_logger("CometaApp.processing")


class CurrentAutopilotSettingsExporter:
    """Экспортирует текущие настройки автопилотов в Google Sheets.

    Формат выгрузки повторяет старый сценарий `cometa_current_settings_hourly.py`,
    но реализован в модульной архитектуре текущего проекта.
    """

    CURRENT_WORKSHEET_NAME = "Текущие настройки автопилота"
    YESTERDAY_WORKSHEET_NAME = "Вчерашние настройки автопилота"
    EXPORT_COLUMNS = [
        "api_key_id",
        "product_id",
        "active",
        "status",
        "target_drr",
        "target_cost_override",
        "min_rem",
        "deposit_type",
        "min_daily_cost",
        "max_daily_cost",
        "search_min_share",
        "brand_traffic",
        "budget_spent_today",
        "target_cost",
        "cost_to_target_pct_today",
        "min_daily_cost_price",
        "min_daily_cost_date_from",
        "target_cost_date",
        "target_drr_date",
        "date",
    ]
    NUMERIC_COLUMNS = ["target_drr", "min_daily_cost", "max_daily_cost", "target_cost"]
    STR_COLUMNS = ["date", "target_cost_date", "target_drr_date", "deposit_type"]

    def __init__(self):
        """Инициализирует API и Google Sheets клиентов."""
        configure_logging()
        load_dotenv()

        api_key = os.getenv("COMETA_API_KEY")
        if not api_key:
            raise ValueError("Missing COMETA_API_KEY")

        self.cometa_client = CometaClient(api_key=api_key)
        self.gs_client = GoogleSheetClient(
            creds_path="creds/creds.json",
            sheet_title="Панель управления продажами Вектор",
        )

    @staticmethod
    def _first_list_item_value(value: Any, key: str) -> Any:
        """Безопасно извлекает `key` из первого элемента списка словарей."""
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value[0].get(key)
        return None

    def _normalize_current_settings(self, raw_items: list[dict]) -> pd.DataFrame:
        """Нормализует данные текущих настроек в формат старого hourly-скрипта."""
        if not raw_items:
            return pd.DataFrame(columns=self.EXPORT_COLUMNS)

        df = pd.DataFrame(raw_items)
        log.info(f"ℹ️ Получено автопилотов из API: {len(df)}")

        if "status" in df.columns:
            df = df[df["status"] != "stopped"]
        log.info(f"ℹ️ После фильтра status != 'stopped': {len(df)}")

        df["min_daily_cost_price"] = df.get("min_daily_cost", pd.Series(dtype=object)).apply(
            lambda x: self._first_list_item_value(x, "cost")
        )
        df["min_daily_cost_date_from"] = df.get("min_daily_cost", pd.Series(dtype=object)).apply(
            lambda x: self._first_list_item_value(x, "date")
        )
        df["target_cost_date"] = df.get("target_cost_override", pd.Series(dtype=object)).apply(
            lambda x: self._first_list_item_value(x, "date")
        )
        df["target_drr_date"] = df.get("target_drr", pd.Series(dtype=object)).apply(
            lambda x: self._first_list_item_value(x, "date")
        )
        df["target_drr"] = df.get("target_drr", pd.Series(dtype=object)).apply(
            lambda x: float(self._first_list_item_value(x, "drr"))
            if self._first_list_item_value(x, "drr") is not None
            else None
        )

        for col in ["target_cost_override", "min_rem"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else ""
                )

        # deposit_type обязательно строкой.
        if "deposit_type" in df.columns:
            df["deposit_type"] = df["deposit_type"].apply(serialize_sheet_cell).astype(str)

        # Колонка date в старом формате для каждой строки.
        df["date"] = datetime.now().strftime("%Y-%m-%d")

        # Сортировка как в старом скрипте.
        if "max_daily_cost" in df.columns:
            df = df.sort_values(by="max_daily_cost", ascending=False)

        for col in self.EXPORT_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        df = df[self.EXPORT_COLUMNS]

        for col in self.NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in self.STR_COLUMNS:
            if col in df.columns:
                df[col] = df[col].astype(str)

        df = df.fillna("")
        return df

    @staticmethod
    def _to_rows(df: pd.DataFrame) -> list[list[object]]:
        """Преобразует DataFrame в rows payload для gspread."""
        serialized_df = df.copy()
        for col in serialized_df.columns:
            serialized_df[col] = serialized_df[col].apply(serialize_sheet_cell)
        return [serialized_df.columns.tolist()] + serialized_df.values.tolist()

    @staticmethod
    def _db_connection_from_env() -> psycopg2.extensions.connection | None:
        """Создает подключение к PostgreSQL из env-переменных старого сценария."""
        db_user = os.getenv("USER_2")
        db_name = os.getenv("NAME_2")
        db_password = os.getenv("PASSWORD_2")
        db_host = os.getenv("HOST_2")
        db_port = os.getenv("PORT_2")

        if not all([db_user, db_name, db_password, db_host, db_port]):
            return None

        return psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )

    def _export_yesterday_settings(self, formatted_time: str) -> None:
        """Опционально экспортирует вчерашние настройки из PostgreSQL в отдельный лист."""
        connection = self._db_connection_from_env()
        if connection is None:
            log.info("ℹ️ Пропуск выгрузки вчерашних настроек: env для PostgreSQL не задан")
            return

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        query = (
            "SELECT * FROM cometa_current_settings_one "
            f"WHERE date = '{yesterday}' AND status IS NOT NULL"
        )

        try:
            df = pd.read_sql(query, connection)
        finally:
            connection.close()

        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
        if "target_drr_date" in df.columns:
            df["target_drr_date"] = df["target_drr_date"].astype(str)

        df = df.astype(str)

        for col in ["target_drr", "min_daily_cost", "max_daily_cost", "target_cost", "product_id"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.fillna("")
        rows = self._to_rows(df)
        self.gs_client.update_worksheet(self.YESTERDAY_WORKSHEET_NAME, rows)

        max_columns = self.gs_client.get_worksheet_col_count(self.YESTERDAY_WORKSHEET_NAME)
        self.gs_client.update_cell(self.YESTERDAY_WORKSHEET_NAME, 1, max_columns, formatted_time)
        log.info(
            f"✅ Обновлен лист '{self.YESTERDAY_WORKSHEET_NAME}', строк: {len(df)}, "
            f"timestamp: {formatted_time}"
        )

    def run(self, export_yesterday: bool = False) -> None:
        """Запускает выгрузку текущих настроек в Google Sheets.

        Args:
            export_yesterday: Если True, дополнительно выгружает вчерашние настройки из PostgreSQL.
        """
        log.info("ℹ️ Старт выгрузки текущих настроек автопилота")
        raw_items = self.cometa_client.get_autopilots()
        df_export = self._normalize_current_settings(raw_items)
        log.info(f"ℹ️ Подготовлено строк к выгрузке: {len(df_export)}")

        rows = self._to_rows(df_export)
        self.gs_client.update_worksheet(self.CURRENT_WORKSHEET_NAME, rows)
        log.info(f"✅ Записан лист '{self.CURRENT_WORKSHEET_NAME}', строк: {len(df_export)}")

        formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        max_columns = self.gs_client.get_worksheet_col_count(self.CURRENT_WORKSHEET_NAME)
        self.gs_client.update_cell(self.CURRENT_WORKSHEET_NAME, 1, max_columns, formatted_time)
        log.info(f"ℹ️ Timestamp последнего обновления: {formatted_time}")

        if export_yesterday:
            self._export_yesterday_settings(formatted_time)

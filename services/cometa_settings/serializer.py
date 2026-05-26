from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import pandas as pd


def serialize_sheet_cell(value: Any) -> Any:
    """Сериализует значение в формат, допустимый для Google Sheets.

    Args:
        value: Произвольное значение из DataFrame/ответа API.

    Returns:
        Any: Одно из поддерживаемых значений:
            - str
            - int
            - float
            - bool
            - "" (пустая строка)

    Notes:
        Google Sheets API не принимает Python `list`/`dict` как значения ячейки.
        Такие объекты сериализуются в JSON-строку.
    """
    if value is None:
        return ""

    # Корректно очищаем NaN/NaT из pandas.
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    if isinstance(value, (list, dict, tuple)):
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, (str, int, float, bool)):
        return value

    # Безопасный fallback для нестандартных типов.
    return str(value)

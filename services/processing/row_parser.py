from datetime import datetime
from typing import Optional

import pandas as pd

from models.autopilot_settings import AutopilotSettings


def parse_autopilot_row(row: pd.Series) -> Optional[AutopilotSettings]:
    """Преобразует строку Google Sheets в модель настроек.

    Args:
        row: Одна строка таблицы как `pandas.Series`.

    Returns:
        AutopilotSettings | None: Валидная модель или `None`, если строка невалидна.

    Notes:
        Функция сохраняет историческое поведение: ошибки парсинга не прерывают
        pipeline, а переводят строку в excluded.
    """
    try:
        def to_int(value: object) -> Optional[int]:
            # `int(float(...))` сохранен для совместимости с ячейками вида "123.0".
            normalized = str(value).strip().replace("\xa0", "")
            if not normalized or normalized.lower() == "nan":
                return None
            return int(float(normalized))

        def to_float(value: object) -> Optional[float]:
            normalized = str(value).strip().replace(",", ".").replace("\xa0", "")
            if not normalized or normalized.lower() == "nan":
                return None
            return float(normalized)

        product_id = to_int(row.get("Артикул"))
        api_key_id = to_int(row.get("Идентификатор юрлица"))
        if not product_id or not api_key_id:
            return None

        settings = AutopilotSettings(api_key_id=api_key_id, product_id=product_id)
        today = datetime.now().strftime("%Y-%m-%d")

        active = str(row.get("Активность")).strip()
        if active == "1":
            settings.active = True
        elif active == "0":
            settings.active = False

        min_cost = to_float(row.get("Минимальный расход"))
        if min_cost is not None:
            settings.min_daily_cost = [{"date": today, "cost": int(min_cost)}]

        max_cost = to_int(row.get("Максимальный расход"))
        if max_cost is not None:
            settings.max_daily_cost = max_cost

        target_drr = to_float(row.get("Целевой ДРР"))
        target_drr_date = str(
            row.get("Дата, начиная с которой будет действовать целевой ДРР")
        ).strip()
        if target_drr is not None:
            # Если дата не передана, используем сегодняшнюю — это текущее правило процесса.
            valid_date = (
                target_drr_date if target_drr_date and target_drr_date != "nan" else today
            )
            settings.target_drr = [{"date": valid_date, "drr": target_drr}]

        return settings
    except Exception:
        # Сохраняем прежнее поведение: любые ошибки парсинга трактуются как invalid-row.
        return None

from datetime import datetime, timedelta
from typing import Optional


def get_tomorrow_date_str() -> str:
    """Возвращает завтрашнюю дату в формате `YYYY-MM-DD`.

    Returns:
        str: Дата на локальном часовом поясе хоста в формате API.

    Notes:
        Используем `astimezone()` для timezone-aware вычисления текущей даты
        на машине запуска, затем сдвигаем на один день.
    """
    local_today = datetime.now().astimezone().date()
    tomorrow = local_today + timedelta(days=1)
    return tomorrow.strftime("%Y-%m-%d")


def enforce_future_budget_dates(payload: dict, target_date: Optional[str] = None) -> dict:
    """Принудительно выставляет дату на завтра для полей с дневными настройками.

    Args:
        payload: Словарь одной настройки, готовой к отправке в API.
        target_date: Предвычисленная дата в формате `YYYY-MM-DD`.
            Если не передана, вычисляется автоматически.

    Returns:
        dict: Тот же payload с обновленными датами:
            - `target_drr[*].date`
            - `min_daily_cost[*].date`

    Notes:
        Логика централизована здесь, чтобы не дублировать вычисление даты в
        parser/batch/client и гарантировать единое поведение перед отправкой.
    """
    effective_date = target_date or get_tomorrow_date_str()

    if isinstance(payload.get("target_drr"), list):
        for item in payload["target_drr"]:
            if isinstance(item, dict):
                item["date"] = effective_date

    if isinstance(payload.get("min_daily_cost"), list):
        for item in payload["min_daily_cost"]:
            if isinstance(item, dict):
                item["date"] = effective_date

    return payload

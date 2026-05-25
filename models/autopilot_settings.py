from dataclasses import asdict, dataclass
from typing import List, Optional


@dataclass
class AutopilotSettings:
    """Модель рекламных настроек для одного артикула.

    Notes:
        Поля с `None` не отправляются в API, что позволяет обновлять только
        явно заданные параметры и не затирать существующие значения.
    """

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
        """Преобразует dataclass в payload для API.

        Returns:
            dict: Словарь без ключей со значением `None`.
        """
        data = asdict(self)
        return {key: value for key, value in data.items() if value is not None}

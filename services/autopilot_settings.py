from dataclasses import dataclass, asdict
from typing import List, Optional

# --- Модель данных ---
@dataclass # Декоратор, который автоматически создает методы __init__, __repr__ и другие
class AutopilotSettings:
    # Обязательные поля
    api_key_id: int   # Уникальный ID вашего рекламного кабинета (целое число)
    product_id: int   # Артикул товара (целое число)

    # Необязательные поля (с дефолтным значением None)
    active: Optional[bool] = None 
    
    # Optional[List[dict]] — здесь ожидается список из словарей, например: [{"date": "...", "drr": 10}]
    target_drr: Optional[List[dict]] = None
    target_cost_override: Optional[List[dict]] = None
    min_rem: Optional[List[dict]] = None
    
    # Optional[List[str]] — список строк, например: ["склад", "транзит"]
    deposit_type: Optional[List[str]] = None
    
    # Настройки бюджета
    min_daily_cost: Optional[List[dict]] = None
    max_daily_cost: Optional[int] = None

    def to_api_dict(self) -> dict:
        """Метод для преобразования объекта класса в обычный словарь Python"""
        
        # asdict(self) — превращает наш красивый объект обратно в словарь {ключ: значение}
        data = asdict(self)
        
        # Итерация (цикл) по словарю:
        # Мы оставляем только те данные (k: v), где значение (v) НЕ равно None.
        # Это критически важно: если мы отправим "active": None в API, 
        # система может сбросить текущие настройки товара, а нам нужно только ОБНОВЛЯТЬ измененное.
        return {k: v for k, v in data.items() if v is not None}
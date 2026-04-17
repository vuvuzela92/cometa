import logging
from colorlog import ColoredFormatter

# --- Настройка логирования ---
def setup_logger():
    # Создаем или получаем логгер с именем "CometaApp" 
    logger = logging.getLogger("CometaApp")
    
    # Устанавливаем минимальный уровень важности
    logger.setLevel(logging.INFO)
    
    # Настраиваем внешний вид строк 
    color_formatter = ColoredFormatter(
        # Формат строки: цветной уровень важности (8 символов), сброс цвета, само сообщение
        "%(log_color)s%(levelname)-8s%(reset)s %(message)s",
        # Назначаем цвета для разных уровней: INFO — зеленый, ERROR — красный и т.д.
        log_colors={
            'DEBUG': 'cyan', 
            'INFO': 'green', 
            'WARNING': 'yellow', 
            'ERROR': 'red', 
            'CRITICAL': 'red,bg_white' # Критическая ошибка будет красной на белом фоне
        }
    )
    
    # Создаем "обработчик" для вывода логов именно в консоль (StreamHandler)
    console_handler = logging.StreamHandler()
    
    # Привязываем созданный выше цветной стиль к этому обработчику
    console_handler.setFormatter(color_formatter)
    
    # если у логгера еще нет обработчиков 
    if not logger.handlers:
        # Добавляем настроенный консольный обработчик к нашему логгеру
        logger.addHandler(console_handler)
    
    # Возвращаем полностью готовый объект логгера для использования в коде
    return logger
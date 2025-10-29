from cometa_utils import main
import logging
from colorlog import ColoredFormatter

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Форматтер
formatter = ColoredFormatter(
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

# Обработчики
file_handler = logging.FileHandler('cometa_change_settings_dashboard.log', encoding='utf-8')
console_handler = logging.StreamHandler()

file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

if __name__ == "__main__":
    logger.info("Начало выполнения скрипта")
    print('Запуск скрипта 🌌')
    main()
    logger.info("Скрипт выполнен успешно")

print("Скрипт завершен")
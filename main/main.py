
from cometa_utils import main
import logging
import sys
import io
from colorlog import ColoredFormatter

# Настройка цветного формата
formatter = ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    }
)

# Принудительно устанавливаем UTF-8 для вывода
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

print("Скрипт запущен в режиме планировщика (каждый час)")

if __name__ == "__main__":
    main()
    
      
print("Скрипт завершен")               
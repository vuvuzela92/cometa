from dotenv import load_dotenv
import os

from cometa.services.logger import setup_logger
from cometa.services.cometa_client import CometaClient
from cometa.services.google_sheet_client import GoogleSheetClient

log = setup_logger()

class AutopilotManager:
    def __init__(self):
        load_dotenv()
        # Загружаем ключ и проверяем его наличие
        api_key = os.getenv('COMETA_API_KEY')
        if not api_key:
            log.error("API ключ COMETA_API_KEY не найден в .env")
            raise ValueError("Missing API Key")

        # Инициализируем клиенты
        self.gs_client = GoogleSheetClient('creds/creds.json', "Панель управления продажами Вектор")
        self.cometa_client = CometaClient(api_key)
        
        # Настраиваем файлы логов
        self.setup_detailed_logging()
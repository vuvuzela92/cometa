import requests
from typing import List, Optional
import time

from cometa.services.logger import setup_logger

log = setup_logger()

# --- Клиент для API Кометы ---
class CometaClient:
    def __init__(self, api_key: str):
        self.url = 'https://api.e-comet.io/v1/autopilots'
        self.headers = {'Authorization': api_key, 'Content-Type': 'application/json'}

    def send_batch(self, batch: List[dict]) -> bool:
        for attempt in range(5):
            try:
                response = requests.post(self.url, headers=self.headers, json=batch, timeout=30)
                if response.status_code == 200:
                    log.info(f"✅ Батч успешно отправлен ({len(batch)} шт.)")
                    return True
                elif response.status_code == 429:
                    wait = (attempt + 1) * 2
                    log.warning(f"⚠️ 429 Too Many Requests. Ждем {wait} сек.")
                    time.sleep(wait)
                else:
                    log.error(f"❌ Ошибка {response.status_code}: {response.text}")
                    return False
            except requests.RequestException as e:
                log.error(f"🌐 Ошибка сети: {e}")
                time.sleep(2)
        return False
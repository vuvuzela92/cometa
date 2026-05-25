import json
import re
import time
from typing import List, Optional, Tuple

import requests

from services.logging_setup import get_logger

log = get_logger("CometaApp.api")


class CometaClient:
    """HTTP-клиент для API Cometa.

    Notes:
        Клиент отвечает за сетевое взаимодействие и retry на транспортном уровне.
        Решения о фильтрации бизнес-данных (исключение артикула) находятся в BatchSender.
    """

    def __init__(self, api_key: str):
        """Инициализирует URL и заголовки авторизации API."""
        self.url = "https://api.e-comet.io/v1/autopilots"
        self.headers = {"Authorization": api_key, "Content-Type": "application/json"}

    def _extract_missing_article_id(self, response_text: str) -> Optional[int]:
        """Извлекает `article_id` из ошибки `Артикул не найден`.

        Args:
            response_text: Тело HTTP-ответа в текстовом виде.

        Returns:
            Optional[int]: Идентификатор артикула или `None`, если формат не совпал.
        """
        try:
            payload = json.loads(response_text)
        except json.JSONDecodeError:
            return None

        detail = str(payload.get("detail", ""))
        match = re.search(r"Артикул не найден:\s*(\d+)", detail)
        if not match:
            return None
        return int(match.group(1))

    def send_batch_with_details(
        self, batch: List[dict]
    ) -> Tuple[bool, Optional[int], Optional[int], str]:
        """Отправляет батч в Cometa API с retry.

        Args:
            batch: Список настроек для отправки одним запросом.

        Returns:
            Tuple[bool, Optional[int], Optional[int], str]:
                success, status_code, missing_article_id, response_text.

        Notes:
            - 429 обрабатывается backoff-паузой.
            - При `400` дополнительно пытаемся извлечь проблемный артикул.
            - Сетевые исключения не прерывают сразу, выполняется повтор.
        """
        for attempt in range(5):
            try:
                response = requests.post(
                    self.url,
                    headers=self.headers,
                    json=batch,
                    timeout=30,
                )
                if response.status_code == 200:
                    log.info(f"✅ Батч успешно отправлен ({len(batch)} шт.)")
                    return True, response.status_code, None, response.text

                if response.status_code == 429:
                    wait = (attempt + 1) * 2
                    log.warning(f"⚠️ 429 Too Many Requests. Ждем {wait} сек.")
                    time.sleep(wait)
                    continue

                log.error(f"❌ Ошибка {response.status_code}: {response.text}")
                missing_article_id: Optional[int] = None
                if response.status_code == 400:
                    missing_article_id = self._extract_missing_article_id(response.text)
                return False, response.status_code, missing_article_id, response.text
            except requests.RequestException as error:
                log.error(f"🌐 Ошибка сети: {error}")
                time.sleep(2)

        return False, None, None, "Network error after retries"

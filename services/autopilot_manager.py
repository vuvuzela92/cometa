import os
from typing import List

from dotenv import load_dotenv

from clients.cometa_api_client import CometaClient
from clients.google_sheets_client import GoogleSheetClient
from services.batch.batch_sender import BatchSender
from services.exclusion_logger import ExclusionLogger
from services.logging_setup import configure_logging, get_logger
from services.processing.row_parser import parse_autopilot_row

log = get_logger("CometaApp.processing")


class AutopilotManager:
    """Оркестрирует полный цикл обновления рекламных настроек.

    Notes:
        Класс intentionally не знает деталей HTTP и retry-алгоритма батча.
        Эти обязанности делегированы клиентам и BatchSender, чтобы упростить
        поддержку и локальные изменения в будущем.
    """

    def __init__(self):
        """Собирает зависимости и проверяет обязательные настройки окружения.

        Raises:
            ValueError: Если не задан `COMETA_API_KEY`.
        """
        configure_logging()
        load_dotenv()
        api_key = os.getenv("COMETA_API_KEY")
        if not api_key:
            log.error("API ключ COMETA_API_KEY не найден в .env")
            raise ValueError("Missing API Key")

        self.gs_client = GoogleSheetClient(
            creds_path="creds/creds.json",
            sheet_title="Панель управления продажами Вектор",
        )
        self.cometa_client = CometaClient(api_key=api_key)
        self.exclusion_logger = ExclusionLogger()
        self.batch_sender = BatchSender(
            cometa_client=self.cometa_client,
            exclusion_logger=self.exclusion_logger,
        )
        self.exclusion_logger.initialize()

    def _build_payload(self) -> List[dict]:
        """Собирает итоговый payload из Google Sheets.

        Returns:
            List[dict]: Подготовленные элементы для отправки в API.

        Side Effects:
            Пишет диагностические сообщения в логи и логирует исключенные строки.

        Notes:
            Правила валидации сохранены без изменения бизнес-логики:
            строки без обязательных ID и строки без полей обновления исключаются.
        """
        dataframe = self.gs_client.get_data("Настройки автопилота")
        log.info(f"Прочитано строк из Google Таблицы: {len(dataframe)}")

        final_payload: List[dict] = []
        stats = {"errors": 0, "empty": 0}

        for index, row in dataframe.iterrows():
            raw_product_id = row.get("Артикул", "Неизвестно")
            settings = parse_autopilot_row(row)
            if not settings:
                self.exclusion_logger.log_row_exclusion(
                    row_index=index,
                    product_id=raw_product_id,
                    reason="Ошибка формата данных или отсутствуют ID",
                )
                stats["errors"] += 1
                continue

            payload = settings.to_api_dict()
            # В payload всегда есть 2 обязательных поля (api_key_id, product_id).
            # Если больше ничего нет, отправка не имеет бизнес-смысла.
            if len(payload) <= 2:
                self.exclusion_logger.log_row_exclusion(
                    row_index=index,
                    product_id=settings.product_id,
                    reason="Нет данных для обновления (все поля пустые)",
                )
                stats["empty"] += 1
                continue

            final_payload.append(payload)

        summary = (
            "\n--- РЕЗУЛЬТАТ ОБРАБОТКИ ---\n"
            f"✅ К отправке: {len(final_payload)}\n"
            f"❌ Ошибки данных: {stats['errors']}\n"
            f"⚠️ Пустые записи: {stats['empty']}\n"
            "Подробности в: logs/excluded_articles.log и logs/processing.log\n"
            "--------------------------"
        )
        log.info(summary)
        return final_payload

    def run(self) -> None:
        """Выполняет orchestration: build payload -> split -> send batch."""
        final_payload = self._build_payload()

        batch_size = 1000
        for batch_number, start in enumerate(
            range(0, len(final_payload), batch_size), start=1
        ):
            batch = final_payload[start : start + batch_size]
            log.info(
                f"ℹ️ Отправка батча #{batch_number}. Исходный размер: {len(batch)}"
            )
            self.batch_sender.send_with_missing_article_filter(
                batch=batch,
                batch_number=batch_number,
            )

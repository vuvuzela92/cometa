import os
from typing import List

from dotenv import load_dotenv

from clients.cometa_api_client import CometaClient
from clients.google_sheets_client import GoogleSheetClient
from services.batch.batch_sender import BatchSender
from services.exclusion_logger import ExclusionLogger
from services.logging_setup import configure_logging, get_logger
from services.processing.payload_date_enforcer import (
    enforce_future_budget_dates,
    get_tomorrow_date_str,
)
from services.processing.row_parser import parse_autopilot_row

log = get_logger("CometaApp.processing")

MAX_MIN_DAILY_COST = 50_000


class AutopilotManager:
    """Оркестрирует полный цикл обновления рекламных настроек."""

    def __init__(self):
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
        """Собирает итоговый payload из Google Sheets."""
        dataframe = self.gs_client.get_data("Настройки автопилота")
        log.info(f"Прочитано строк из Google Таблицы: {len(dataframe)}")

        # Единая дата для всех дневных настроек в этом запуске.
        effective_date = get_tomorrow_date_str()
        log.info(
            f"ℹ️ Для полей target_drr и min_daily_cost принудительно установлена дата: "
            f"{effective_date}"
        )

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
            payload = enforce_future_budget_dates(payload, target_date=effective_date)

            min_daily_cost = payload.get("min_daily_cost")
            if isinstance(min_daily_cost, list):
                invalid_costs = [
                    item.get("cost")
                    for item in min_daily_cost
                    if isinstance(item, dict)
                    and item.get("cost") is not None
                    and item["cost"] > MAX_MIN_DAILY_COST
                ]
                if invalid_costs:
                    self.exclusion_logger.log_row_exclusion(
                        row_index=index,
                        product_id=settings.product_id,
                        reason=(
                            "Минимальный расход превышает лимит Cometa "
                            f"{MAX_MIN_DAILY_COST}: {invalid_costs[0]}"
                        ),
                    )
                    stats["errors"] += 1
                    continue

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

            # Логируем фактически примененную дату на уровне батча.
            if batch:
                target_drr_date = None
                min_daily_cost_date = None
                if isinstance(batch[0].get("target_drr"), list) and batch[0]["target_drr"]:
                    target_drr_date = batch[0]["target_drr"][0].get("date")
                if (
                    isinstance(batch[0].get("min_daily_cost"), list)
                    and batch[0]["min_daily_cost"]
                ):
                    min_daily_cost_date = batch[0]["min_daily_cost"][0].get("date")
                log.info(
                    f"ℹ️ Батч #{batch_number}: даты payload "
                    f"target_drr={target_drr_date}, min_daily_cost={min_daily_cost_date}"
                )

            self.batch_sender.send_with_missing_article_filter(
                batch=batch,
                batch_number=batch_number,
            )

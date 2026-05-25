from typing import List

from clients.cometa_api_client import CometaClient
from services.exclusion_logger import ExclusionLogger
from services.logging_setup import get_logger

log = get_logger("CometaApp.processing")


class BatchSender:
    """Управляет отправкой батчей и повторными попытками на уровне batch.

    Главная задача: сохранить максимум валидных записей, даже если в одном батче
    встречаются артикулы, которых нет в Cometa.
    """

    def __init__(self, cometa_client: CometaClient, exclusion_logger: ExclusionLogger):
        self.cometa_client = cometa_client
        self.exclusion_logger = exclusion_logger

    def send_with_missing_article_filter(self, batch: List[dict], batch_number: int) -> None:
        """Отправляет батч с фильтрацией проблемных артикулов.

        Args:
            batch: Список payload-элементов для одного API-запроса.
            batch_number: Порядковый номер батча для логирования.

        Notes:
            Если API возвращает `400` с сообщением `Артикул не найден: <id>`,
            исключается только этот артикул, после чего тот же батч отправляется повторно.
            Это необходимо, потому что API отклоняет весь батч целиком.
        """
        current_batch = list(batch)
        initial_batch_size = len(current_batch)

        while current_batch:
            success, status_code, missing_article_id, response_text = (
                self.cometa_client.send_batch_with_details(current_batch)
            )
            if success:
                if len(current_batch) != initial_batch_size:
                    log.info(
                        f"✅ Батч #{batch_number} успешно переотправлен после исключений "
                        f"({len(current_batch)} из {initial_batch_size} шт.)"
                    )
                return

            # Повторяем отправку батча после исключения невалидного артикула,
            # потому что API Cometa отклоняет batch целиком при одной ошибке.
            if status_code == 400 and missing_article_id is not None:
                log.warning(
                    f"⚠️ Батч #{batch_number}: найден проблемный артикул "
                    f"{missing_article_id}"
                )
                before_size = len(current_batch)
                current_batch = [
                    item
                    for item in current_batch
                    if item.get("product_id") != missing_article_id
                ]
                after_size = len(current_batch)

                log.info(
                    f"ℹ️ Батч #{batch_number}: исключен артикул {missing_article_id}. "
                    f"Размер {before_size} -> {after_size}"
                )
                self.exclusion_logger.log_api_exclusion(
                    batch_number=batch_number,
                    initial_batch_size=initial_batch_size,
                    article_id=missing_article_id,
                    after_size=after_size,
                )

                if after_size == 0:
                    log.warning(
                        f"⚠️ Батч #{batch_number} полностью исключен после удаления "
                        f"проблемных артикулов"
                    )
                    return
                continue

            # Любая другая ошибка не имеет безопасной автоматической стратегии.
            log.error(
                f"❌ Батч #{batch_number}: необрабатываемая ошибка API "
                f"(status={status_code}): {response_text}"
            )
            return

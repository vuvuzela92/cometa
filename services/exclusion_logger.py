from services.logging_setup import get_logger


class ExclusionLogger:
    """Логирует исключенные записи в выделенный поток логов.

    Notes:
        Класс инкапсулирует формат сообщений, чтобы бизнес-код в менеджере и batch
        не дублировал строковые шаблоны.
    """

    def __init__(self):
        """Инициализирует специализированные логгеры исключений."""
        # Логгер для строк, исключенных на этапе предобработки.
        self.rows_log = get_logger("CometaApp.excluded.rows")
        # Логгер для артикулов, исключенных после ответа API.
        self.articles_log = get_logger("CometaApp.excluded.articles")

    def initialize(self) -> None:
        """Сохраняет совместимость со старым интерфейсом.

        Notes:
            Раньше метод подготавливал файлы вручную. Теперь это no-op, так как
            ротацией и созданием файлов управляет `configure_logging`.
        """
        return None

    def log_row_exclusion(self, row_index: int, product_id: object, reason: str) -> None:
        """Пишет запись об исключении строки на этапе предвалидации.

        Args:
            row_index: Индекс строки DataFrame (0-based).
            product_id: Значение артикула из таблицы (как есть).
            reason: Причина исключения.
        """
        self.rows_log.info(
            f"Строка {row_index + 2}: Артикул [{product_id}] - Причина: {reason}"
        )

    def log_api_exclusion(
        self,
        batch_number: int,
        initial_batch_size: int,
        article_id: int,
        after_size: int,
    ) -> None:
        """Пишет запись об исключении артикула из батча по ответу API.

        Args:
            batch_number: Порядковый номер батча.
            initial_batch_size: Размер батча до фильтрации.
            article_id: Проблемный артикул, возвращенный API.
            after_size: Размер батча после исключения.
        """
        self.articles_log.info(
            f"Батч #{batch_number} (исходный размер {initial_batch_size}): "
            f"исключен артикул [{article_id}], размер после исключения: {after_size}"
        )

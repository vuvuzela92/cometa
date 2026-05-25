import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from colorlog import ColoredFormatter

_CONFIGURED = False


class _ApiErrorsOnlyFilter(logging.Filter):
    """Фильтрует события только для отдельного файла API-ошибок."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith("CometaApp.api") and record.levelno >= logging.ERROR


class _ExcludedOnlyFilter(logging.Filter):
    """Фильтрует события об исключенных строках/артикулах."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith("CometaApp.excluded")


def configure_logging(logs_dir: Path = Path("logs"), retention_days: int = 14) -> None:
    """Централизованно настраивает логирование проекта.

    Args:
        logs_dir: Директория для лог-файлов.
        retention_days: Количество ротаций для хранения.

    Side Effects:
        Создает папку логов, добавляет консольный и файловые handlers.

    Notes:
        Конфигурация идемпотентна и применяется один раз за процесс.
        Ротация выполняется ежедневно в полночь (`when="midnight"`).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    logs_dir.mkdir(parents=True, exist_ok=True)

    base_logger = logging.getLogger("CometaApp")
    base_logger.setLevel(logging.INFO)
    base_logger.propagate = False
    base_logger.handlers.clear()

    color_formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
    text_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)
    base_logger.addHandler(console_handler)

    # Главный поток обработки (чтение, валидация, батчи, статус выполнения).
    processing_handler = TimedRotatingFileHandler(
        filename=logs_dir / "processing.log",
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
    )
    processing_handler.setFormatter(text_formatter)
    base_logger.addHandler(processing_handler)

    # Отдельный файл для API-ошибок ускоряет разбор инцидентов.
    api_errors_handler = TimedRotatingFileHandler(
        filename=logs_dir / "api_errors.log",
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
    )
    api_errors_handler.setFormatter(text_formatter)
    api_errors_handler.addFilter(_ApiErrorsOnlyFilter())
    base_logger.addHandler(api_errors_handler)

    # Отдельный файл для исключений полезен для ручной сверки с таблицей.
    excluded_handler = TimedRotatingFileHandler(
        filename=logs_dir / "excluded_articles.log",
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
    )
    excluded_handler.setFormatter(text_formatter)
    excluded_handler.addFilter(_ExcludedOnlyFilter())
    base_logger.addHandler(excluded_handler)

    _CONFIGURED = True


def get_logger(name: str = "CometaApp") -> logging.Logger:
    """Возвращает именованный логгер.

    Args:
        name: Иерархическое имя логгера (`CometaApp.*`).

    Returns:
        logging.Logger: Готовый к использованию логгер.
    """
    configure_logging()
    return logging.getLogger(name)

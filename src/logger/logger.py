import sys
from logging import Formatter, Logger, StreamHandler, getLogger

# TODO level должен приходить из конфига
# TODO output-stream зависит от уровня. Если debug - консоль, если другой, то например файла в докере


class LogManager(Logger):
    """Python Logging Manager for project."""

    __slots__ = ("_level",)

    def __init__(self, level: str) -> None:
        self._level = level

    def get_logger(self, name: str) -> Logger:
        """Returns configured logger instance

        ## Parameters
        `name` : Name of the logger

        ## Returns
        `logging.Logger` : Returns Logger class object
        """
        logger = getLogger(name=name)

        logger.setLevel(level=self._level)

        if logger.hasHandlers():
            logger.handlers.clear()

        handler = StreamHandler(stream=sys.stdout)

        handler.setFormatter(
            fmt=Formatter(
                fmt=r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s",
                datefmt=r"%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False

        return logger

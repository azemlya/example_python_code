
"""
Наш логгер примесь
"""
from airflow_se.obj_imp import LoggingMixin

__all__ = [
    'LoggingMixinSE',
]


class LoggingMixinSE(LoggingMixin):
    """
    Наш класс логирования
    """
    def __init__(self, debug: bool = False, context = None):
        """
        Если debug включён, то все наши сообщения отправленные в debug, будут перенаправлены в info
        """
        super().__init__(context)
        # перенаправляем debug в info, если debug is True
        self.debug = self.log.info if debug is True else self.log.debug
        # создаём прямые ссылки, чтоб короче были вызовы
        self.info = self.log.info
        self.warning = self.log.warning
        self.error = self.log.error
        self.critical = self.log.critical
        self.exception = self.log.exception
        # дублируем с префиксом `log_`, ну, чтоб понятнее и удобнее было (и так, и так, можно)
        self.log_info = self.info
        self.log_debug = self.debug
        self.log_warning = self.log.warning
        self.log_error = self.log.error
        self.log_critical = self.log.critical
        self.log_exception = self.log.exception


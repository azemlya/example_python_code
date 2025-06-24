
"""
    Kafka SE processing
    No comments ¯\_(ツ)_/¯
"""
# отключение лишних сообщений в логах (из подключаемых библиотек) о неподдерживаемых функциях и т.д.
from warnings import simplefilter as warnings_simplefilter
warnings_simplefilter("ignore")

from typing import Optional, Union, Dict, Any
from json import loads, dumps
from argparse import ArgumentParser, Namespace
from time import sleep
from datetime import datetime
from confluent_kafka import Producer, version as confluent_kafka_version, libversion as confluent_kafka_libversion

from airflow_se.logger import LoggingMixinSE
from airflow_se.settings import Settings, get_settings
from airflow_se.utils import timedelta_to_human_format
from airflow_se.db import mark_part_messages, get_part_messages_for_delivery

__all__ = [
    "run",
]

def run():
    parser = ArgumentParser(description="Airflow SE references log pusher to Kafka")
    parser.add_argument("se_kafka", type=str, help="se_kafka")
    parser.add_argument("--pid", dest="pid", type=str, help="ProcessID file name")
    return Kafka(parser.parse_args())()


class Kafka(LoggingMixinSE):
    """
    Отправка событий аудита в Кафку.
    """

    def __call__(self, *args, **kwargs):
        """This is a callable object"""
        self.run(*args, **kwargs)

    def __init__(self, cla: Namespace):
        """Constructor"""
        self.__settings: Settings = get_settings()
        super().__init__(self.conf.debug)
        self.__cla = cla  # cla - Command Line Arguments
        self.__cla_pid = self.__cla.pid if hasattr(self.__cla, "pid") and self.__cla.pid else None
        hr = "*" * 80
        self.log_info(hr)
        self.log_info(f"    - `confluent-kafka` version: {confluent_kafka_version()}")
        self.log_info(f"    - `lirrdkafka` version: {confluent_kafka_libversion()}")
        self.log_info(f"""Application is started. ProcessID file name: """
                      f"""{self.__cla_pid if self.__cla_pid else "PID file is not use"}""")
        try:
            if self.__cla_pid:
                with open(self.__cla_pid, "w") as f:
                    from os import getpid
                    f.write(str(getpid()))
        except Exception as e:
            self.log_error(f"PID file write error: {e}")
        self.log_info(hr)
        self.__app_start = datetime.now()
        self.__counter: int = 0
        self.__producer: Optional[Producer] = None

    @property
    def conf(self) -> Settings:
        return self.__settings

    @property
    def counter(self) -> int:
        return self.__counter

    @counter.setter
    def counter(self, value: int):
        self.__counter = value

    @property
    def app_start(self):
        return self.__app_start

    @property
    def process_timeout(self) -> int:
        """Таймаут процесса (на сколько секунд он будет "засыпать")"""
        return self.conf.kafka_process_timeout

    @property
    def process_retry(self) -> Optional[int]:
        """Количество повторений цикла, None - бесконечный цикл"""
        return self.conf.kafka_process_retry

    @property
    def producer_settings(self) -> Dict[str, Any]:
        """Настройки продюсера для подключения к Кафке"""
        return self.conf.kafka_producer_settings

    @property
    def topic(self) -> str:
        """Топик Кафки"""
        return self.conf.kafka_topic

    @property
    def page_size(self) -> int:
        """Максимальное количество сообщений, которые обрабатывать за одну итерацию цикла"""
        return self.conf.kafka_page_size

    @staticmethod
    def format_message(mess: Dict[str, Any]) -> Dict[str, Any]:
        """Форматирует сообщение"""
        _mess = {
            "app_id": mess.get("app_id"),
            "type_id": mess.get("type_id"),
            "subtype_id": mess.get("subtype_id"),
            # "timestamp": int(mess.get("ts").timestamp() * 1000),  # не обязательное поле (тип Integer)
            "cluster_name": mess.get("host"),  # не обязательное поле
            # "environment": "",  # не обязательное поле
            # "message": "",  # обязательное для сообщений технического типа "type_id" = "Tech"
            "IP_ADDRESS": mess.get("remote_addr"),
            "USER_LOGIN": mess.get("remote_login"),
            "STATUS": mess.get("status_op"),
            "OPERATION_DATE": mess.get("ts").strftime("%d.%m.%Y %H-%M-%S"),
        }
        _mess.update(mess.get("extras"))
        _mess.update({"inner_source_id": mess.get("id")})
        return _mess

    def producer(self, recreate: bool = False) -> Optional[Producer]:
        """Producer Kafka"""
        if recreate or not self.__producer:
            # self.log_info(f"{self.producer_settings=}")
            self.__producer = Producer(self.producer_settings)
        return self.__producer

    def delivery_report(self, err, msg):
        """Функция-обработчик ответа от Кафки"""
        if err is not None:
            self.log_error(f"""Message NOT delivered: """
                           f"""key = "{msg.key().decode("utf-8")}", value = "{msg.value().decode("utf-8")}" """
                           f"""// Error delivery to topic "{msg.topic()}"[partition "{msg.partition()}"]: {err}""")
        else:
            ids: Optional[int] = None
            try:
                ids = loads(msg.value().decode("utf-8")).get("inner_source_id")
            except Exception as e:
                self.log_error(f"Error parsing JSON from returned Kafka producer message: {e}")
            if isinstance(ids, int) and ids > 0:
                self.log_debug(f"""Message delivered to topic "{msg.topic()}"[partition "{msg.partition()}"]: """ 
                               f"""timestamp = "{msg.timestamp()}", key = "{msg.key().decode("utf-8")}", """
                               f"""value = "{msg.value().decode("utf-8")}\"""")
                try:
                    mark_part_messages(ids=ids)
                except Exception as e:
                    self.log_error(f"Audit record with id = {ids} was delivered to Kafka "
                                   f"and NOT updated in DB as pushed: {e}")
                else:
                    self.log_info(f"Audit record with id = {ids} was delivered to Kafka and updated in DB as pushed")
            else:
                self.log_debug(f"""Message delivered to topic "{msg.topic()}"[partition "{msg.partition()}"]: """
                               f"""timestamp = "{msg.timestamp()}", key = "{msg.key().decode("utf-8")}", """
                               f"""value = "{msg.value().decode("utf-8")}\"""")
                self.log_error(f"""Kafka producer return invalid value for key "inner_source_id": """
                               f"""type = "{type(ids)}", value = "{ids}\"""")

    def delivery_messages_to_kafka(self, messages: Union[list, tuple, None] = None):
        """Отправка сообщений в Кафку"""
        if isinstance(messages, (list, tuple)) and len(messages) > 0:
            self.log_info(f"Messages for delivery in the queue: {len(messages)}")
            self.log_info(f"Start delivery this messages...")
            try:
                producer = self.producer()
                for mess in messages:
                    _mess = self.format_message(mess)
                    producer.produce(topic=self.topic,
                                     key=mess.get("code_op").encode("utf-8"),
                                     value=dumps(_mess).encode("utf-8"),
                                     timestamp=int(mess.get("ts").timestamp() * 1000),
                                     callback=self.delivery_report,
                                     )
                    producer.poll(10)
                    self.log_debug(f"Produce and pool message: inner_source_id={_mess.get('inner_source_id')}")
                self.log_debug("Flush: 1 minute...")
                producer.flush(60)
                self.log_debug("Producer flushed")
            except Exception as e:
                self.log_error(f"Kafka producer error: {e}")
                self.log_exception(e)
        else:
            self.log_info("No messages for delivery to Kafka")

    def run(self, *args, **kwargs):
        """Бесконечный цикл приложения"""
        _, _ = args, kwargs
        while True:
            try:
                self.counter += 1
                self.log_info(f"""========  Iteration {self.counter} of """
                              f"""{self.process_retry if self.process_retry else "infinity"}.  ========""")
                days, hours, minutes, seconds = timedelta_to_human_format(datetime.now() - self.app_start)
                self.log_info(f"Continuous operation time of the application: "
                              f"{days} days, {hours:02}:{minutes:02}:{seconds:02}")
                # проверка на количество итераций
                if isinstance(self.process_retry, int) and (self.counter > self.process_retry):
                    self.log_info(f"Number of iterations ({self.process_retry}) completed. Application closed.")
                    break
            except Exception as e:
                self.log_error(f"Exception: {e}")
            self.delivery_messages_to_kafka(get_part_messages_for_delivery(part=self.page_size))
            ths, tms, tss = self.process_timeout // 3600, (self.process_timeout % 3600) // 60, self.process_timeout % 60
            self.log_info(f"Sleep at time {ths:02}:{tms:02}:{tss:02}")
            self.log_info("<< ... ... ... ... ...  sweet dream  ... ... ... ... ... >>")
            sleep(self.process_timeout)


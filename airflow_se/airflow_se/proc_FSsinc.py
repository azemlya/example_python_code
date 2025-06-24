"""
Процесс синхронизации корзины S3 хранилища с содержимым DAGS_FOLDER
"""
import os
import sys
#from dotenv import load_dotenv
# С помощью библиотеки python dotenv загружаем данные из .env файла
# load_dotenv()
from socket import getfqdn

import boto3
import logging

from datetime import datetime as dt
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler
from airflow.configuration import AIRFLOW_HOME, conf # объект класса AirflowConfigParser

from airflow_se.settings import Settings, get_settings
from airflow_se.config import get_config_value
from airflow_se.logger import LoggingMixinSE
from airflow_se.crypt import decrypt, encrypt
from airflow_se.secman import get_secman_data, auth_secman
from airflow_se.utils import info, info_attrs, get_env, env
from airflow_se.commons import SECMAN_KEY_FOR_SECRET
from airflow_se.db import create_new_audit_record, Session

log = LoggingMixinSE(debug=True)
#log = LoggingMixinSE.logging.getLogger().setLevel(logging.INFO)
#log = logging.getLogger(__name__)

SE_AWS_URL = get_config_value("AWS_URL")
SE_AWS_PORT = get_config_value("AWS_PORT")
SE_AWS_BUCKET_NAME = get_config_value("AWS_BUCKET_NAME")
SE_AWS_BUCKET_DIR = get_config_value("AWS_BUCKET_DIR")
SE_AWS_ACCESS_ID = get_config_value("AWS_ACCESS_ID")
# получение секрета из секмана
sm_secrets = get_secman_data(SECMAN_KEY_FOR_SECRET, auth_secman())
log.info(f"240423_1214 {info(sm_secrets)=}")
se_aws_secret_key = sm_secrets.get('SE_AWS_SECRET_KEY')
log.info(f"240423_1215 {info(se_aws_secret_key)=}")
conf_settings: Settings = get_settings()
if se_aws_secret_key:
    SE_AWS_SECRET_KEY = decrypt(se_aws_secret_key).decode('utf-8')  # get_config_value("AWS_SECRET_KEY")
    log.info(f"240423_1216 {info(SE_AWS_SECRET_KEY)=}")
else:
    log.critical("240423_1216 не найден ключ SE_AWS_SECRET_KEY в секмане")
    raise ValueError("not found SE_AWS_SECRET_KEY in Secman")
SE_DAGS_FOLDER = conf.get('core', 'dags_folder')
log.info(f"240425_1746 {SE_DAGS_FOLDER=}")

# s3 = boto3.resource(service_name="s3",
#                     aws_secret_access_key=SE_AWS_SECRET_KEY,
#                     aws_access_key_id=SE_AWS_ACCESS_ID,
#                     endpoint_url=":".join([SE_AWS_URL, SE_AWS_PORT])
# my_bucket = s3.Bucket(s3_bucket_name)
client = boto3.client(service_name="s3",
                      aws_secret_access_key=SE_AWS_SECRET_KEY,
                      aws_access_key_id=SE_AWS_ACCESS_ID,
                      endpoint_url=":".join([SE_AWS_URL, SE_AWS_PORT])
                      ) # https://habr.com/ru/articles/748276/

def sync_from_fs_to_s3(path: str, file: str, event_type: str) -> None:
    file_path = os.path.join(path, file)
    temp_file_path = os.path.join(path, '__pycache__', f'{file}_temp_for_S3')
    # Если в бакете есть папка SE_AWS_BUCKET_DIR, файлы будут загружены туда, в противном случае можно оставить только str(file)
    upload_file_key = "".join([SE_AWS_BUCKET_DIR, "/", file]) #"airflow_dev_003/" + str(file)
    aud_msg = {
        "ts": dt.now(),
        "host": getfqdn(),
        "remote_addr": "not applicable",
        "remote_login": "not applicable",
        "code_op": "other audit operations",
        "app_id": conf_settings.kafka_app_id,
        "type_id": "Audit",
        "subtype_id": "F0",
        "status_op": "SUCCESS",
        "extras": {
            "PARAMS": dict(EVENT_TYPE=f"FS sync to S3 Ozone through encode by Fernet",),
            "SESSION_ID": "not applicable",
            "USER": "not applicable",
        },
    }
    if event_type in ["created", "modified"]:
        log.info(f"240409_1741-01 ======== sync_from_fs_to_s3 - start if event_type in [created, modified]: =======================")
        log.info(f"240402_1815-00, Непосредственно загружаем файл {file_path=} в S3 бакет, {event_type=}, через {temp_file_path=}")
        with open(file_path, "rt") as f:
            v = f.read()
            t = f"240402_1815-01 содержимое файла {file_path=} в папке ДАГов текстовое: \n{v[0:100]=}"
            log.info(t)
        with open(temp_file_path, "wt") as f:
            encrypt_v = encrypt(v)
            t = f"240402_1815-02 содержимое файла {temp_file_path=} для S3 корзины бинарное после encrypt(v): \n{encrypt_v[0:100]=}"
            f.write(encrypt_v)
            log.info(t)
        client.upload_file(temp_file_path, SE_AWS_BUCKET_NAME, upload_file_key)
        log.info(f"240409_1741-02 ======== sync_from_fs_to_s3 - end if event_type in [created, modified]: =======================")
    elif event_type in ["deleted", "moved"]:
        log.info(f"240402_1815-04 удаляем файл {file_path=} из S3 бакета, {event_type=} ")
        client.delete_object(Bucket=SE_AWS_BUCKET_NAME, Key=upload_file_key)
    aud_msg["extras"]["PARAMS"].update(dict(FS_OPERATION=f"Due to: '{event_type}' file action",
                                            FS_FILE_NAME=file_path,
                                            OZONE_OBJ_NAME=upload_file_key,
                                            ))
    msg = audit_action_add(**aud_msg)
    log.info(f"audit recorded: {msg=}")

def audit_action_add(host: str,
                     remote_addr:str,
                     remote_login: str,
                     code_op: str,
                     app_id: str,
                     type_id: str,
                     subtype_id: str,
                     status_op: str,
                     extras: dict,
                     ts: dt = dt.now(),
                     ):
    kwargs = {'ts': ts,
              'host': host,
              'remote_addr': remote_addr,
              'remote_login': remote_login,
              'code_op': code_op,
              'app_id': app_id,
              'type_id': type_id,
              'subtype_id': subtype_id,
              'status_op': status_op,
              'extras': extras}
    # вырезал аудит в файловую систему, что по задаче 239, так как для openshift не используется
    airflow_audit_logs_file = os.environ.get('SE_AIRFLOW_AUDIT_LOGS_FILE')
    if airflow_audit_logs_file is not None and isinstance(airflow_audit_logs_file, str):
        with open(airflow_audit_logs_file, 'a') as f:
            f.write(''.join(['\n', str(kwargs)]))
    return create_new_audit_record(**kwargs)

# def dags_folder() -> str:
#     # from airflow_se.config import get_config_value
#     #DAGS_FOLDER = get_config_value("AIRFLOW__CORE__DAGS_FOLDER")
#     # Через конфиг парсер Airflow # from airflow.configuration import conf # объект класса AirflowConfigParser
#     DAGS_FOLDER = conf.get('core', 'dags_folder')
#     return DAGS_FOLDER



class Handler(FileSystemEventHandler):

    # @staticmethod
    # def dags_folder() -> str:
    #     # from airflow_se.config import get_config_value
    #     # DAGS_FOLDER = get_config_value("AIRFLOW__CORE__DAGS_FOLDER")
    #     # Через конфиг парсер Airflow # from airflow.configuration import conf # объект класса AirflowConfigParser
    #     DAGS_FOLDER = conf.get('core', 'dags_folder')
    #     return DAGS_FOLDER

    def on_any_event(self, event):
        path = SE_DAGS_FOLDER  # dags_folder()  # вызывает цикличность - ПОТОМ РАЗОБРАТЬСЯ ПОЧЕМУ - "/opt/airflow_dev/dags" # !!!
        file_name_only = event.src_path.split('/')[-1]
        #not in ['opened','closed']
        if event.is_directory is False \
            and not event.src_path.startswith(os.path.join(path, '__pycache__')) \
            and str(event.event_type) in ["created", "modified", "deleted", "moved"] \
            and event.is_synthetic is False \
            and not file_name_only.startswith('.') \
            and (file_name_only.endswith('.py') or file_name_only.endswith('.ipynb')):
            dir_name = os.path.dirname(event.src_path)
            file_name = os.path.basename(event.src_path)
            sync_from_fs_to_s3(dir_name, file_name, str(event.event_type))

def run():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    event_handler = Handler() #LoggingEventHandler()
    observer = Observer()
    path = SE_DAGS_FOLDER  # dags_folder()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while observer.is_alive(): #isAlive():
            observer.join(10)
    except KeyboardInterrupt:
        observer.stop()
    finally:
        observer.stop()
        observer.join()
        #log.info(f"Finaly file changes: {observer.__dict__=}")

if __name__ == "__main__":
    run()
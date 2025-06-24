"""
Процесс синхронизации DAGS_FOLDER с корзиной в S3 хранилище
"""

import datetime
import time
from re import match, subn
from socket import getfqdn

import boto3
import os
from argparse import ArgumentParser, Namespace
from functools import cached_property
import logging
#from dotenv import load_dotenv
# С помощью библиотеки python dotenv загружаем данные из .env файла
# load_dotenv()
from botocore.exceptions import ClientError

from airflow.configuration import AIRFLOW_HOME, conf # объект класса AirflowConfigParser

from airflow_se.crypt import decrypt, encrypt
from airflow_se.config import get_config_value
from airflow_se.logger import LoggingMixinSE
from airflow_se.settings import Settings, get_settings
from airflow_se.secman import get_secman_data, auth_secman
from airflow_se.utils import info, info_attrs, get_env
from airflow_se.commons import SECMAN_KEY_FOR_SECRET
from airflow_se.db import create_new_audit_record

__all__ = ["run", ]

#log = LoggingMixinSE(debug=True)
#log = LoggingMixinSE.logging.getLogger().setLevel(logging.INFO)

log = logging.getLogger(__name__)

SE_AWS_URL = get_config_value("AWS_URL")
SE_AWS_PORT = get_config_value("AWS_PORT")
SE_AWS_BUCKET_NAME = get_config_value("AWS_BUCKET_NAME")
SE_AWS_BUCKET_DIR = get_config_value("AWS_BUCKET_DIR")
SE_AWS_ACCESS_ID = get_config_value("AWS_ACCESS_ID")
# получение секрета из секмана
sm_secrets = get_secman_data(SECMAN_KEY_FOR_SECRET, auth_secman())
log.info(f"240423_1214 {info(sm_secrets)=}")
se_aws_secret_key = sm_secrets.get('SE_AWS_SECRET_KEY', False)
log.info(f"240423_1215 {info(se_aws_secret_key)=}")
if se_aws_secret_key:
    SE_AWS_SECRET_KEY = decrypt(se_aws_secret_key).decode('utf-8')  # get_config_value("AWS_SECRET_KEY")
    log.info(f"240423_1216 {info(SE_AWS_SECRET_KEY)=}")
else:
    log.critical("240423_1216 не найден ключ SE_AWS_SECRET_KEY в секмане")
    raise ValueError("not found SE_AWS_SECRET_KEY in Secman")

def run():
    parser = ArgumentParser(description="Airflow se_S3sinc")
    parser.add_argument("se_S3sinc", type=str, help="se_S3sinc")
    parser.add_argument("--pid", dest="pid", type=str, required=False, help="ProcessID file name")
    parser.add_argument("--sync-interval", dest="sync_interval", type=int, required=False, default=5, help="sync interval")
    return S3sinc(parser.parse_args())()


class S3sinc(LoggingMixinSE):
    """"""
    def __init__(self, cla: Namespace):
        self.__settings: Settings = get_settings()
        super().__init__(self.conf.debug)
        self.__cla = cla  # cla - Command Line Arguments
        cla_pid = self.__cla.pid if hasattr(self.__cla, "pid") and self.__cla.pid else None
        self.log_info(f"""Application is started. ProcessID file name: """
                      f"""{cla_pid if cla_pid else "PID file is not use"}""")
        try:
            if cla_pid:
                with open(cla_pid, "w") as f:
                    f.write(str(os.getpid()))
        except Exception as e:
            self.log_error(f"PID file write error: {e}")
        self.sync_interval = self.__cla.sync_interval if hasattr(self.__cla, "sync_interval") and self.__cla.sync_interval else 5

    def __call__(self):
        """This is a callable object"""
        self.run()

    @property
    def conf(self) -> Settings:
        return self.__settings

    def run(self):
        self.sync_s3_to_fs(bucket_name=SE_AWS_BUCKET_NAME, dag_dir=self.dags_folder, sync_interval=self.sync_interval)

    @cached_property
    def dags_folder(self) -> str:
        #DAGS_FOLDER = os.path.expanduser(conf.get("core", "DAGS_FOLDER"))
        # from airflow_se.config import get_config_value
        # DAGS_FOLDER = get_config_value("AIRFLOW__CORE__DAGS_FOLDER")
        # более правильно через конфиг парсер Airflow # from airflow.configuration import conf # объект класса AirflowConfigParser
        DAGS_FOLDER = conf.get('core', 'dags_folder')
        return DAGS_FOLDER

    @cached_property
    def client(self):
        return boto3.client(service_name="s3",
                            aws_secret_access_key=SE_AWS_SECRET_KEY,
                            aws_access_key_id=SE_AWS_ACCESS_ID,
                            endpoint_url=":".join([SE_AWS_URL, SE_AWS_PORT]),
                            )

    def sync_s3_to_fs(self, bucket_name: str, dag_dir: str, sync_interval: int = 5):
        while True:
            fs_files = set(os.listdir(dag_dir))
            content = self.client.list_objects(Bucket=bucket_name) #s3.list_objects_v2(Bucket=bucket_name)
            for content_list_member in content['Contents']:
                #self.log.info(f"240409_1028-01, {content_list_member['Key']=}")
                s3_file = subn(f'{SE_AWS_BUCKET_DIR}/', '', content_list_member['Key'], 1)[0]
                #self.log.info(f"240409_1028-02, {s3_file=} {'is True' if s3_file else 'is False'} ")
                if match(f'^{SE_AWS_BUCKET_DIR}/', content_list_member['Key']) and s3_file:
                    #self.log.info(f"240402_1827-00, объект <{s3_file=}> ")
                    if s3_file not in fs_files:
                        self.log.info(f"240409_1010-01 ======== start - if s3_file not in fs_files =======================")
                        file_path = os.path.join(dag_dir, s3_file)
                        temp_file_path = os.path.join(dag_dir, '__pycache__', f'{s3_file}_temp_for_S3')
                        upload_file_key = "".join([SE_AWS_BUCKET_DIR, "/", s3_file])
                        self.log.info(f"240402_1827-04, объект корзины S3 {upload_file_key=} отсутствует среди файлов FS {fs_files}"
                                      f" - добавляем его в папку {file_path=} через {temp_file_path=}")
                        aud_msg = {
                            "ts": datetime.datetime.now(),
                            "host": getfqdn(),
                            "remote_addr": "not applicable",
                            "remote_login": "not applicable",
                            "code_op": "other audit operations",
                            "app_id": self.conf.kafka_app_id,
                            "type_id": "Audit",
                            "subtype_id": "F0",
                            "status_op": "SUCCESS",
                            "extras": {
                                "PARAMS": dict(EVENT_TYPE=f"S3 Ozone sync to file system through decode by Fernet", ),
                                "SESSION_ID": "not applicable",
                                "USER": "S3 bucket",
                            },
                        }
                        try:
                            client.download_file(bucket_name, upload_file_key, temp_file_path) # внимание! s3_client.download_file не создаст каталог.
                            # Его можно создать как pathlib.Path('/path/to/file.txt').parent.mkdir(parents=True, exist_ok=True).
                            with open(temp_file_path, "rb") as f:
                                vv = f.read()
                                self.log.info(f"240402_1843-01 бинарное шифрованное содержимое файла {temp_file_path=}, только что пришедшего из S3 бакета: \n{vv[0:200]=}")
                            with open(file_path, "wb") as f:
                                v = decrypt(vv)
                                self.log.info(f"240402_1843-02 расшифрованное содержимое файла {temp_file_path=} из S3 бакета текстовое (после decrypt),"
                                              f" которое записываем в файл {file_path=}: \n{v[0:200].decode('utf-8')=}")
                                f.write(v)
                            aud_msg["extras"]["PARAMS"].update(dict(S3_OPERATION="sync S3 to FS",
                                                                    OZONE_OBJ_NAME=upload_file_key,
                                                                    FS_FILE_NAME=file_path,
                                                                    ))
                            msg = self.audit_action_add(**aud_msg)
                            self.log.info(f"audit recorded: {msg=}")
                        except ClientError as ce:
                            self.log.info(f"240402_1513-01, исключение ClientError = искали объект {s3_file} корзины {SE_AWS_BUCKET_DIR} "
                                          f"среди файлов {fs_files}\n текст ошибки: {ce=}")
                        except Exception as e:
                            self.log.info(f"240402_1513-02, Exception {e=}")
                        self.log.info(f"240409_1010-02 ======== end - if s3_file not in fs_files =======================")
            time.sleep(sync_interval)

    @staticmethod
    def audit_action_add(host: str,
                         remote_addr: str,
                         remote_login: str,
                         code_op: str,
                         app_id: str,
                         type_id: str,
                         subtype_id: str,
                         status_op: str,
                         extras: dict,
                         ts: datetime.datetime.now(),
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


s3 = boto3.resource(service_name="s3",
                    aws_secret_access_key=SE_AWS_SECRET_KEY,
                    aws_access_key_id=SE_AWS_ACCESS_ID,
                    endpoint_url=":".join([SE_AWS_URL, SE_AWS_PORT])
                    )
# my_bucket = s3.Bucket(s3_bucket_name)
client = boto3.client(service_name="s3",
                     aws_secret_access_key=SE_AWS_SECRET_KEY,
                      aws_access_key_id=SE_AWS_ACCESS_ID,
                      endpoint_url=":".join([SE_AWS_URL, SE_AWS_PORT])
                      ) # https://habr.com/ru/articles/748276/

def dags_folder() -> str:
    # log.info(f"{conf=},\n{conf.__dict__=}")
    # DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')  # DAGS_FOLDER = f"{AIRFLOW_HOME}/dags"
    DAGS_FOLDER = os.path.expanduser(conf.get("core", "DAGS_FOLDER"))
    return DAGS_FOLDER

def sync_s3_to_fs_experiments(bucket_name: str = SE_AWS_BUCKET_NAME, dag_dir: str = dags_folder()):
    fs_files = set(os.listdir(dag_dir))
    for t in os.listdir(dag_dir):
        file_edit_time = os.path.getmtime(os.path.join(dag_dir, t))
        file_edit_time = datetime.datetime.fromtimestamp(file_edit_time)
        # log.info(f"{t=}, {file_edit_time=}")
    # log.info("#------------------------получение списка объектов через boto3.resource")
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        pass #log.info(f'240403_1746-00 {obj.key=}')
    # log.info("#------------------------")
    # log.info("#------------------------получение списка объектов через boto3.client")
    content = client.list_objects(Bucket=bucket_name) #s3.list_objects_v2(Bucket=bucket_name)
    for content_list_member in content['Contents']:
        # log.info(content_list_member['Key'])
        if match(f'^{SE_AWS_BUCKET_DIR}/',content_list_member['Key']):
            # log.info(f"объект {content_list_member['Key']} корзины {SE_AWS_BUCKET_DIR} среди файлов {fs_files}")
            obj_list_member = subn(f'{SE_AWS_BUCKET_DIR}/', '', content_list_member['Key'], 1)[0]
            # log.info(obj_list_member)
    # log.info("#------------------------")
    for content_list_member in client.list_objects(Bucket=bucket_name)['Contents']:
        s3_file = subn(f'{SE_AWS_BUCKET_DIR}/', '', content_list_member['Key'], 1)[0]
        file_path = os.path.join(dag_dir, s3_file)
        if os.path.isfile(file_path):
            name_, _ext = os.path.splitext(s3_file)
            time_info = [time.ctime(fn(file_path)) for fn in (os.path.getatime, os.path.getmtime, os.path.getctime)]
            #file = {'каталог': dag_dir,'файл': file_path,'файл_имя': name_,'файл_расширение': _ext,'время последнего доступа': time_info[0],'время последнего изменения': time_info[1],'время создания': time_info[2],}
            # log.info('\n'.join('{:<30} : {}'.format(*f) for f in sorted(file.items())), '\n')
        if s3_file not in fs_files:
            #bucket.download_file(s3_file, os.path.join(dag_dir, s3_file))  # bucket, key, filename
            # Если в бакете есть папка SE_AWS_BUCKET_DIR, файлы будут загружены туда, в противном случае можно оставить только str(file)
            upload_file_key = "".join([SE_AWS_BUCKET_DIR, "/", s3_file]) #"airflow_dev_003/" + str(file)
            # log.info(f"{upload_file_key=}")
            # log.info("выгружаем из S3 в FS")
            try:
                client.download_file(bucket_name, upload_file_key, file_path) # внимание! s3_client.download_file не создаст каталог. Его можно создать как pathlib.Path('/path/to/file.txt').parent.mkdir(parents=True, exist_ok=True).
            except ClientError as ce:
                log.info(f"240402_1748, исключение ClientError = не найден объект корзины {s3_file} среди файлов {fs_files}\n текст ошибки: {ce=}")
            except Exception as e:
                log.info(f"240402_1748-01 Exception {e=}")
        else:
            log.info(f"240402_1748-02, объект {s3_file} корзины {SE_AWS_BUCKET_DIR} уже есть среди файлов FS {fs_files}")

if __name__ == "__main__":
    #run(sync_interval=60, "1 args", "2 args", 3_kwargs_key="3 kwargs values", 4_kwargs_key="4 kwargs values")
    sync_s3_to_fs_experiments(bucket_name=SE_AWS_BUCKET_NAME, dag_dir=dags_folder(), sync_interval=5)

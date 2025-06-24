"""
Процесс обновления тикетов
"""
# отключение лишних сообщений в логах (из подключаемых библиотек) о неподдерживаемых функциях и т.д.
from warnings import simplefilter as warnings_simplefilter
warnings_simplefilter("ignore")

from os import getpid, chmod
from typing import Optional
from argparse import ArgumentParser, Namespace
from time import sleep
from datetime import datetime

from airflow_se.logger import LoggingMixinSE
from airflow_se.commons import SECMAN_KEY_FOR_TGT, EMPTY_KEY_SECMAN
from airflow_se.crypt import encrypt, decrypt
from airflow_se.utils import DataPaths
from airflow_se.settings import Settings, get_settings
from airflow_se.utils import run_kinit, run_kvno, run_klist, timedelta_to_human_format
from airflow_se.secman import auth_secman, get_secman_data, push_secman_data
from airflow_se.db import TGSList

__all__ = ["run", ]

def run():
    parser = ArgumentParser(description="Airflow SE ticket renewer")
    parser.add_argument("se_ticketman", type=str, help="se_ticketman")
    parser.add_argument("--pid", dest="pid", type=str, required=False, help="ProcessID file name")
    return TicketMan(parser.parse_args())()


class TicketMan(LoggingMixinSE):
    """
    Обновление тикетов
    """
    newln = '\n'
    hr = "*" * 80

    def __call__(self, *args, **kwargs):
        """This is a callable object"""
        self.run(*args, **kwargs)

    def __init__(self, cla: Namespace):
        """Constructor"""
        self.__settings: Settings = get_settings()
        super().__init__(self.conf.debug)
        self.log_info(self.hr)
        self.__cla = cla  # cla - Command Line Arguments
        cla_pid = self.__cla.pid if hasattr(self.__cla, "pid") and self.__cla.pid else None
        self.log_info(f"""Application is started. ProcessID file name: """
                      f"""{cla_pid if cla_pid else "PID file is not use"}""")
        try:
            if cla_pid:
                with open(cla_pid, "w") as f:
                    f.write(str(getpid()))
        except Exception as e:
            self.log_error(f"PID file write error: {e}")
        self.__app_start = datetime.now()
        self.__counter: int = 0
        self.log_debug(f"{self.proc_timeout=}; {self.proc_retry=}")
        self.log_info(self.hr)

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
    def proc_timeout(self) -> int:
        """Таймаут процесса (на сколько секунд он будет "засыпать")"""
        return self.conf.ticketman_process_timeout
        # return 60

    @property
    def proc_retry(self) -> Optional[int]:
        """Количество повторений цикла, None - бесконечный цикл"""
        return self.conf.ticketman_process_retry

    def run(self, *args, **kwargs):
        """Бесконечный цикл приложения"""
        _, _, hr = args, kwargs, '=' * 60
        while True:
            try:
                self.counter += 1
                self.log_info(f"""========  Iteration {self.counter} of """
                              f"""{self.proc_retry if self.proc_retry else "infinity"}.  ========""")
                days, hours, minutes, seconds = timedelta_to_human_format(datetime.now() - self.app_start)
                self.log_info(f"Continuous operation time of the application: "
                              f"{days} days, {hours:02}:{minutes:02}:{seconds:02}")
                # проверка на количество итераций
                if isinstance(self.proc_retry, int) and (self.counter > self.proc_retry):
                    self.log_info(f"Number of iterations ({self.proc_retry}) completed. Application closed.")
                    break
            except Exception:
                self.log_exception('')

            try:
                _token = auth_secman()
                if not _token:
                    raise RuntimeError('Missing authentication to SecMan')
            except Exception:
                self.log_exception('')
            else:
                sm_tgt_return = dict()
                try:
                    dp: DataPaths = DataPaths(
                        base_path=self.conf.secret_path,
                        inner_dir="se_ticketman",
                        name="TicketManSE",
                    )
                    sm_tgt = get_secman_data(SECMAN_KEY_FOR_TGT, _token)
                    if not isinstance(sm_tgt, dict):
                        self.log_warning("SecMan don't return tickets (empty secret or not exists)")
                        sm_tgt = dict()

                    for k, v in sm_tgt.items():
                        if k == EMPTY_KEY_SECMAN:
                            continue
                        self.log_info(hr)
                        full_path = dp.get(k)
                        self.log_info(f'Ticket "{k}" processing...')
                        try:
                            body = decrypt(v)
                        except Exception:
                            self.log_error(f'Don\'t encrypt body ticket "{k}"')
                            self.log_exception('')
                            continue
                        else:
                            try:
                                with open(full_path, "wb") as f:
                                    f.write(body)
                                chmod(full_path, 0o600)
                            except Exception:
                                self.log_error(f'Don\'t write body ticket "{k}" to file "{full_path}"')
                                self.log_exception('')
                                continue

                        ret = run_klist(ticket=full_path, klist=self.conf.krb_klist_path)
                        ret_msg = f"(before) KLIST return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ]" \
                                  f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                                  f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
                        if ret.get("returncode") == 0:
                            self.log_debug(ret_msg)
                        else:
                            self.log_error(ret_msg)

                        ret = run_kinit(ticket=full_path, kinit=self.conf.krb_kinit_path, renew=True, verbose=True)
                        ret_msg = f"KINIT return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ]" \
                                  f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                                  f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
                        if ret.get("returncode") == 0:
                            self.log_debug(ret_msg)
                            # получение TGS по списку (если не получается, то ошибку пишем только в лог, процесс не должен прерываться)
                            for tgs in TGSList.get_tgs_list():
                                ret = run_kvno(ticket=full_path, service=tgs)
                                ret_msg = f"KVNO return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ]" \
                                          f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                                          f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
                                if ret.get("returncode") == 0:
                                    self.log_debug(ret_msg)
                                else:
                                    self.log_error(ret_msg)
                                    self.log_error(f'Missing get TGS "{tgs}" for ticket "{k}"')
                                    TGSList.del_tgs(tgs)
                            self.log_info(f"Ticket \"{k}\" has bin renewed")
                            try:
                                with open(full_path, "rb") as f:
                                    tgt_enc = encrypt(f.read())
                                    sm_tgt_return[k] = tgt_enc
                            except Exception:
                                self.log_exception(f"Exception on read file \"{full_path}\"")
                                self.log_error(f"Ticket \"{k}\" NOT add to SecMan pushing data")
                            else:
                                self.log_info(f"Ticket \"{k}\" add to SecMan pushing data")
                        else:
                            self.log_error(ret_msg)
                            self.log_error(f"Ticket \"{k}\" NOT renewed and NOT add to SecMan pushing data")

                        ret = run_klist(ticket=full_path, klist=self.conf.krb_klist_path)
                        ret_msg = f"(after) KLIST return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ]" \
                                  f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                                  f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
                        if ret.get("returncode") == 0:
                            self.log_debug(ret_msg)
                        else:
                            self.log_error(ret_msg)
                except Exception:
                    self.log_exception('')

                self.log_info(hr)
                self.log_info(f'Start pushing SecMan data')
                if len(sm_tgt_return) > 0:
                    if push_secman_data(SECMAN_KEY_FOR_TGT, sm_tgt_return, _token):
                        self.log_info("Tickets being pushed to SecMan")
                    else:
                        self.log_error("Tickets don't pushed to SecMan")
                else:
                    push_secman_data(SECMAN_KEY_FOR_TGT, {EMPTY_KEY_SECMAN: ""}, _token)
                    self.log_warning("No tickets for push to SecMan, push empty key")

                del dp

            self.log_info(hr)
            ths, tms, tss = self.proc_timeout // 3600, (self.proc_timeout % 3600) // 60, self.proc_timeout % 60
            self.log_info(f"Sleep at time {ths:02}:{tms:02}:{tss:02}")
            self.log_info("<< ... ... ... ... ...  sweet dream  ... ... ... ... ... >>")
            sleep(self.proc_timeout)

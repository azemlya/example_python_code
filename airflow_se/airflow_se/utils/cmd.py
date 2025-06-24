
"""
Модуль для работы с командной строкой.
"""
from os import environ
from shlex import split
from subprocess import Popen, PIPE
from typing import Optional, Union, Iterable, Mapping, Dict, List, Any

__all__ = [
    "run_command",
]

def run_command(
        cmd: Union[Iterable[str], str],
        comm: Optional[str] = None,
        envs: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    """
    _cmd: List[str] = list(cmd) if isinstance(cmd, (list, tuple, set)) else split(cmd)
    _envs: Dict[str, str] = dict(**environ)
    if isinstance(envs, Mapping):
        _envs.update(envs)
    ret: Dict[str, Any] = dict(command=_cmd, environments=_envs)
    with Popen(
            _cmd,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            bufsize=-1,
            env=_envs,
            close_fds=True,
            universal_newlines=True,
            encoding="utf-8",
            text=True,
            # start_new_session=True,
    ) as prc:
        try:
            out: Optional[str]
            err: Optional[str]
            if isinstance(comm, str):
                out, err = prc.communicate(comm, timeout=timeout)
            else:
                _ = prc.wait(timeout=timeout)
                out, err = prc.stdout.read() if prc.stdout else None, prc.stderr.read() if prc.stderr else None
            ret.update(stdout=out, stderr=err, returncode=prc.returncode)
        except Exception as e:
            ret.update(exception=f"{e}")
    return ret


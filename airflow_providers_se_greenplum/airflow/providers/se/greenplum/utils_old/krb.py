"""
Модуль для работы с утилитами kinit и klist через командную строку.
Только так. Других вариантов нет. API интерфейса не существует ¯\_(ツ)_/¯
"""
from typing import Optional, Mapping, Dict, List, Any
from .cmd import run_command

__all__ = ["run_kinit", "run_klist", ]

def run_kinit(
        userid: Optional[str] = None,
        realm: Optional[str] = None,
        pwd: Optional[str] = None,
        keytab: Optional[str] = None,
        ticket: Optional[str] = None,
        renewable: Optional[str] = None,
        lifetime: Optional[str] = None,
        forwardable: Optional[bool] = None,
        include_ip: Optional[bool] = None,
        envs: Optional[Mapping[str, str]] = None,
        kinit: Optional[str] = None,
        renew: bool = False,
        verbose: bool = False,
        timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    """
    _cmd: List[str] = [kinit if isinstance(kinit, str) and not kinit.isspace() else "kinit", ]
    _verbose: Optional[List[str]] = ["-V", ] if verbose is True else None
    _lifetime: Optional[List[str]] = ["-l", lifetime, ] if (isinstance(lifetime, str) and
                                                            not lifetime.isspace()
                                                            ) else None
    _renewable: Optional[List[str]] = ["-r", renewable, ] if (isinstance(renewable, str) and
                                                              not renewable.isspace()
                                                              ) else None
    _forwardable: Optional[List[str]] = ["-f", ] if forwardable is True else ["-F", ] if forwardable is False else None
    _include_ip: Optional[List[str]] = ["-a", ] if include_ip is True else ["-A", ] if include_ip is False else None
    _keytab: Optional[List[str]] = ["-k", "-t", keytab, ] if isinstance(keytab, str) and not keytab.isspace() else None
    _ticket: Optional[List[str]] = ["-c", ticket, ] if isinstance(ticket, str) and not ticket.isspace() else None
    _principal: Optional[List[str]] = [f"{userid}@{realm}", ] if (isinstance(userid, str) and not userid.isspace() and
                                                                  isinstance(realm, str) and not realm.isspace()
                                                                  ) else None
    if _verbose:
        _cmd.extend(_verbose)
    if _lifetime:
        _cmd.extend(_lifetime)
    if _renewable:
        _cmd.extend(_renewable)
    if _forwardable:
        _cmd.extend(_forwardable)
    if _include_ip:
        _cmd.extend(_include_ip)
    if renew is True:
        _cmd.append("-R")
        if _ticket:
            _cmd.extend(_ticket)
        if _principal:
            _cmd.extend(_principal)
        return run_command(cmd=_cmd, envs=envs, timeout=timeout)
    if _keytab:
        _cmd.extend(_keytab)
    if _ticket:
        _cmd.extend(_ticket)
    if _principal:
        _cmd.extend(_principal)
    return run_command(cmd=_cmd, comm=pwd, envs=envs, timeout=timeout)

def run_klist(
        ticket: Optional[str] = None,
        envs: Optional[Mapping[str, str]] = None,
        klist: Optional[str] = None,
        timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    """
    _cmd: List[str] = [klist if isinstance(klist, str) and not klist.isspace() else "klist", ]
    _ticket: Optional[List[str]] = ["-c", ticket, ] if isinstance(ticket, str) and not ticket.isspace() else None
    if _ticket:
        _cmd.extend(_ticket)
    return run_command(cmd=_cmd, envs=envs, timeout=timeout)


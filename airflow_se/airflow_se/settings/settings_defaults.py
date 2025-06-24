
"""
Дефолтные значения параметров
"""
from datetime import time, timedelta

__all__ = [
    "DEF_USER_REGISTRATION",
    "DEF_USER_REGISTRATION_ROLE",
    "DEF_ROLES_SYNC_AT_LOGIN",
    "DEF_KRB_KINIT_PATH",
    "DEF_KRB_KLIST_PATH",
    "DEF_KRB_TICKET_LIFETIME",
    "DEF_KRB_RENEW_UNTIL",
    "DEF_PAM_POLICY",
    "DEF_LDAP_USE_TLS",
    "DEF_LDAP_ALLOW_SELF_SIGNED",
    "DEF_LDAP_TLS_DEMAND",
    "DEF_LDAP_FIRSTNAME_FIELD",
    "DEF_LDAP_LASTNAME_FIELD",
    "DEF_LDAP_EMAIL_FIELD",
    "DEF_LDAP_UID_FIELD",
    "DEF_LDAP_BIND_USER_PATH_KRB5CC",
    "DEF_LDAP_GROUP_FIELD",
    "DEF_LDAP_REFRESH_CACHE_ON_START",
    "DEF_LDAP_PROCESS_TIMEOUT",
    "DEF_LDAP_PROCESS_RETRY",
    "DEF_LDAP_TIME_START",
    "DEF_LDAP_TIMEDELTA_UP",
    "DEF_LDAP_TIMEDELTA_DOWN",
    "DEF_LDAP_DELETE_CACHES_DAYS_AGO",
    "DEF_KAFKA_PROCESS_TIMEOUT",
    "DEF_KAFKA_PROCESS_RETRY",
    "DEF_KAFKA_PAGE_SIZE",
    "DEF_TICKETMAN_SCAN_DIRS",
    "DEF_TICKETMAN_PROCESS_TIMEOUT",
    "DEF_TICKETMAN_PROCESS_RETRY",
    "DEF_DEBUG",
    "DEF_DEBUG_MASK",
    "DEF_BLOCK_CHANGE_POLICY",
    "DEF_PAM_IS_AUTH",
    "DEF_LDAP_CLEAR_CACHES",
    "DEF_LDAP_IS_AUTH",
    "DEF_LDAP_IS_AUTH_DEF_ROLES",
]

#################################################################################################
###                                 Настройки общие                                           ###
#################################################################################################

DEF_USER_REGISTRATION = True
DEF_USER_REGISTRATION_ROLE = "Public"
DEF_ROLES_SYNC_AT_LOGIN = True

#################################################################################################
###                                 Настройки Kerberos                                        ###
#################################################################################################

DEF_KRB_KINIT_PATH = "kinit"
DEF_KRB_KLIST_PATH = "klist"

DEF_KRB_TICKET_LIFETIME = "8h"
DEF_KRB_RENEW_UNTIL = "7d"

#################################################################################################
###                                 Настройки PAM                                             ###
#################################################################################################

DEF_PAM_POLICY = "airflow"

#################################################################################################
###                                 Настройки LDAP                                            ###
#################################################################################################

DEF_LDAP_USE_TLS = True
DEF_LDAP_ALLOW_SELF_SIGNED = True
DEF_LDAP_TLS_DEMAND = False
DEF_LDAP_FIRSTNAME_FIELD = "givenName"
DEF_LDAP_LASTNAME_FIELD = "sn"
DEF_LDAP_EMAIL_FIELD = "mail"
DEF_LDAP_UID_FIELD = "uid"
DEF_LDAP_BIND_USER_PATH_KRB5CC = "/tmp/airflow_spn_ccache"
DEF_LDAP_GROUP_FIELD = "memberOf"

DEF_LDAP_REFRESH_CACHE_ON_START = False
DEF_LDAP_PROCESS_TIMEOUT = 600
DEF_LDAP_PROCESS_RETRY = None
DEF_LDAP_TIME_START = time(hour=2)
DEF_LDAP_TIMEDELTA_UP = timedelta(days=1, hours=2)
DEF_LDAP_TIMEDELTA_DOWN = timedelta(hours=6)
DEF_LDAP_DELETE_CACHES_DAYS_AGO = timedelta(days=30)

#################################################################################################
###                                 Настройки Kafka                                           ###
#################################################################################################

DEF_KAFKA_PROCESS_TIMEOUT = 300
DEF_KAFKA_PROCESS_RETRY = None
DEF_KAFKA_PAGE_SIZE = 1000

#################################################################################################
###                                 Настройки TicketMan                                       ###
#################################################################################################

DEF_TICKETMAN_SCAN_DIRS = None
DEF_TICKETMAN_PROCESS_TIMEOUT = 25200
DEF_TICKETMAN_PROCESS_RETRY = None

#################################################################################################
###                                 Общие параметры.                                          ###
#################################################################################################

DEF_DEBUG = False
DEF_DEBUG_MASK = "DEBUG >> {}"
DEF_BLOCK_CHANGE_POLICY = True
DEF_PAM_IS_AUTH = False
DEF_LDAP_CLEAR_CACHES = False
DEF_LDAP_IS_AUTH = True
DEF_LDAP_IS_AUTH_DEF_ROLES = {"Admin", }


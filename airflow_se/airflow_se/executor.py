
"""
Запускатор, наша точка входа
"""

print(f"\n{'*'*80}\n=== Start Airflow SE ===\n{'*'*80}")

from sys import argv
from os import environ, chmod
from re import sub
from json import loads, dumps

from .commons import CLEAR_MASK_FOR_AIRFLOW_SE, SECMAN_KEY_FOR_SECRET
from .crypt import decrypt
from .utils import show_logo, DataPaths

se: str
try:
    se = environ.get("AIRFLOW_SE")
    if not se:
        raise RuntimeError("Environment variable `AIRFLOW_SE` is empty")
    se = sub(CLEAR_MASK_FOR_AIRFLOW_SE, "", se)
except:
    print(f"ERROR: DON'T set environment variable `AIRFLOW_SE`\n{'*'*80}")
    raise

try:
    environ.update(loads(decrypt(se).decode('utf-8')))
except:
    print(f"ERROR: DON'T convert value from environment variable `AIRFLOW_SE`\n{'*'*80}")
    raise

from airflow_se.secman import get_secman_data
from airflow_se.config import get_config_value

__all__ = ["run", ]

secret_path = get_config_value("SECRET_PATH")
if not secret_path or not isinstance(secret_path, str) or secret_path.isspace():
    secret_path = '/tmp'
    environ['SE_SECRET_PATH'] = secret_path
    print('Configuration parameter "SECRET_PATH" don\'t set or invalid, value set equal to "/tmp"')

def run():
    try:
        dp = DataPaths(base_path=secret_path, name="ExecutorSE")
        sm_data = get_secman_data(SECMAN_KEY_FOR_SECRET)
        if sm_data:
            print("=== Data from SecMan has been successfully downloaded ===")

            ### Checking block
            sm_data_keys = sm_data.keys()
            # Metadata DB URI
            if "SE_DB_METADATA_PG_USERPASS" not in sm_data_keys:
                raise RuntimeError("Key `SE_DB_METADATA_PG_USERPASS` is not found in SecMan data")
            # if "SE_DB_METADATA_PG_SSLROOTCERT" not in sm_data_keys:
            #     raise RuntimeError("Key `SE_DB_METADATA_PG_SSLROOTCERT` is not found in SecMan data")
            # if "SE_DB_METADATA_PG_SSLCERT" not in sm_data_keys:
            #     raise RuntimeError("Key `SE_DB_METADATA_PG_SSLCERT` is not found in SecMan data")
            # if "SE_DB_METADATA_PG_SSLKEY" not in sm_data_keys:
            #     raise RuntimeError("Key `SE_DB_METADATA_PG_SSLKEY` is not found in SecMan data")
            # Kafka
            # if "SE_KAFKA_SSL_CA_CERT" not in sm_data_keys or "SE_KAFKA_SSL_PUBLISHED_CERT" not in sm_data_keys or \
            #         "SE_KAFKA_SSL_PRIVATE_KEY" not in sm_data_keys:
            #     if "SE_KAFKA_SSL_KEYSTORE_PKCS12" not in sm_data_keys or \
            #             "SE_KAFKA_SSL_KEYSTORE_PASS" not in sm_data_keys:
            #         raise RuntimeError("Certificates for Kafka is not found in SecMan data")
            # SPN
            if "SE_AIRFLOW_SPN_KEYTAB" not in sm_data_keys:
                raise RuntimeError("Key `SE_AIRFLOW_SPN_KEYTAB` is not found in SecMan data")
            # LDAP
            if "SE_LDAP_TUZ_KEYTAB" not in sm_data_keys:
                raise RuntimeError("Key `SE_LDAP_TUZ_KEYTAB` is not found in SecMan data")
            # Webserver certs
            # if "SE_WEB_SERVER_SSL_CERT" not in sm_data_keys:
            #     raise RuntimeError("Key `SE_WEB_SERVER_SSL_CERT` is not found in SecMan data")
            # if "SE_WEB_SERVER_SSL_KEY" not in sm_data_keys:
            #     raise RuntimeError("Key `SE_WEB_SERVER_SSL_KEY` is not found in SecMan data")
            # Secret Keys
            if "AIRFLOW__WEBSERVER__SECRET_KEY" not in sm_data_keys:
                raise RuntimeError("Key `AIRFLOW__WEBSERVER__SECRET_KEY` is not found in SecMan data")
            if "AIRFLOW__CORE__FERNET_KEY" not in sm_data_keys:
                raise RuntimeError("Key `AIRFLOW__CORE__FERNET_KEY` is not found in SecMan data")

            # Metadata DB URI
            param = get_config_value('SE_DB_METADATA_PG_PARAMS') or \
                    '?connect_timeout=10&application_name=airflow_se&require_auth=scram-sha-256&gssencmode=disable'
            if "SE_DB_METADATA_PG_SSLROOTCERT" in sm_data_keys and "SE_DB_METADATA_PG_SSLCERT" in sm_data_keys and \
                    "SE_DB_METADATA_PG_SSLKEY" in sm_data_keys:
                with open(dp.meta_db_sslrootcert_path, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_DB_METADATA_PG_SSLROOTCERT")))
                chmod(dp.meta_db_sslrootcert_path, 0o600)
                environ["SE_DB_METADATA_PG_SSLROOTCERT"] = dp.meta_db_sslrootcert_path
                with open(dp.meta_db_sslcert_path, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_DB_METADATA_PG_SSLCERT")))
                chmod(dp.meta_db_sslcert_path, 0o600)
                environ["SE_DB_METADATA_PG_SSLCERT"] = dp.meta_db_sslcert_path
                with open(dp.meta_db_sslkey_path, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_DB_METADATA_PG_SSLKEY")))
                chmod(dp.meta_db_sslkey_path, 0o600)
                environ["SE_DB_METADATA_PG_SSLKEY"] = dp.meta_db_sslkey_path
                param += f"&sslmode=verify-full&sslrootcert={dp.meta_db_sslrootcert_path}" \
                         f"&sslcert={dp.meta_db_sslcert_path}&sslkey={dp.meta_db_sslkey_path}"
            else:
                param += "&sslmode=disable"
                print("Warning!!! Metadata DB mTLS: Off (certificates not present to SecMan)")
            environ["SE_DB_METADATA_PG_USERPASS"] = decrypt(sm_data.get('SE_DB_METADATA_PG_USERPASS')).decode('utf-8')
            environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"postgresql+psycopg2://" \
                f"{get_config_value('DB_METADATA_PG_USERNAME')}:" \
                f"{get_config_value('DB_METADATA_PG_USERPASS')}" \
                f"@{get_config_value('DB_METADATA_PG_HOST')}:{get_config_value('DB_METADATA_PG_PORT')}" \
                f"/{get_config_value('DB_METADATA_PG_DBNAME')}{param}"
            environ["AIRFLOW__DATABASE__SQL_ALCHEMY_SCHEMA"] = get_config_value("DB_METADATA_PG_SCHEMA")
            # https://www.postgresql.org/docs/current/libpq-envars.html
            # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-REQUIRE-AUTH
            # environ["PGREQUIREAUTH"] = "scram-sha-256"
            print("Metadata DB URI: Ok")

            # Kafka
            _kafka_producer_settings = get_config_value("KAFKA_PRODUCER_SETTINGS")
            if _kafka_producer_settings and isinstance(_kafka_producer_settings, str):
                try:
                    _kafka_producer_settings = loads(_kafka_producer_settings.strip())
                except:
                    _kafka_producer_settings = loads(sub(r"\s", "", _kafka_producer_settings))
            if not isinstance(_kafka_producer_settings, dict) or not _kafka_producer_settings.get("bootstrap.servers"):
                raise RuntimeError("Invalid parameter `SE_KAFKA_PRODUCER_SETTINGS`")
            _kafka_producer_settings["ssl.endpoint.identification.algorithm"] = "none"
            if sm_data.get("SE_KAFKA_SSL_CA_CERT") and sm_data.get("SE_KAFKA_SSL_PUBLISHED_CERT") and \
                    sm_data.get("SE_KAFKA_SSL_PRIVATE_KEY"):
                with open(dp.kafka_ssl_ca_cert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_KAFKA_SSL_CA_CERT")))
                chmod(dp.kafka_ssl_ca_cert, 0o600)
                with open(dp.kafka_ssl_published_cert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_KAFKA_SSL_PUBLISHED_CERT")))
                chmod(dp.kafka_ssl_published_cert, 0o600)
                with open(dp.kafka_ssl_private_key, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_KAFKA_SSL_PRIVATE_KEY")))
                chmod(dp.kafka_ssl_private_key, 0o600)
                _kafka_producer_settings["security.protocol"] = "SSL"
                _kafka_producer_settings["ssl.ca.location"] = dp.kafka_ssl_ca_cert
                _kafka_producer_settings["ssl.certificate.location"] = dp.kafka_ssl_published_cert
                _kafka_producer_settings["ssl.key.location"] = dp.kafka_ssl_private_key
                if "ssl.keystore.location" in _kafka_producer_settings.keys():
                    _kafka_producer_settings.pop("ssl.keystore.location")
                if "ssl.keystore.password" in _kafka_producer_settings.keys():
                    _kafka_producer_settings.pop("ssl.keystore.password")
            if sm_data.get("SE_KAFKA_SSL_KEYSTORE_PKCS12") and sm_data.get("SE_KAFKA_SSL_KEYSTORE_PASS"):
                with open(dp.kafka_ssl_keystore_pkcs12, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_KAFKA_SSL_KEYSTORE_PKCS12")))
                chmod(dp.kafka_ssl_keystore_pkcs12, 0o600)
                _kafka_producer_settings["security.protocol"] = "SSL"
                _kafka_producer_settings["ssl.keystore.location"] = dp.kafka_ssl_keystore_pkcs12
                _kafka_producer_settings["ssl.keystore.password"] = \
                    decrypt(sm_data.get("SE_KAFKA_SSL_KEYSTORE_PASS")).decode('utf-8')
                if "ssl.ca.location" in _kafka_producer_settings.keys():
                    _kafka_producer_settings.pop("ssl.ca.location")
                if "ssl.certificate.location" in _kafka_producer_settings.keys():
                    _kafka_producer_settings.pop("ssl.certificate.location")
                if "ssl.key.location" in _kafka_producer_settings.keys():
                    _kafka_producer_settings.pop("ssl.key.location")
            # else:
            #     raise RuntimeError("Unreachable: Certificates for Kafka is not found in SecMan data")
            environ["SE_KAFKA_PRODUCER_SETTINGS"] = dumps(_kafka_producer_settings)
            print("Kafka secret: Ok")

            # Airflow SPN
            with open(dp.airflow_spn_keytab, "wb") as f:
                f.write(decrypt(sm_data.get("SE_AIRFLOW_SPN_KEYTAB")))
            chmod(dp.airflow_spn_keytab, 0o600)
            # по всей видимости, из-за этого параметра происходит DDOS IPA (highly likely)
            # environ["AIRFLOW__CORE__SECURITY"] = "kerberos"
            environ["AIRFLOW__KERBEROS__KEYTAB"] = dp.airflow_spn_keytab
            environ["KRB5_KTNAME"] = dp.airflow_spn_keytab
            environ["AIRFLOW__KERBEROS__CCACHE"] = dp.airflow_spn_ccache
            environ["KRB5CCNAME"] = dp.airflow_spn_ccache
            print("Airflow SPN: Ok")

            # LDAP TUZ
            with open(dp.ldap_tuz_keytab, "wb") as f:
                f.write(decrypt(sm_data.get("SE_LDAP_TUZ_KEYTAB")))
            chmod(dp.ldap_tuz_keytab, 0o600)
            environ["SE_LDAP_BIND_USER_KEYTAB"] = dp.ldap_tuz_keytab
            environ["SE_LDAP_BIND_USER_PATH_KRB5CC"] = dp.ldap_tuz_ccache
            print("LDAP TUZ: Ok")

            # Webserver certs
            if sm_data.get("SE_WEB_SERVER_SSL_CERT") and sm_data.get("SE_WEB_SERVER_SSL_KEY"):
                with open(dp.web_server_ssl_cert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_WEB_SERVER_SSL_CERT")))
                chmod(dp.web_server_ssl_cert, 0o600)
                environ["AIRFLOW__WEBSERVER__WEB_SERVER_SSL_CERT"] = dp.web_server_ssl_cert
                with open(dp.web_server_ssl_key, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_WEB_SERVER_SSL_KEY")))
                chmod(dp.web_server_ssl_key, 0o600)
                environ["AIRFLOW__WEBSERVER__WEB_SERVER_SSL_KEY"] = dp.web_server_ssl_key
                print("Webserver certs: Ok")
            else:
                print("Webserver certs: No present to SecMan")

            # параметры и сертификаты для gunicorn-а
            if sm_data.get("SE_GUNICORN_SSL_KEY") and sm_data.get("SE_GUNICORN_SSL_CERT") and \
                    sm_data.get("SE_GUNICORN_SSL_CA_CERTS"):
                with open(dp.gunicorn_ssl_key, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_GUNICORN_SSL_KEY")))
                chmod(dp.gunicorn_ssl_key, 0o600)
                environ["SE_GUNICORN_SSL_KEY"] = dp.gunicorn_ssl_key
                with open(dp.gunicorn_ssl_cert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_GUNICORN_SSL_CERT")))
                chmod(dp.gunicorn_ssl_cert, 0o600)
                environ["SE_GUNICORN_SSL_CERT"] = dp.gunicorn_ssl_cert
                gunicorn_cmd_args = list()
                # from os.path import join, dirname
                # gunicorn_cmd_args.append(f'--config python:{join(dirname(__file__), "gunicorn_conf.py")}')
                gunicorn_cmd_args.append(f'--config python:airflow_se.gunicorn_conf')
                gunicorn_cmd_args.extend([f'--keyfile "{dp.gunicorn_ssl_key}"', f'--certfile "{dp.gunicorn_ssl_cert}"', ])
                if sm_data.get("SE_GUNICORN_SSL_CA_CERTS"):
                    with open(dp.gunicorn_ssl_ca_certs, "wb") as f:
                        f.write(decrypt(sm_data.get("SE_GUNICORN_SSL_CA_CERTS")))
                    chmod(dp.gunicorn_ssl_ca_certs, 0o600)
                    environ["SE_GUNICORN_SSL_CA_CERTS"] = dp.gunicorn_ssl_ca_certs
                    gunicorn_cmd_args.append(f'--ca-certs "{dp.gunicorn_ssl_ca_certs}"')
                gunicorn_cmd_args.append('--cert-reqs 1')
                gunicorn_cmd_args.append('--do-handshake-on-connect')
                environ['GUNICORN_CMD_ARGS'] = ' '.join(gunicorn_cmd_args)
                # print(f'GUNICORN_CMD_ARGS = {environ.get("GUNICORN_CMD_ARGS")}')
                print("Gunicorn certs: Ok")
            else:
                print("Gunicorn certs: No present to SecMan")

            # Secret Keys
            environ["AIRFLOW__WEBSERVER__SECRET_KEY"] = \
                decrypt(sm_data.get("AIRFLOW__WEBSERVER__SECRET_KEY")).decode('utf-8')
            environ["AIRFLOW__CORE__FERNET_KEY"] = \
                decrypt(sm_data.get("AIRFLOW__CORE__FERNET_KEY")).decode('utf-8')
            print("Secret Keys: Ok")

            # дополнительные переменные среды окружения

            # для провайдера GP
            if "SE_PROVIDER_GP_SSLROOTCERT" in sm_data_keys and "SE_PROVIDER_GP_SSLCERT" in sm_data_keys \
                    and "SE_PROVIDER_GP_SSLKEY" in sm_data_keys:
                with open(dp.provider_gp_sslrootcert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_PROVIDER_GP_SSLROOTCERT")))
                chmod(dp.provider_gp_sslrootcert, 0o600)
                environ["SE_PROVIDER_GP_SSLROOTCERT"] = dp.provider_gp_sslrootcert
                with open(dp.provider_gp_sslcert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_PROVIDER_GP_SSLCERT")))
                chmod(dp.provider_gp_sslcert, 0o600)
                environ["SE_PROVIDER_GP_SSLCERT"] = dp.provider_gp_sslcert
                with open(dp.provider_gp_sslkey, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_PROVIDER_GP_SSLKEY")))
                chmod(dp.provider_gp_sslkey, 0o600)
                environ["SE_PROVIDER_GP_SSLKEY"] = dp.provider_gp_sslkey

            # для провайдера CTL
            if "SE_PROVIDER_CTL_SSLROOTCERT" in sm_data_keys:
                with open(dp.provider_ctl_sslrootcert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_PROVIDER_CTL_SSLROOTCERT")))
                chmod(dp.provider_ctl_sslrootcert, 0o600)
                environ["SE_PROVIDER_CTL_SSLROOTCERT"] = dp.provider_ctl_sslrootcert
            if "SE_PROVIDER_CTL_SSLCERT" in sm_data_keys:
                with open(dp.provider_ctl_sslcert, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_PROVIDER_CTL_SSLCERT")))
                chmod(dp.provider_ctl_sslcert, 0o600)
                environ["SE_PROVIDER_CTL_SSLCERT"] = dp.provider_ctl_sslcert
            if "SE_PROVIDER_CTL_SSLKEY" in sm_data_keys:
                with open(dp.provider_ctl_sslkey, "wb") as f:
                    f.write(decrypt(sm_data.get("SE_PROVIDER_CTL_SSLKEY")))
                chmod(dp.provider_ctl_sslkey, 0o600)
                environ["SE_PROVIDER_CTL_SSLKEY"] = dp.provider_ctl_sslkey

            # токен ТУЗа в Bitbucket
            if "SE_BITBUCKET_TUZ_TOKEN" in sm_data_keys:
                environ["SE_BITBUCKET_TUZ_TOKEN"] = decrypt(sm_data.get("SE_BITBUCKET_TUZ_TOKEN")).decode('utf-8')

            # SECRET_KEY в Озоне
            if "SE_AWS_SECRET_KEY" in sm_data_keys:
                environ["SE_AWS_SECRET_KEY"] = decrypt(sm_data.get("SE_AWS_SECRET_KEY")).decode('utf-8')

            print("=== SecMan data successfully loaded to environments variables ===")
        else:
            raise RuntimeError("Missing load SecMan data")

        # перекрытие некоторых важных переменных среды окружения
        environ['AIRFLOW__API__AUTH_BACKENDS'] = 'airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.kerberos_auth'
        environ['AIRFLOW__CLI__API_CLIENT'] = 'airflow.api.client.local_client'
        # наш XCom
        # environ['AIRFLOW__CORE__XCOM_BACKEND'] = 'airflow_se.xcom.XComSE'
        # ограничение на длину XCom
        environ['AIRFLOW__CORE__MAX_MAP_LENGTH'] = '3'

        if environ.get("HIDDEN_DEBUG") and environ.get("HIDDEN_DEBUG") == "True":
            print(f"{'*' * 80}")
            for k, v in environ.items():
                print(f"    {k} = {v}")
            print(f"{'*' * 80}")
            for k, v in dp.items():
                print(f"    {k} = {v}")
            print(f"{'*' * 80}")

        show_logo()

        cmd = argv[1] if len(argv) > 1 else None
        if cmd and cmd == "se_ldap":
            import airflow_se.proc_ldap as proc_ldap
            ret = proc_ldap.run()
            return ret
        elif cmd and cmd == "se_kafka":
            import airflow_se.proc_kafka as proc_kafka
            ret = proc_kafka.run()
            return ret
        elif cmd and cmd == "se_ticketman":
            import airflow_se.proc_ticketman as proc_ticketman
            ret = proc_ticketman.run()
            return ret
        elif cmd and cmd == "se_S3sinc":
            import airflow_se.proc_S3sinc as proc_S3sinc
            ret = proc_S3sinc.run()
            return ret
        elif cmd and cmd == "se_FSsinc":
            import airflow_se.proc_FSsinc as proc_FSsinc
            ret = proc_FSsinc.run()
            return ret
        elif cmd and cmd == "se_change_password_dbmd":
            from airflow_se.change_pass_dbmd import change_pass_dbmd
            ret = change_pass_dbmd()
            return ret

        import airflow.__main__ as airflow_main
        ret = airflow_main.main()
        return ret
    finally:
        del dp


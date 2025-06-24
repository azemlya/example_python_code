
# https://github.com/benoitc/gunicorn/blob/master/examples/example_config.py

def ssl_context(conf, default_ssl_context_factory):
    # так и не заработало, не понятно почему gunicorn не читает конфиг
    import ssl
    context: ssl.SSLContext = default_ssl_context_factory()
    context.load_cert_chain(
        certfile="/mnt/secrets-A/cert/webserver_server_certs/published_pem.txt",
        keyfile="/mnt/secrets-A/cert/webserver_server_certs/tklds-airfl0004.delta.sbrf.ru.key",
    )
    context.verify_mode = ssl.CERT_OPTIONAL
    context.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN
    context.minimum_version = ssl.TLSVersion.TLSv1_3

    def sni_callback(socket, server_hostname, context):
        if server_hostname == "127.0.0.1":
            new_context = context or default_ssl_context_factory()
            context.load_cert_chain(
                certfile="/mnt/secrets-A/cert/webserver_server_certs/published_pem.txt",
                keyfile="/mnt/secrets-A/cert/webserver_server_certs/tklds-airfl0004.delta.sbrf.ru.key",
            )
            new_context.minimum_version = ssl.TLSVersion.TLSv1_3
            new_context.verify_mode = ssl.CERT_OPTIONAL
            new_context.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
        else:
            new_context = context or default_ssl_context_factory()
            context.load_cert_chain(
                certfile="/mnt/secrets-A/cert/webserver_server_certs/published_pem.txt",
                keyfile="/mnt/secrets-A/cert/webserver_server_certs/tklds-airfl0004.delta.sbrf.ru.key",
            )
            new_context.minimum_version = ssl.TLSVersion.TLSv1_3
            new_context.verify_mode = ssl.CERT_OPTIONAL
            new_context.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
        socket.context = new_context

    context.sni_callback = sni_callback

    return context


# def ssl_context(conf, default_ssl_context_factory):
#     import ssl
#
#     # The default SSLContext returned by the factory function is initialized
#     # with the TLS parameters from config, including TLS certificates and other
#     # parameters.
#     context = default_ssl_context_factory()
#
#     # The SSLContext can be further customized, for example by enforcing
#     # minimum TLS version.
#     context.minimum_version = ssl.TLSVersion.TLSv1_3
#     context.verify_mode = ssl.CERT_OPTIONAL
#     context.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
#
#     # Server can also return different server certificate depending which
#     # hostname the client uses. Requires Python 3.7 or later.
#     def sni_callback(socket, server_hostname, context):
#         if server_hostname == "foo.127.0.0.1.nip.io":
#             new_context = context or default_ssl_context_factory()
#             # new_context.load_cert_chain(certfile="foo.pem", keyfile="foo-key.pem")
#             new_context.minimum_version = ssl.TLSVersion.TLSv1_3
#             new_context.verify_mode = ssl.CERT_OPTIONAL
#             new_context.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
#         else:
#             new_context = context or default_ssl_context_factory()
#             # new_context.load_cert_chain(certfile="foo.pem", keyfile="foo-key.pem")
#             new_context.minimum_version = ssl.TLSVersion.TLSv1_3
#             new_context.verify_mode = ssl.CERT_OPTIONAL
#             new_context.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
#         socket.context = new_context
#
#     context.sni_callback = sni_callback
#
#     return context
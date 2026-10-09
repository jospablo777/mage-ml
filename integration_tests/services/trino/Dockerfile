FROM trinodb/trino:483

USER root
RUN rm /etc/trino/catalog/*.properties
USER trino
COPY jvm.config config.properties /etc/trino/
COPY catalog/ /etc/trino/catalog/
HEALTHCHECK --interval=3s --timeout=5s --retries=100 CMD /usr/lib/trino/bin/health-check

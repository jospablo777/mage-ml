# Spark Connect for the Spark stream tests (ADR-0047): Spark 4.3.0 in local mode, its Python
# workers on Python 3.12 with pyarrow and pandas (Spark needs the client's Python version;
# the image's own is 3.10), and the Kafka connector in its jars, so it starts offline.
FROM ghcr.io/astral-sh/uv:0.11.29 AS uv

FROM apache/spark:4.3.0-python3

USER root
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python
RUN uv venv --python 3.12 /opt/workers \
    && uv pip install --python /opt/workers/bin/python pyarrow==26.0.0 pandas==3.0.6 \
    && mkdir -p /opt/ivy /checkpoints \
    && chown -R spark:spark /opt/workers /opt/uv-python /opt/ivy /checkpoints
ENV PYSPARK_PYTHON=/opt/workers/bin/python

# spark-submit resolves --packages before it looks for the class, which it then does not
# find; the resolved jars go into Spark's own.
USER spark
RUN /opt/spark/bin/spark-submit --conf spark.jars.ivy=/opt/ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
        --class none.Resolve /opt/spark/examples/jars/spark-examples.jar || true; \
    ls /opt/ivy/jars/*spark-sql-kafka*.jar
USER root
RUN cp /opt/ivy/jars/*.jar /opt/spark/jars/
USER spark

ENV SPARK_NO_DAEMONIZE=1
EXPOSE 15002
CMD ["/opt/spark/sbin/start-connect-server.sh", \
     "--master", "local[2]", \
     "--conf", "spark.driver.memory=1g", \
     "--conf", "spark.sql.shuffle.partitions=2", \
     "--conf", "spark.connect.grpc.binding.port=15002"]
HEALTHCHECK --interval=3s --timeout=5s --retries=100 \
    CMD bash -c 'echo > /dev/tcp/127.0.0.1/15002'

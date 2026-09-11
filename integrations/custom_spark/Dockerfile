FROM eclipse-temurin:11-jdk-jammy AS java
FROM ghcr.io/astral-sh/uv:0.11.29 AS uv
FROM python:3.12-slim-trixie
COPY --from=java /opt/java/openjdk /opt/java/openjdk
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends tini libstdc++6 libgomp1 fontconfig && \
    rm -rf /var/lib/apt/lists/* && uv pip uninstall --system pip
ENV JAVA_HOME=/opt/java/openjdk \
    UV_PROJECT_ENVIRONMENT=/opt/spark-env \
    PATH="/opt/spark-env/bin:/opt/java/openjdk/bin:$PATH"
WORKDIR /build/spark
COPY runtimes/custom-spark/pyproject.toml runtimes/custom-spark/uv.lock ./
RUN uv sync --locked --no-cache && uv pip check --python /opt/spark-env/bin/python
ENV SPARK_HOME=/opt/spark-env/lib/python3.12/site-packages/pyspark \
    PYSPARK_PYTHON=/opt/spark-env/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/spark-env/bin/python \
    SCALA_VERSION=2.12 \
    SPARK_VERSION=3.5.9
COPY integrations/custom_spark/hive-site.xml /opt/spark-env/lib/python3.12/site-packages/pyspark/conf/hive-site.xml
WORKDIR /home/src
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD ["bash"]

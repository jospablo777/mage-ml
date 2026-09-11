FROM node:24-bookworm-slim AS frontend
WORKDIR /build/mage_ai/frontend
ENV NEXT_TELEMETRY_DISABLED=1
COPY mage_ai/frontend/package.json mage_ai/frontend/yarn.lock ./
RUN yarn install --frozen-lockfile --non-interactive
COPY mage_ai/frontend ./
RUN NODE_OPTIONS=--max-old-space-size=4096 yarn export_prod && \
    NODE_OPTIONS=--max-old-space-size=4096 yarn export_prod_base_path

FROM ghcr.io/astral-sh/uv:0.11.29 AS uv

FROM python:3.12-slim-trixie AS runtime-base
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
COPY --from=uv /uv /uvx /usr/local/bin/

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends ca-certificates curl git openssh-client && \
    curl -fsSL https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb \
      -o /tmp/packages-microsoft-prod.deb && \
    dpkg -i /tmp/packages-microsoft-prod.deb && \
    rm /tmp/packages-microsoft-prod.deb && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      tini msodbcsql18 libodbc2 libmagic1 libgssapi-krb5-2 libgomp1 \
      graphviz postgresql-client r-base && \
    uv pip uninstall --system pip && \
    rm -rf /var/lib/apt/lists/*

ENV UV_PROJECT_ENVIRONMENT=/opt/mage \
    PATH="/opt/mage/bin:$PATH" \
    MAGE_RUNTIME_CONSTRAINTS=/opt/mage/runtime-constraints.txt

FROM runtime-base AS python-build
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libkrb5-dev unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build/mage
COPY pyproject.toml uv.lock README.md MANIFEST.in ./
COPY mage_integrations ./mage_integrations
COPY vendor ./vendor
COPY mage_ai ./mage_ai
COPY --from=frontend /build/mage_ai/server/frontend_dist ./mage_ai/server/frontend_dist
COPY --from=frontend /build/mage_ai/server/frontend_dist_base_path_template ./mage_ai/server/frontend_dist_base_path_template
RUN --mount=type=cache,target=/root/.cache/uv \
    UV_LINK_MODE=copy uv sync --locked --no-editable --no-default-groups \
      --extra all --extra integrations && \
    uv pip check --python /opt/mage/bin/python && \
    uv export --locked --no-default-groups --extra all --extra integrations \
      --no-emit-workspace --no-hashes --no-header --output-file "$MAGE_RUNTIME_CONSTRAINTS"

COPY runtimes/livy /build/livy
RUN --mount=type=cache,target=/root/.cache/uv \
    UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/mage-livy \
      uv sync --project /build/livy --locked && \
    uv pip check --python /opt/mage-livy/bin/python && \
    mkdir -p /opt/mage/share/jupyter/kernels/pysparkkernel /root/.sparkmagic && \
    cp /build/livy/kernel.json /opt/mage/share/jupyter/kernels/pysparkkernel/kernel.json && \
    cp /build/livy/config.json /root/.sparkmagic/config.json && \
    rm -rf /build

RUN R -e "install.packages(c('pacman', 'renv'), repos='https://cloud.r-project.org'); stopifnot(requireNamespace('pacman', quietly=TRUE), requireNamespace('renv', quietly=TRUE))"

FROM runtime-base AS runtime
COPY --from=python-build /opt/mage /opt/mage
COPY --from=python-build /opt/mage-livy /opt/mage-livy
COPY --from=python-build /root/.sparkmagic /root/.sparkmagic
COPY --from=python-build /usr/local/lib/R/site-library /usr/local/lib/R/site-library
COPY --chmod=0755 scripts/install_other_dependencies.py scripts/run_app.sh /app/
ENV MAGE_DATA_DIR=/home/src/mage_data \
    PYTHONPATH=/home/src
WORKDIR /home/src
EXPOSE 6789 7789
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD ["/app/run_app.sh"]

FROM runtime AS development
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libkrb5-dev unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*
COPY --from=frontend /usr/local/bin/node /usr/local/bin/node
COPY --from=frontend /opt/yarn-v1.22.22 /opt/yarn-v1.22.22
RUN ln -s /opt/yarn-v1.22.22/bin/yarn /usr/local/bin/yarn
COPY . /home/src
RUN uv sync --locked --extra all --extra integrations --group dev && \
    uv pip check --python /opt/mage/bin/python
WORKDIR /home/src/mage_ai/frontend
RUN yarn install --frozen-lockfile --non-interactive && \
    yarn cache clean
WORKDIR /home/src

FROM development AS spark
RUN apt-get update && apt-get install -y --no-install-recommends default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_HOME=/opt/mage-spark/lib/python3.12/site-packages/pyspark \
    PYSPARK_PYTHON=/opt/mage-spark/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/mage-spark/bin/python
RUN UV_PROJECT_ENVIRONMENT=/opt/mage-spark uv sync --project runtimes/local-spark --locked --no-cache && \
    uv pip check --python /opt/mage-spark/bin/python

FROM runtime AS production

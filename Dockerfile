FROM python:3.10-bookworm
LABEL description="Deploy Mage on ECS"
USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

## System Packages
RUN \
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
  curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
  apt-get -y update && \
  ACCEPT_EULA=Y apt-get -y install --no-install-recommends \
  # NFS dependencies
  nfs-common \
  # odbc dependencies
  msodbcsql18\
  unixodbc-dev \
  graphviz \
  # postgres dependencies \
  postgresql-client \
  # R
  r-base && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

## R Packages
RUN \
  R -e "install.packages('pacman', repos='http://cran.us.r-project.org')" && \
  R -e "install.packages('renv', repos='http://cran.us.r-project.org')"

# uv is the package manager for this project. Pin it so image builds and CI
# resolve dependencies the same way.
ARG UV_VERSION=0.11.29
RUN pip3 install --no-cache-dir "uv==$UV_VERSION"

## Python Packages
RUN \
  uv pip install --system --no-cache-dir sparkmagic && \
  mkdir ~/.sparkmagic && \
  curl https://raw.githubusercontent.com/jupyter-incubator/sparkmagic/master/sparkmagic/example_config.json > ~/.sparkmagic/config.json && \
  sed -i 's/localhost:8998/host.docker.internal:9999/g' ~/.sparkmagic/config.json && \
  jupyter-kernelspec install --user "$(uv pip show sparkmagic | grep Location | cut -d' ' -f2)/sparkmagic/kernels/pysparkkernel"
# Packages that are not resolved from uv.lock.
RUN \
  uv pip install --system --no-cache-dir "git+https://github.com/wbond/oscrypto.git@d5f3437ed24257895ae1edd9e503cfb352e635a8" && \
  uv pip install --system --no-cache-dir "git+https://github.com/dremio-hub/arrow-flight-client-examples.git#egg=dremio-flight&subdirectory=python/dremio-flight" && \
  uv pip install --system --no-cache-dir "git+https://github.com/mage-ai/singer-python.git#egg=singer-python" && \
  uv pip install --system --no-cache-dir "git+https://github.com/mage-ai/dbt-mysql.git#egg=dbt-mysql" && \
  uv pip install --system --no-cache-dir "git+https://github.com/mage-ai/sqlglot#egg=sqlglot" && \
  # faster-fifo is not supported on Windows: https://github.com/alex-petrenko/faster-fifo/issues/17
  uv pip install --system --no-cache-dir faster-fifo

# Mage integrations, installed from this build context.
COPY mage_integrations /tmp/mage_integrations
RUN \
  uv pip install --system --no-cache-dir /tmp/mage_integrations && \
  rm -rf /tmp/mage_integrations

# Mage, built from this build context with dependencies resolved from uv.lock.
# The image must carry this fork so the container scan covers shipped code.
# --inexact preserves the packages installed above, which the lockfile omits.
# --no-deps on the project install: the sync already placed its dependencies.
ENV UV_PROJECT_ENVIRONMENT=/usr/local
COPY pyproject.toml uv.lock README.md MANIFEST.in /tmp/mage/
COPY mage_ai /tmp/mage/mage_ai
RUN \
  uv sync --project /tmp/mage --locked --no-install-project --inexact --no-cache \
  --extra all --extra integrations && \
  uv pip install --system --no-cache-dir --no-deps /tmp/mage && \
  rm -rf /tmp/mage


## Startup Script
COPY --chmod=0755 ./scripts/install_other_dependencies.py ./scripts/run_app.sh /app/

ENV MAGE_DATA_DIR="/home/src/mage_data"
ENV PYTHONPATH="${PYTHONPATH}:/home/src"
WORKDIR /home/src
EXPOSE 6789
EXPOSE 7789

CMD ["/bin/sh", "-c", "/app/run_app.sh"]

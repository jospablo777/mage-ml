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
      graphviz postgresql-client && \
    uv pip uninstall --system pip && \
    rm -rf /var/lib/apt/lists/*

# R 4.6 from CRAN's Debian repository, and rv, which installs the packages of each
# project's R environment (mage r init). Debian trixie ships R 4.5. rv installs
# binaries from Posit Package Manager where it has them, as for amd64, and builds the
# other packages from source with r-base-dev, as on arm64. The -dev libraries are those
# that rv sysdeps lists for the default environment (the tidyverse, arrow and the DBI
# drivers); without them a source build fails, such as fs without libuv.
ARG RV_VERSION=0.20.0
RUN curl -fsSL \
      'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7' \
      -o /etc/apt/trusted.gpg.d/cran_debian_key.asc && \
    printf '%s\n' 'Types: deb' 'URIs: https://cloud.r-project.org/bin/linux/debian/' \
      'Suites: trixie-cran46/' 'Components:' \
      'Signed-By: /etc/apt/trusted.gpg.d/cran_debian_key.asc' \
      > /etc/apt/sources.list.d/cran.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends r-base-core r-recommended r-base-dev \
      cmake libcurl4-openssl-dev libfontconfig1-dev libfreetype6-dev libfribidi-dev \
      libharfbuzz-dev libicu-dev libjpeg-dev libmariadb-dev libpng-dev libpq-dev \
      libssl-dev libtiff-dev libuv1-dev libwebp-dev libxml2-dev make xz-utils \
      zlib1g-dev && \
    curl -fsSL "https://github.com/A2-ai/rv/releases/download/v${RV_VERSION}/rv-v${RV_VERSION}-$(uname -m)-unknown-linux-gnu.tar.gz" \
      | tar -xz -C /usr/local/bin rv && \
    Rscript -e 'stopifnot(getRversion() >= "4.6.0")' && \
    rv --version && \
    rm -rf /var/lib/apt/lists/*

# arrow built from source, as on arm64, leaves out S3 and GCS unless this is false.
ENV LIBARROW_MINIMAL=false

ENV UV_PROJECT_ENVIRONMENT=/opt/mage \
    PATH="/opt/mage/bin:$PATH" \
    MAGE_RUNTIME_CONSTRAINTS=/opt/mage/runtime-constraints.txt

FROM runtime-base AS python-build
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libkrb5-dev unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

# Rust compiles the ColumnAtlas extension (rust/column_atlas) here. The runtime image
# copies the installed environment only, so it carries no toolchain. Match
# rust/column_atlas/rust-toolchain.toml.
ARG RUST_VERSION=1.98.0
ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH="/opt/cargo/bin:$PATH"
RUN curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | \
      sh -s -- -y --no-modify-path --profile minimal --default-toolchain "$RUST_VERSION" && \
    rustc --version

WORKDIR /build/mage
COPY pyproject.toml uv.lock README.md MANIFEST.in ./
COPY mage_integrations ./mage_integrations
COPY vendor ./vendor
COPY rust ./rust
COPY mage_ai ./mage_ai
COPY --from=frontend /build/mage_ai/server/frontend_dist ./mage_ai/server/frontend_dist
COPY --from=frontend /build/mage_ai/server/frontend_dist_base_path_template ./mage_ai/server/frontend_dist_base_path_template
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/opt/cargo/registry \
    UV_LINK_MODE=copy uv sync --locked --no-editable --no-default-groups \
      --extra all --extra integrations && \
    uv pip check --python /opt/mage/bin/python && \
    /opt/mage/bin/python -c 'import column_atlas_native' && \
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

FROM runtime-base AS runtime
COPY --from=python-build /opt/mage /opt/mage
COPY --from=python-build /opt/mage-livy /opt/mage-livy
COPY --from=python-build /root/.sparkmagic /root/.sparkmagic
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
# The editable install below builds the ColumnAtlas extension from the source tree.
COPY --from=python-build /opt/rustup /opt/rustup
COPY --from=python-build /opt/cargo /opt/cargo
ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH="/opt/cargo/bin:$PATH"
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

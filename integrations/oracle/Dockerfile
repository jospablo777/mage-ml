# The MageML image with Oracle Instant Client, for python-oracledb thick mode
# (mode: thick in io_config.yaml).
#
# Build the MageML image first, from the repository root:
#   docker build -t mageml .
# then this one:
#   docker build -t mageml-oracle integrations/oracle
# Set BASE_IMAGE to use another MageML image.
ARG BASE_IMAGE=mageml
FROM ${BASE_IMAGE}

ARG INSTANT_CLIENT_VERSION=23.7.0.25.01
ARG INSTANT_CLIENT_DIR=2370000
ARG TARGETARCH

# libaio1t64 replaces libaio1 on Debian trixie; Instant Client still loads libaio.so.1.
RUN apt-get update && \
    apt-get install -y --no-install-recommends unzip libaio1t64 && \
    rm -rf /var/lib/apt/lists/* && \
    case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
      amd64) platform=x64 ;; \
      arm64) platform=arm64 ;; \
      *) echo "Oracle Instant Client is published for amd64 and arm64 only" >&2; exit 1 ;; \
    esac && \
    curl -fsSL -o /tmp/instantclient.zip \
      "https://download.oracle.com/otn_software/linux/instantclient/${INSTANT_CLIENT_DIR}/instantclient-basic-linux.${platform}-${INSTANT_CLIENT_VERSION}.zip" && \
    mkdir -p /opt/oracle && \
    unzip -q /tmp/instantclient.zip -d /opt/oracle && \
    rm /tmp/instantclient.zip && \
    libdir="/usr/lib/$(uname -m)-linux-gnu" && \
    ln -sf "$libdir/libaio.so.1t64" "$libdir/libaio.so.1" && \
    echo /opt/oracle/instantclient_* > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

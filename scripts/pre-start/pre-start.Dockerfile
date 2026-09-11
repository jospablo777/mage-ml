FROM ghcr.io/astral-sh/uv:0.11.29 AS uv
FROM python:3.12-slim-trixie
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/* && \
    uv pip uninstall --system pip
ENV UV_PROJECT_ENVIRONMENT=/opt/pre-start PATH="/opt/pre-start/bin:$PATH"
WORKDIR /build/pre-start
COPY runtimes/pre-start/pyproject.toml runtimes/pre-start/uv.lock ./
RUN uv sync --locked --no-cache && uv pip check --python /opt/pre-start/bin/python
COPY --chmod=0755 scripts/pre-start /app/
CMD ["/app/pre-start.sh"]

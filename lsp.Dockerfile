FROM ghcr.io/astral-sh/uv:0.11.29 AS uv
FROM python:3.12-slim-trixie
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/* && \
    uv pip uninstall --system pip
ENV UV_PROJECT_ENVIRONMENT=/opt/lsp PATH="/opt/lsp/bin:$PATH"
WORKDIR /build/lsp
COPY runtimes/lsp/pyproject.toml runtimes/lsp/uv.lock ./
RUN uv sync --locked --no-cache && uv pip check --python /opt/lsp/bin/python
WORKDIR /home/src
ENV PORT=8765
EXPOSE 8765
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD ["sh", "-c", "exec pylsp --ws --host 0.0.0.0 --port ${PORT}"]

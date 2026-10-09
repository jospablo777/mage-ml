# MinIO no longer publishes Docker images, so the release is built from its source.
FROM golang:1.24 AS build
ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
RUN git clone --depth 1 --branch "${MINIO_RELEASE}" https://github.com/minio/minio.git /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /minio .

FROM alpine:3.22
COPY --from=build /minio /usr/bin/minio
EXPOSE 9000
HEALTHCHECK --interval=2s --timeout=5s --retries=60 \
    CMD wget -q -O /dev/null http://127.0.0.1:9000/minio/health/live || exit 1
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--address", ":9000"]

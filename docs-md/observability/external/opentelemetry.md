# Monitoring with OpenTelemetry

## Overview

This guide details how to send traces from a MageML server to an OpenTelemetry Collector.

## Prerequisites

Ensure the following prerequisites are in place before proceeding:
- An OpenTelemetry Collector is set up and configured to receive OTLP data over gRPC.
- The OpenTelemetry packages are installed. They are part of the `all` extra
  (`uv add "mage-ml[all]"`), which the image built from the repository's Dockerfile includes.

## Configuring OpenTelemetry

### Setting environment variables

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to the gRPC endpoint of your OpenTelemetry Collector:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="192.168.1.56:4317"
```

MageML exports traces with the OTLP gRPC exporter over an insecure channel (no TLS), with the
service name `mage-ai-server`. There is no HTTP exporter setting.

### Instrumentation in the application

1. **SQLAlchemy Instrumentation**:
MageML uses the SQLAlchemyInstrumentor from OpenTelemetry to instrument operations on its own database. This generates traces for database interactions.

2. **Tornado Instrumentation**:
To monitor HTTP server operations, MageML uses OpenTelemetry's Tornado instrumentation. This collects data related to HTTP requests and server performance.

## Telemetry data collection and analysis

With the environment variable set, restart the server. OpenTelemetry starts collecting traces based on the instrumentation above.

1. **Data Collection**:
MageML sends the collected traces to the OpenTelemetry Collector endpoint. For metrics, see [Prometheus](prometheus.md).

2. **Analyzing Telemetry Data**:
Connect your OpenTelemetry Collector to backend tools that support OpenTelemetry data (like Grafana or Jaeger).

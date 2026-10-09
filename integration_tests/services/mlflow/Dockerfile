FROM python:3.12-slim

WORKDIR /mlflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY expected.py seed.py entrypoint.py ./

ENV MLFLOW_DISABLE_AGENT_HINT=1
EXPOSE 5000
HEALTHCHECK --interval=3s --timeout=5s --retries=100 \
    CMD python -c "import os, sys, urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/health'); sys.exit(0 if os.path.exists('/mlflow/seeded') else 1)"
CMD ["python", "entrypoint.py"]

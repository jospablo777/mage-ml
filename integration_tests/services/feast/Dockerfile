# Feast requires pandas below 3, so it runs as its own service.
FROM python:3.12-slim

WORKDIR /feast
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY feature_repo ./feature_repo
COPY dataset.py typed_push.py entrypoint.py ./

EXPOSE 6566
HEALTHCHECK --interval=3s --timeout=5s --retries=100 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:6566/health')"
CMD ["python", "entrypoint.py"]

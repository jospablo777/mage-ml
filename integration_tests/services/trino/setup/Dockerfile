FROM python:3.12-slim

RUN pip install --no-cache-dir psycopg2-binary==2.9.11 boto3==1.43.110
COPY setup.py /setup.py
CMD ["python", "/setup.py"]

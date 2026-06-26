FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary

COPY scripts /app/scripts

EXPOSE 9108

CMD ["python", "-m", "scripts.monitoring.cdc_validation_exporter"]

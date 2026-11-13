FROM python:3.12.8-slim-bookworm

WORKDIR /app
COPY deploy/lanes/etl/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY engine /app/engine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

USER 10001:10001
ENTRYPOINT ["python", "-m", "engine.lanes.etl"]

FROM python:3.12.8-slim-bookworm

COPY deploy/lanes/elt/requirements.txt /opt/elt/requirements.txt
# L'extension DuckDB `postgres` est installée à la construction (pas de téléchargement à l'exécution).
RUN python -m pip install --no-cache-dir -r /opt/elt/requirements.txt \
    && python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL postgres')" \
    && mkdir -p /work \
    && useradd --system --uid 10001 --create-home elt \
    && cp -r /root/.duckdb /home/elt/.duckdb && chown -R 10001:10001 /work /opt/elt /home/elt

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MDM_ELT_WORKDIR=/work

USER 10001:10001
WORKDIR /workspace
ENTRYPOINT ["python", "-m", "engine.lanes.elt"]

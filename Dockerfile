FROM python:3.12.8-slim-bookworm AS runtime

WORKDIR /app
COPY pyproject.toml README.md ./
COPY engine ./engine
RUN python -m pip install --no-cache-dir ".[elt]" \
    && useradd --system --uid 10001 --no-create-home app \
    && mkdir /work && chown app /work

RUN mkdir /data && chown app /data
ENV HOME=/work PYTHONDONTWRITEBYTECODE=1
WORKDIR /work
USER app
# Exemple : docker run --rm --memory 8g -e MDM_STATE_DSN=... -v ./configs:/cfg mdm-engine \
#   python -m engine.lanes.etl ingest --config /cfg/customer.yml --source crm --run-id R1
CMD ["python", "-m", "engine.cli", "--help"]

FROM runtime AS test
USER root
RUN python -m pip install --no-cache-dir "/app[dev]"
USER app
CMD ["python", "-m", "pytest"]

FROM runtime AS final

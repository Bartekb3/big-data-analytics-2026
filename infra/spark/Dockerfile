# Shared image of the Spark jobs (speed, batch) and of the spark-marked tests.
# Dependencies only: src/, tests/ and pyproject.toml are bind-mounted by docker-compose.yml, so a code change
# needs a restart, not a rebuild. Rebuild after a change of uv.lock or of the files in infra/spark/.
# Build context: the repository root (see .dockerignore).

# bookworm, not the default trixie: trixie no longer ships OpenJDK 17.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv==0.13.6

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTEST_ADDOPTS="-p no:cacheprovider" \
    SPARK_CONF_DIR=/opt/spark-conf

WORKDIR /app

# The project itself is not installed: its code comes from the bind mount through PYTHONPATH.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --extra spark --group dev --no-cache

COPY infra/spark/fetch_kafka_jars.py /tmp/fetch_kafka_jars.py
RUN python /tmp/fetch_kafka_jars.py && rm /tmp/fetch_kafka_jars.py

COPY infra/spark/spark-defaults.conf /opt/spark-conf/spark-defaults.conf

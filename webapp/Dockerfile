FROM python:3.13-slim

LABEL maintainer="Conservation International"
LABEL description="Avoided emissions web application"

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app

COPY webapp/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY webapp/ /app/
COPY gee_export/ /app/gee_export/

COPY webapp/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN useradd -m appuser
RUN mkdir -p /app/celerybeat /run/gunicorn && chown -R appuser:appuser /app/celerybeat /run/gunicorn
USER appuser

EXPOSE 8050

ENTRYPOINT ["/entrypoint.sh"]
# Increased timeout from 120 to 600 seconds to handle large site datasets (>100k polygons)
# Large GeoJSON serialization can take several minutes; timeout is per-request
# Keep workers=2 to limit memory for concurrent large requests
CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "--timeout", "600", "--max-requests", "200", "--max-requests-jitter", "30", "--worker-tmp-dir", "/run/gunicorn", "--max-requests-jitter", "40", "--limit-request-line", "8190", "--limit-request-fields", "100", "--limit-request-field-size", "16384", "app:server"]

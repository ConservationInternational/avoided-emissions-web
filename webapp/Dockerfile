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
COPY gee-export/ /app/gee-export/

COPY webapp/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN useradd -m appuser
RUN mkdir -p /app/celerybeat /run/gunicorn && chown -R appuser:appuser /app/celerybeat /run/gunicorn
USER appuser

EXPOSE 8050

ENTRYPOINT ["/entrypoint.sh"]
CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "4", "--timeout", "120", "--worker-tmp-dir", "/run/gunicorn", "app:server"]

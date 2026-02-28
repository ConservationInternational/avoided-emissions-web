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

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN useradd -m appuser
USER appuser

EXPOSE 8050

ENTRYPOINT ["/entrypoint.sh"]
CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "4", "--timeout", "120", "app:server"]

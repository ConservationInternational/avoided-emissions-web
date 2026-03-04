FROM rocker/r-ver:4.5.2

LABEL maintainer="Conservation International"
LABEL description="Avoided emissions propensity score matching analysis"

# System dependencies for spatial R packages and Python geospatial libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libudunits2-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libxml2-dev \
    libsqlite3-dev \
    gdal-bin \
    git \
    jq \
    python3 \
    python3-gdal \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN install2.r --error --skipinstalled --ncpus -1 \
    sf \
    terra \
    tidyverse \
    optmatch \
    lubridate \
    units \
    foreach \
    dtplyr \
    jsonlite \
    arrow \
    httr \
    && rm -rf /tmp/downloaded_packages

WORKDIR /app
COPY scripts/ /app/scripts/
COPY entrypoint.sh /app/entrypoint.sh
COPY src/ /app/
COPY requirements.txt /app/requirements.txt
RUN chmod +x /app/entrypoint.sh

# Install Python dependencies. Step 1 (extract) now runs in Python for
# speed using GDAL, rioxarray, and xarray to read COGs.
# Use --system-site-packages so the venv can access the pre-built
# python3-gdal system package (osgeo), avoiding a slow source compile.
RUN python3 -m venv --system-site-packages /app/.venv \
    && /app/.venv/bin/pip install --no-cache-dir \
        -r /app/requirements.txt \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["/app/entrypoint.sh"]

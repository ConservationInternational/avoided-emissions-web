FROM rocker/r-ver:4.5.2

LABEL maintainer="Conservation International"
LABEL description="Avoided emissions propensity score matching analysis"

# System dependencies for spatial R packages
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
    awscli \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN install2.r --error --skipinstalled --ncpus -1 \
    sf \
    terra \
    raster \
    exactextractr \
    fasterize \
    tidyverse \
    optmatch \
    lubridate \
    units \
    foreach \
    doParallel \
    Rcpp \
    dtplyr \
    biglm \
    jsonlite \
    gdalUtilities \
    arrow \
    httr \
    && rm -rf /tmp/downloaded_packages

WORKDIR /app
COPY scripts/ /app/scripts/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

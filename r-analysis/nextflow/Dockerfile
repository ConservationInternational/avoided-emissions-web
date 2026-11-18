FROM public.ecr.aws/amazoncorretto/amazoncorretto:25-al2023

RUN dnf install -y python3 python3-pip procps-ng tar gzip \
    && dnf clean all \
    && python3 -m pip install --no-cache-dir 'boto3>=1.36,<2' 'awscli>=1.32,<2' \
    && ln -s /usr/bin/python3 /usr/local/bin/python
ENV NXF_VER=26.10.6 \
    NXF_HOME=/opt/nextflow \
    NXF_ANSI_LOG=false
RUN curl --fail --show-error --location \
        https://github.com/nextflow-io/nextflow/releases/download/v26.10.6/nextflow-26.10.6-dist \
        --output /usr/local/bin/nextflow \
    && chmod +x /usr/local/bin/nextflow \
    && nextflow -version

RUN for attempt in 1 2 3; do \
        if nextflow -log /tmp/nextflow-plugin-install.log plugin install nf-amazon@3.12.1; then \
            rm -f /tmp/nextflow-plugin-install.log; \
            break; \
        fi; \
        echo "Plugin installation failed on attempt $attempt" >&2; \
        if [ -f /tmp/nextflow-plugin-install.log ]; then \
            cat /tmp/nextflow-plugin-install.log >&2; \
        fi; \
        if [ "$attempt" -eq 3 ]; then exit 1; fi; \
        sleep $((attempt * 5)); \
    done
WORKDIR /work
COPY nextflow/ /opt/ae-workflow/
COPY src/workflow_store.py src/nextflow_worker.py src/summary_shards.py /opt/ae-workflow/
ENTRYPOINT []
CMD ["python", "/opt/ae-workflow/driver.py"]

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
    && nextflow -version \
    && nextflow plugin install nf-amazon@3.12.1
WORKDIR /work
COPY nextflow/ /opt/ae-workflow/
COPY src/workflow_store.py src/nextflow_worker.py /opt/ae-workflow/
ENTRYPOINT []
CMD ["python", "/opt/ae-workflow/driver.py"]

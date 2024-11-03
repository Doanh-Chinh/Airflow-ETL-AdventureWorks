FROM apache/airflow:2.10.2

# Install build packages
# gcc, heimdal-dev as provider packages dependencies
# openjdk for pyspark client (https://airflow.apache.org/docs/docker-stack/build.html#example-of-adding-airflow-provider-package-and-apt-package)
USER root
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        gcc \
        heimdal-dev \
        openjdk-17-jre-headless \
    && apt-get autoremove -yqq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Airflow & Python packages
USER ${AIRFLOW_UID}
COPY ./requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Migrate DB & create account
RUN airflow db migrate \
    && airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@localhost

# Init webserver & scheduler
ENTRYPOINT ["airflow", "standalone"]
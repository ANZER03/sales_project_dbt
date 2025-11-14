FROM my-airflow-java:latest

ENV AIRFLOW_HOME=/opt/airflow \
    PYTHONUNBUFFERED=1 \
    PATH="/home/airflow/.local/bin:${PATH}"

ARG DBT_VERSION=1.8.6

USER root

# Ensure dbt compilation dependencies exist before installing pip packages
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#         build-essential \
#         libpq-dev \
#         git \
#     && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt packages as airflow user per Airflow Docker guidelines
RUN python -m pip install --no-cache-dir \
        "dbt-core" \
        "dbt-postgres"

WORKDIR ${AIRFLOW_HOME}

CMD ["airflow","version"]

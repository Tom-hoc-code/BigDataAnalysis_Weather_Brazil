# =====================================
# Luong Ngoc Huy - 23133028
# =====================================
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.2
ENV PYTHON_VERSION=3.10
ENV PATH="/opt/airflow/venv/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-venv \
        build-essential \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        curl \
        wget \
        git \
        nano \
        iputils-ping \
        netcat && \
    apt-get clean

RUN useradd -ms /bin/bash airflow

RUN mkdir -p ${AIRFLOW_HOME} && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

COPY start.sh /start.sh

RUN sed -i 's/\r$//' /start.sh && chmod +x /start.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}

RUN python3 -m venv venv

RUN pip install --upgrade pip

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    pip install "apache-airflow-providers-amazon" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    pip install requests psycopg2-binary

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False

EXPOSE 8080

CMD ["/start.sh"]
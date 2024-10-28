FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install --assume-yes git

RUN python -m venv /venv-meltano
ENV PATH="/venv-meltano/bin:${PATH}"

RUN pip install "SQLAlchemy<2.0"

RUN pip install meltano

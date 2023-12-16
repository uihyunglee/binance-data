FROM apache/airflow:2.7.3-python3.11

LABEL maintainer="hongcana"

COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install airflow-code-editor
RUN pip install -r /requirements.txt
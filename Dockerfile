FROM apache/airflow:3.1.0-python3.11
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

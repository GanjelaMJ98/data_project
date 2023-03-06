FROM apache/airflow:2.4.1

COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --user -r /requirements.txt

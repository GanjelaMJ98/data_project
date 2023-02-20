FROM apache/airflow:2.4.1

VOLUME ./my_libs/smartc_lib /usr/local/lib/python3.7/site-packages/smartc_lib

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --user -r /requirements.txt
# RUN ["pip", "install", "-r", "~/requirements.txt"]
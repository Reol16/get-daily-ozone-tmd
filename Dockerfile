FROM apache/airflow:2.9.0-python3.9


RUN pip install --upgrade pip
RUN pip install psycopg2-binary


COPY requirements.txt .
RUN pip install -r requirements.txt


import os
import requests
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_ozone_tmd_availability():
    url = 'http://ozone.tmd.go.th/Data/Surface/'
    response = requests.get(url)
    if response.status_code == 200:
        return True
    else:
        raise Exception("Failed to connect to ozone.tmd.go.th")


def download_tmdstationcsv():
    tmdstation_url = "http://ozone.tmd.go.th/Data/Surface/TMD-station.csv"
    dir_path = '/opt/airflow/dags/data'
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, 'TMD-station.csv')
    if not os.path.exists(file_path):
        response = requests.get(tmdstation_url)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"File downloaded and saved to {file_path}")
    else:
        logger.info(f"File already exists at {file_path}, skipping download.")

def get_vr_dailycsv():
    base_url = "http://ozone.tmd.go.th/Data/Surface/Ventilation_daily_forecast/"
    current_time = datetime.now().strftime("%d.%m.%Y")
    folder_path = f'/opt/airflow/dags/data/{current_time}'
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"Directory '{folder_path}' was created.")
    else:
        logger.info(f"Directory '{folder_path}' already exists.")
    response = requests.get(base_url)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a')
    station_files = [link.get('href') for link in links if link.get('href').endswith('.csv')]

    for station_file in station_files:
        station_file_path = os.path.join(folder_path, station_file)
        if not os.path.exists(station_file_path):
            url = base_url + station_file
            file_response = requests.get(url)
            if file_response.status_code == 200:
                with open(station_file_path, 'wb') as file:
                    file.write(file_response.content)
                logger.info(f"File {station_file} downloaded and saved to {station_file_path}")
            else:
                logger.warning(f"Failed to download file {station_file}")
        else:
            logger.info(f"File {station_file} already exists at {station_file_path}, skipping download.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 20),
    'retries': 6,
}

dag = DAG(
    'web_scraping_daily_ozonetmd',
    default_args=default_args,
    description='Dag to daily download TMD-station.csv from ozone.tmd.go.th',
    schedule_interval='@daily',
    catchup=False,
)

check_url_availability = PythonOperator(
    task_id='check_url_availability',
    python_callable=check_ozone_tmd_availability,
    dag=dag,
)

download_tmdstationcsv_task = PythonOperator(
    task_id='download_tmdstationcsv',
    python_callable=download_tmdstationcsv,
    dag=dag,
)

get_vr_dailycsv_task = PythonOperator(
    task_id='get_vr_dailycsv',
    python_callable=get_vr_dailycsv,
    dag=dag,
)

check_url_availability >> download_tmdstationcsv_task >> get_vr_dailycsv_task

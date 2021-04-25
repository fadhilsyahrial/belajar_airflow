from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from pprint import pprint

import pandas as pd 

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='web_scrapping',
    default_args=args,
    schedule_interval=None,
    tags=['homework']
)

def web_scraper():
    url='https://id.wikipedia.org/wiki/Daftar_orang_terkaya_di_Indonesia'
    data=pd.read_html(url)
    data_2020=data[7]
    data_2020['tahun']=2020
    data_2020.to_csv('/usr/local/airflow/list_orang_terkaya_di_indonesia.csv')

scraping = PythonOperator(
    task_id='web_scraping',
    python_callable=web_scraper,
    dag=dag,
)

send_email = EmailOperator(
        task_id='send_email',
        to='fia.digitalskola@gmail.com',
        subject='RizkiFadhilSyahrial_DigitalSkola_Airflow',
        html_content=""" <h3>Hi Mbak Fia, berikut terlampir homework 3, week 8.</h3> """,
        files=['/usr/local/airflow/list_orang_terkaya_di_indonesia.csv'],
        dag=dag
)

scraping >> send_email


from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from Funciones import obtener_datos_desde_api, transformar_data, conectar_redshift, envio_mail 


# argumentos por defecto para el DAG
     
default_args = {
    'owner': 'marianolopez_coderhouse',
    'depends_on_pas': True,
    'email': ['marianolopez7749@gmail.com'],
    'email_on_retry': True,
    'email_on_failure': True,
    'start_date': datetime(2023,8,26),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='Nasdaq',
    default_args=default_args,
    description='Valores del Nasdaq',
    schedule_interval="@daily",
    catchup=False
)


task_1 = PythonOperator(
    task_id='obtener_datos_desde_api',
    python_callable=obtener_datos_desde_api,
    op_args=['https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.csv'],
    dag=dag,
)

task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    dag=dag
)

task_3 = PythonOperator(
    task_id='conectar_redshift',
    python_callable=conectar_redshift,
    dag=dag
)

task_4 = PythonOperator(
    task_id='envio_mail',
    python_callable=envio_mail,
    dag=dag
)
    

task_1 >> task_2 >> task_3 >> task_4


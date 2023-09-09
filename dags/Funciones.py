import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd 
import requests
from sqlalchemy.engine.url import URL
import traceback
from pathlib import Path
import os
from email.mime.text import MIMEText
import smtplib


def obtener_datos_desde_api(url):
    try:
        dag_path = os.getcwd()
        # Realizar una solicitud GET a la API
        response = requests.get(url)

        # Verificar el código de estado de la respuesta
        if response.status_code == 200:
           
            with open(dag_path+'/raw_data/temp.csv', "wb") as datos_file:
                datos_file.write(response.content)
                                     
        else:
            print(f"Error al obtener los datos. Código de estado: {response.status_code}")
            return None
    except requests.RequestException as e:
           print(f"Error de conexión: {e}")
           return None
    

def transformar_data():

    dag_path = os.getcwd()  

    with open(dag_path+'/raw_data/temp.csv', 'r') as datos_file:
      dt = pd.read_csv(datos_file)
      dt["Date"] = pd.to_datetime(dt["Date"], format="%Y-%m-%d")
      datos_seleccionados = dt[dt["Date"] > pd.to_datetime("2016-01-01", format="%Y-%m-%d")]
      datos_seleccionados = datos_seleccionados.drop_duplicates()
      datos_seleccionados.to_csv(dag_path+'/processed_data/datos_finales.csv', index=False, encoding='utf-8')
        
      


def conectar_redshift(**context):
    try:  
        
        dag_path = os.getcwd()     #path original.. home en Docker

        
        user = context['var']['value'].get('user_redshift')
        pwd = context['var']['value'].get('password_redshift')
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
        port=5439
        database='data-engineer-database'

        

        conn_str = f"postgresql://{user}:{pwd}@{host}:{port}/{database}"

        print('test 1')
        engine = sa.create_engine(conn_str)
        print('test 3')
        
       
        print('test 2')
        print(engine)

        # Nombre de la tabla en Redshift donde se cargarán los datos
        table_name = 'nasdaq_table'
        schema_name = 'marianolopez7749_coderhouse'

        with open(dag_path+'/processed_data/datos_finales.csv', "r") as datos_finales:
            dt = pd.read_csv(datos_finales)
               
        # Cargar el DataFrame en la tabla de Redshift
            dt.to_sql(
                name=table_name,
                schema=schema_name,
                con=engine,
                if_exists='replace',
                index=False
            )
              
            print("Datos cargados exitosamente en Redshift.")

    except Exception as e:
        print(f"Error al cargar los datos en Redshift: {e}")

        traceback.print_exc()  # Print the full exception traceback for debugging purposes

    finally:
        if 'conn' in locals() and engine is not None:
            try:
                
                engine.close()
                
            except Exception as e:
                print(f"Error al cerrar la conexión: {e}")
                traceback.print_exc()

        if 'engine' in locals() and engine is not None:
           engine.dispose()


def envio_mail(**context):

    subjet = 'Email tarea completada'
    sender = context['var']['value'].get('Email_origen')
    recipients = context['var']['value'].get('Email_destino')
    password = context['var']['value'].get('Email_password')

    msg = MIMEText('Proceso de actualización de base finalizado con éxito')
    msg['Subject'] = subjet
    msg['From'] = sender
    msg['To'] = recipients

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())
    print('Mensaje enviado por email')

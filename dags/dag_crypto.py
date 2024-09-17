from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
from airflow.models import Variable



redshift_host = Variable.get("REDSHIFT_HOST")
redshift_db = Variable.get("REDSHIFT_DB")
redshift_user = Variable.get("REDSHIFT_USER")
redshift_password = Variable.get("REDSHIFT_PASSWORD")

smtp_server = Variable.get("SMTP_SERVER")
smtp_port = Variable.get("SMTP_PORT")
email_user = Variable.get("EMAIL_USER")
email_password = Variable.get("EMAIL_PASSWORD")
email_from = Variable.get("EMAIL_FROM")
email_to = Variable.get("EMAIL_TO")


def fetch_crypto_data():
    cryptos = ["bitcoin", "ethereum", "tether", "ripple"]
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {'vs_currency': 'usd', 'ids': ','.join(cryptos)}
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        df_filtered = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'high_24h', 'low_24h', 
                          'price_change_24h', 'price_change_percentage_24h', 'market_cap_change_24h', 
                          'market_cap_change_percentage_24h', 'circulating_supply', 'total_supply', 
                          'ath', 'ath_change_percentage', 'last_updated']]
        df_filtered.columns = ['ID', 'Symbol', 'Name', 'Current_Price', 'Market_Cap', 'Total_Volume', 'High_24h', 'Low_24h', 
                               'Price_Change_24h', 'Price_Change_Percentage_24h', 'Market_Cap_Change_24h', 
                               'Market_Cap_Change_Percentage_24h', 'Circulating_Supply', 'Total_Supply', 'Ath', 
                               'Ath_Change_Percentage', 'DateTime']
        
        
        print(df_filtered)
        return df_filtered.to_dict('records')
    else:
        print("Error al recuperar datos. Reintentando en 10 minutos...")
        time.sleep(10 * 60)
        return fetch_crypto_data()


def combine_api_csv(**context):
    crypto_data = context['ti'].xcom_pull(task_ids='fetch_data')  
    df_api = pd.DataFrame(crypto_data)

    csv_path = '/opt/airflow/dags/crypto_data_CSV.csv'
    df_csv = pd.read_csv(csv_path)
    print (df_csv)

    combined_data = pd.concat([df_csv, df_api], ignore_index=True)
    print(combined_data)
    combined_data_cleaned = combined_data.dropna(subset=['Symbol', 'Current_Price'])
    print(combined_data_cleaned)
    context['ti'].xcom_push(key='combined_data', value=combined_data_cleaned.to_dict('records'))  


def load_to_redshift(**context):
    combined_data = context['ti'].xcom_pull(key='combined_data', task_ids='combine_api_csv')  
    df = pd.DataFrame(combined_data)
    
    df = df.dropna(subset=['Symbol', 'Name', 'Current_Price'])
    df.fillna(value={
        'Market_Cap': 0,
        'Total_Volume': 0,
        'High_24h': 0,
        'Low_24h': 0,
        'Price_Change_24h': 0,
        'Price_Change_Percentage_24h': 0,
        'Market_Cap_Change_24h': 0,
        'Market_Cap_Change_Percentage_24h': 0,
        'Circulating_Supply': 0,
        'Total_Supply': 0,
        'Ath': 0,
        'Ath_Change_Percentage': 0,
    }, inplace=True)

    try:
        conn = psycopg2.connect(
            host=redshift_host,
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            port='5439'
        )
        print("Conectado a Redshift correctamente")
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}")
        return

    try:
        with conn.cursor() as cur:
            # Crear tabla si no existe
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crypto_data (
                    ID VARCHAR(255),
                    Symbol VARCHAR(50) NOT NULL,
                    Name VARCHAR(255) NOT NULL,
                    Current_Price DECIMAL(18, 8) NOT NULL,
                    Market_Cap DECIMAL(38, 2) NOT NULL,
                    Total_Volume DECIMAL(38, 2),
                    High_24h DECIMAL(18, 8),
                    Low_24h DECIMAL(18, 8),
                    Price_Change_24h DECIMAL(18, 8),
                    Price_Change_Percentage_24h DECIMAL(5, 2),
                    Market_Cap_Change_24h DECIMAL(38, 2),
                    Market_Cap_Change_Percentage_24h DECIMAL(5, 2),
                    Circulating_Supply DECIMAL(38, 2),
                    Total_Supply DECIMAL(38, 2),
                    Ath DECIMAL(18, 8),
                    Ath_Change_Percentage DECIMAL(5, 2),
                    DateTime TIMESTAMP,
                    Extracted_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (Symbol, DateTime)
                );
            """)
            conn.commit()
            print("Tabla creada correctamente")

            # Insertar datos en Redshift
            execute_values(
                cur,
                """
                INSERT INTO crypto_data (
                    ID, Symbol, Name, Current_Price, Market_Cap, Total_Volume, High_24h, Low_24h,
                    Price_Change_24h, Price_Change_Percentage_24h, Market_Cap_Change_24h,
                    Market_Cap_Change_Percentage_24h, Circulating_Supply, Total_Supply,
                    Ath, Ath_Change_Percentage, DateTime
                ) VALUES %s
                """,
                [tuple(row) for row in df.itertuples(index=False, name=None)]
            )
            conn.commit()
            print("Datos cargados correctamente en Redshift")
    except Exception as e:
        print(f"Error cargando los datos en Redshift: {e}")
    finally:
        conn.close()

def send_alerts(**context):
    crypto_data = context['ti'].xcom_pull(key='combined_data', task_ids='combine_api_csv')
    with open('dags/config.json', 'r') as json_config:
        config = json.load(json_config)
    print(crypto_data)
    for crypto in crypto_data:
        symbol = crypto['Symbol']
        min_t = config['thresholds'][symbol]['min']
        max_t = config['thresholds'][symbol]['max']
        current_price = crypto['Current_Price']

        if pd.isna(symbol):
            print("Skipping row with missing symbol.")
            continue

        if current_price < min_t:
            subject = f"Alerta: {symbol} está por debajo del rango permitido!"
            message_content = f"El precio de {symbol} es {current_price}, por debajo de lo qeu estableciste como mínimo:{min_t}."
            send_alert_email(subject, message_content)
        
        if current_price > max_t:
            subject = f"Alerta: {symbol} está por encima del rango permitido!"
            message_content = f"El precio de {symbol} es {current_price}, por debajo de lo que estableciste como máximo {max_t}."
            send_alert_email(subject, message_content)

# Función para enviar correos de alerta
def send_alert_email(subject, message_content):
    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = email_to
    msg['Subject'] = subject
    msg.attach(MIMEText(message_content, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email_user, email_password)
        server.sendmail(email_from, email_to, msg.as_string())
        server.quit()
        print('Correo de alerta enviado con éxito')
    except Exception as e:
        print(f"Error al enviar correo: {e}")


default_args = {
    'owner': 'Federico',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

crypto_dag = DAG(
    dag_id="crypto_etl_pipeline",
    default_args=default_args,
    description="DAG para ETL de criptomonedas con carga en Redshift y alertas",
    start_date=datetime(2024, 9, 11),
    schedule_interval='@daily',
    catchup=True 
)

Inicio = BashOperator(task_id='primera_tarea',
    bash_command='echo Iniciando...',
    dag=crypto_dag
)



fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_crypto_data,
    dag=crypto_dag,
)

combine_api_csv_task = PythonOperator(
    task_id='combine_api_csv',
    python_callable=combine_api_csv,
    provide_context=True,
    dag=crypto_dag,
)

load_to_redshift_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=crypto_dag,
)

send_alerts_task = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts,
    provide_context=True,
    dag=crypto_dag,
)

Fin = BashOperator(
    task_id= 'ultima_tarea',
    bash_command='echo Proceso completado...',
    dag=crypto_dag
)

Inicio  >> fetch_data_task >> combine_api_csv_task >> load_to_redshift_task >> send_alerts_task >> Fin

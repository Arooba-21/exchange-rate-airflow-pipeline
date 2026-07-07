from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import requests
import os

dag = DAG(
    'Exchange_Rate_API',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False
)

def extract():
    API_KEY = os.getenv("EXCHANGE_API_KEY")
    print(f"API KEY loaded: {API_KEY is not None}")  
    
    url = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD'
    response = requests.get(url)
    data = response.json()
    
    print(f"API Response keys: {data.keys()}")  
    print(f"Result: {data.get('result')}")       
    
    target_currencies = ["PKR", "EUR", "GBP", "AED", "SAR", "CNY", "INR", "JPY"]
    rates = data["conversion_rates"]
    filtered_rates = {k: v for k, v in rates.items() if k in target_currencies}
    
    df = pd.DataFrame(list(filtered_rates.items()), columns=["currency", "rate_vs_usd"])
    df["pkr_rate"] = rates["PKR"]  # transform ke liye save karo
    
    df.to_csv('/opt/airflow/data/extracted.csv', index=False)
    print(f"Extracted {df.shape[0]} rows")

def transform():
    df = pd.read_csv('/opt/airflow/data/extracted.csv')
    
    df["rate_vs_pkr"] = (df["pkr_rate"] / df["rate_vs_usd"]).round(2)
    df["fetched_at"] = datetime.now()
    df["base_currency"] = "USD"
    df.drop(columns=["pkr_rate"], inplace=True)  # extra column hata do
    
    df.to_csv('/opt/airflow/data/transformed.csv', index=False)
    print(f"Transformed {df.shape[0]} rows")

def load():
    df = pd.read_csv('/opt/airflow/data/transformed.csv')
    
    db_password = os.getenv("DB_PASSWORD")
    engine = create_engine(
        f'postgresql://postgres:{db_password}@host.docker.internal:5432/airflow_data'
    )
    
    df.to_sql('Exchangerate_airflow', engine, if_exists='append', index=False)
    print(f"Loaded {df.shape[0]} rows to PostgreSQL")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

extract_task >> transform_task >> load_task
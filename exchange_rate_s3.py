from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import boto3
import json
import os
from sqlalchemy import create_engine
from io import StringIO

dag = DAG(
    'exchange_rate_s3_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
)

# LocalStack S3 connection
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

def extract():
    api_key = os.environ.get('EXCHANGE_API_KEY')
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    
    response = requests.get(url)
    data = response.json()
    
    # Raw data S3 mein save karo
    s3 = get_s3_client()
    
    try:
        s3.create_bucket(Bucket='exchange-rate-bucket')
        print("Bucket created")
    except Exception as e:
        print(f"Bucket already exists: {e}")
    
    raw_data = json.dumps(data)
    filename = f"raw/exchange_rate_{datetime.now().strftime('%Y%m%d')}.json"
    
    s3.put_object(
        Bucket='exchange-rate-bucket',
        Key=filename,
        Body=raw_data
    )
    print(f"Raw data saved to S3: {filename}")

def transform():
    s3 = get_s3_client()
    
    # S3 se latest file padho
    filename = f"raw/exchange_rate_{datetime.now().strftime('%Y%m%d')}.json"
    
    response = s3.get_object(
        Bucket='exchange-rate-bucket',
        Key=filename
    )
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Transform
    currencies = ['PKR', 'EUR', 'GBP', 'AED', 'SAR', 'CNY', 'INR', 'JPY']
    rates = data['conversion_rates']
    
    df = pd.DataFrame([{
        'date': datetime.now().strftime('%Y-%m-%d'),
        'base_currency': 'USD',
        'currency': cur,
        'rate': rates.get(cur)
    } for cur in currencies])
    
    # Transformed data bhi S3 mein save karo
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket='exchange-rate-bucket',
        Key=f"processed/exchange_rate_{datetime.now().strftime('%Y%m%d')}.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"Transformed {len(df)} rows saved to S3")

def load():
    s3 = get_s3_client()
    
    # S3 se processed file padho
    filename = f"processed/exchange_rate_{datetime.now().strftime('%Y%m%d')}.csv"
    response = s3.get_object(
        Bucket='exchange-rate-bucket',
        Key=filename
    )
    
    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    
    # PostgreSQL mein load
    password = os.environ.get('DB_PASSWORD')
    engine = create_engine(
        f'postgresql://postgres:{password}@host.docker.internal:5432/airflow_data'
    )
    
    df.to_sql('exchange_rates_s3', engine, if_exists='append', index=False)
    print(f"Loaded {len(df)} rows to PostgreSQL")

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
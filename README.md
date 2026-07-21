## PKR Exchange Rate ETL Pipeline (Airflow)

Automated ETL pipeline that fetches live PKR exchange rates daily using Apache Airflow and Docker.

## Pipeline Architecture
```
ExchangeRate API (JSON) → Extract → Transform → Load → PostgreSQL
API → S3 (raw) → Transform → S3 (processed) → PostgreSQL
```
## Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=flat&logo=sqlalchemy&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=flat&logo=amazons3&logoColor=white)
![LocalStack](https://img.shields.io/badge/LocalStack-8C4FFF?style=flat&logo=localstack&logoColor=white)
![boto3](https://img.shields.io/badge/boto3-FF9900?style=flat&logo=amazonaws&logoColor=white)
## DAG Structure
3 isolated tasks running daily:
- **Extract** — Fetches live rates from ExchangeRate-API for 8 target currencies
- **Transform** — Calculates PKR equivalent rates, adds timestamp
- **Load** — Appends to PostgreSQL (history preserved)

## Currencies Tracked
PKR, USD, EUR, GBP, AED, SAR, CNY, INR, JPY

## Setup

1. Clone the repo
2. Create `.env` file:
```
EXCHANGE_API_KEY=your_key
DB_PASSWORD=your_password
```
3. Run:
```bash
docker-compose up -d
```
4. Open `localhost:8080` — login with your credentials
5. Enable `Exchange_Rate_API` DAG

## Project Structure
```
├── dags/
│   └── Exchange_rate_dag.py          # Airflow DAG
├── docker-compose.yaml
├── .gitignore
└── README.md
```

## Key Concepts Practiced
- Airflow DAGs, Tasks, PythonOperator
- Docker Compose for Airflow + PostgreSQL setup
- Securing credentials via .env + docker env_file
- Inter-task data passing via intermediate CSV files
- Append mode loading for historical data preservation

## Airflow UI
![DAG Success](airflow_success.PNG)


import psycopg2
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def conf_db():
    conn = psycopg2.connect(dbname="postgres", user="airflow", password="airflow", host="host.docker.internal")
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
    cur = conn.cursor()

    schema_name = "churn"
    cur.execute(f"""CREATE DATABASE {schema_name};""")
    conn.commit()

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname="churn", user="airflow", password="airflow", host="host.docker.internal")
    cur = conn.cursor()


    sql_create_churn_raw = """
    CREATE TABLE IF NOT EXISTS churn_raw (
        id SERIAL PRIMARY KEY,
        customer_id VARCHAR(32),
        gender VARCHAR(32),
        SeniorCitizen INT,
        Partner VARCHAR(32),
        Dependents VARCHAR(32),
        tenure INT,
        PhoneService VARCHAR(32),
        MultipleLines VARCHAR(32),
        InternetService VARCHAR(32),
        OnlineSecurity VARCHAR(32),
        OnlineBackup VARCHAR(32),
        DeviceProtection VARCHAR(32),
        TechSupport VARCHAR(32),
        StreamingTV VARCHAR(32),
        StreamingMovies VARCHAR(32),
        Contract VARCHAR(32),
        PaperlessBilling VARCHAR(32),
        PaymentMethod VARCHAR(32),
        MonthlyCharges FLOAT,
        TotalCharges FLOAT,
        Churn VARCHAR(32),
        hist_date VARCHAR(32)
    );
    """

    sql_create_churn_processed = f"""
    CREATE TABLE IF NOT EXISTS churn_processed (
        id  SERIAL PRIMARY KEY,
        customer_id  VARCHAR(32),
        seniorcitizen  int,
        tenure  int,
        monthlycharges float,
        totalcharges  float,
        hist_date  VARCHAR(32),
        gender_Male  int,
        partner_Yes  int,
        dependents_Yes  int,
        phoneservice_Yes  int,
        multiplelines_No_phone_service  int,
        multiplelines_Yes  int,
        internetservice_Fiber_optic  int,
        internetservice_No  int,
        onlinesecurity_No_internet_service  int,
        onlinesecurity_Yes  int,
        onlinebackup_No_internet_service  int,
        onlinebackup_Yes  int,
        deviceprotection_No_internet_service  int,
        deviceprotection_Yes int,
        techsupport_No_internet_service int,
        techsupport_Yes int,
        streamingtv_No_internet_service  int,
        streamingtv_Yes int,
        streamingmovies_No_internet_service  int,
        streamingmovies_Yes  int,
        contract_One_year int,
        contract_Two_year int,
        paperlessbilling_Yes  int,
        paymentmethod_Credit_card_automatic int,
        paymentmethod_Electronic_check int,
        paymentmethod_Mailed_check  int,
        target int
    );
    """

    sql_create_models = f"""
    CREATE TABLE IF NOT EXISTS models (
        model_id SERIAL PRIMARY KEY,
        model_name VARCHAR(255) NOT NULL,
        model_params JSONB,
        model_metrics JSONB
    );
    """

    sql_create_predictions = f"""
    CREATE TABLE IF NOT EXISTS predictions (
        prediction_id SERIAL PRIMARY KEY,
        customer_id VARCHAR(32),
        hist_date VARCHAR(32),
        model_name VARCHAR(32),
        prediction FLOAT NOT NULL
    );
    """

    for sql_statement in [
        sql_create_churn_raw,
        sql_create_churn_processed,
        sql_create_models,
        sql_create_predictions
    ]:
        cur.execute(sql_statement)

    conn.commit()

    cur.close()
    conn.close()

with DAG(
    dag_id='config_db_v8',
    description='',
    start_date=datetime(2024, 4, 28),
    schedule_interval='@once'
) as dag:
    task1 = PythonOperator(
        task_id='conf',
        python_callable=conf_db
    )
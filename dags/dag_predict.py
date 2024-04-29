import os
import pandas as pd
import psycopg2
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import lightgbm as lgb


default_args = {
    'owner': 'mosula',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def predict():
    conn = psycopg2.connect(dbname="churn", user="airflow", password="airflow", host="host.docker.internal")
    cur = conn.cursor()

    df = pd.read_sql(
    """ 
    select * from churn_processed
    """, con=conn
    )

    with open('predict_config.json') as file:
        config = json.load(file)

    model = lgb.Booster(model_file=os.path.join('models', config['model_name']))

    df['prediction'] = model.predict(df.drop(columns=['id', 'customer_id', 'hist_date', 'target']))
    df = df.loc[:, ['customer_id', 'hist_date', 'prediction']]
    df['model_name'] = config['model_name']

    columns_str = str(tuple(df.columns)).replace("'", "")
    for _, row in df.iterrows():
        sql = f"""
            INSERT INTO predictions {columns_str}
            VALUES {tuple(row)}
        """
        cur.execute(sql)
    
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    default_args=default_args,
    dag_id='predict_v14',
    description='',
    start_date=datetime(2024, 4, 28),
    schedule_interval='@monthly'
) as dag:
    task1 = PythonOperator(
        task_id='train',
        python_callable=predict
    )
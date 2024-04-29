import os
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'mosula',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def fill_raw(data_name):
    print(os.listdir())
    df = pd.read_csv(os.path.join(os.getcwd(), 'data', data_name))
    df = df.rename(columns={'customerID': 'customer_id'})
    df['hist_date'] = pd.Timestamp.today().strftime('%Y-%m-%d')
    df['MonthlyCharges'] = df['MonthlyCharges'].astype('float64')
    df['TotalCharges'] = df['TotalCharges'].replace(' ', '0').astype('float64')

    conn = psycopg2.connect(dbname="churn", user="airflow", password="airflow", host="host.docker.internal")
    cur = conn.cursor()

    columns_str = str(tuple(df.columns)).replace("'", "")
    for _, row in df.iterrows():
        sql = f"""
            INSERT INTO churn_raw {columns_str}
            VALUES {tuple(row)}
        """
        cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()


def fill_processed():
    conn = psycopg2.connect(dbname="churn", user="airflow", password="airflow", host="host.docker.internal")
    cur = conn.cursor()
    df = pd.read_sql(""" 

    select *
    from churn_raw
    where hist_date=(select max(hist_date) from churn_raw)
    """, con=conn)

    df_proc = pd.get_dummies(df, columns=['gender', 'partner', 'dependents',
                                                'phoneservice', 'multiplelines', 
                                                'internetservice', 'onlinesecurity',
                                                'onlinebackup', 'deviceprotection',
                                                'techsupport', 'streamingtv', 
                                                'streamingmovies', 'contract',   
                                                'paperlessbilling', 'paymentmethod', 'churn'], drop_first=True)
    df_proc.columns = df_proc.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    df_proc = df_proc.rename(columns={'churn_Yes': 'target'})

    for col in df_proc.columns:
        if df_proc[col].dtype == 'bool':
            df_proc[col] = df_proc[col].astype('int16')

    columns_str = str(tuple(df_proc.columns)).replace("'", "")
    for _, row in df_proc.iterrows():
        sql = f"""
            INSERT INTO churn_processed {columns_str}
            VALUES {tuple(row)}
        """
        cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    default_args=default_args,
    dag_id='fill_data_v11',
    description='',
    start_date=datetime(2024, 4, 28),
    schedule_interval='@monthly'
) as dag:

    task1 = PythonOperator(
        task_id='raw',
        python_callable=fill_raw,
        op_kwargs={'data_name': 'data.csv'}
    )

    task2 = PythonOperator(
        task_id='processed',
        python_callable=fill_processed
    )
    task1 >> task2
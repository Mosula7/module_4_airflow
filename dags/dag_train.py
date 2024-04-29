from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import json
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


def split_data(df: pd.DataFrame, target: str, test_size: float, 
               val_size: float=None, random_state:int = 0):
    """
    returns (X_train, y_train, X_val, y_val, X_test, y_test)
    """
    if not val_size:
        val_size = test_size / (1 - test_size)

    train_val, test = train_test_split(df, test_size=test_size, stratify=df[target], random_state=random_state)
    train, val = train_test_split(train_val, test_size=val_size, stratify=train_val[target], random_state=random_state)

    X_train = train[train.columns.drop(target)]
    X_val = val[val.columns.drop(target)]
    X_test = test[test.columns.drop(target)]

    y_train = train[target]
    y_val = val[target]
    y_test = test[target]
    
    return X_train, y_train, X_val, y_val, X_test, y_test


def train_model():
    conn = psycopg2.connect(dbname="churn", user="airflow", password="airflow", host="host.docker.internal")
    cur = conn.cursor()

    df = pd.read_sql(
    """ 
    select * from churn_processed
    """, con=conn
    ).drop(columns=['id', 'customer_id', 'hist_date'])
    X_train, y_train, X_val, y_val, X_test, y_test = split_data(df, test_size=0.15, target='target')

    with open('model_config.json') as file:
        config = json.load(file)

    params = {}
    for key, value in config.items():
        params[key] = value
    
    model = lgb.LGBMClassifier(**params)
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)])

    metrics = {}
    for key, data in {'train': [X_train, y_train], 'val': [X_val, y_val], 'test': [X_test, y_test]}.items():
        pred = model.predict_proba(data[0])[:,-1]

        auc = roc_auc_score(data[1], pred)
        acc = accuracy_score(data[1], pred>.5)

        metrics[f'{key}_auc'] = auc
        metrics[f'{key}_acc'] = acc

    model_name = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
    sql = f"""
        INSERT INTO models (model_name, model_params, model_metrics)
        VALUES ('{model_name}', '{str(params).replace("'", '"')}', '{str(metrics).replace("'", '"')}')
    """
    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()

    model.booster_.save_model(os.path.join('models', f'model_{model_name}.txt'))


with DAG(
    default_args=default_args,
    dag_id='train_model_v8',
    description='',
    start_date=datetime(2024, 4, 28),
    schedule_interval='@monthly'
) as dag:
    task1 = PythonOperator(
        task_id='train',
        python_callable=train_model
    )

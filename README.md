The Project Consists of four main dags, please run them in the order listed here:
* configure_database - makes four tables for raw data, processed data, model training logs and predictions under churn database.
* raw_processed - 1. uploads the raw data into the churn_raw table 2. takes the data from churn_raw table makes processes it so model can be trained on this data.
* train - trains the model on the data from churn_processed and trains a model on it logs performance to models table and saves the model booster txt in the models folder. takes hyperparameters from model_config file.
* predict - takes the data from churn_processed makes predictions on it and saves it in the predictions table. it takes the model name from predict_config file.

docker-compose.yml - makes volumes for logs, dags, config_files, data and models folder, makes a postgress database.

to run the container first run:
```
docker-compose up airflow-init
```
After this is done to run all the contaiers you can run
```
dcoker-compose up -d
```

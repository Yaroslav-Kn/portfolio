#!/bin/sh
source ./.env
export AIRFLOW_PROJ_DIR=$AIRFLOW_PROJ_DIR
export AIRFLOW_UID=$AIRFLOW_UID
export TEL_TOKEN=$TEL_TOKEN
export TEL_CHAT_ID=$TEL_CHAT_ID

cd airflow
mkdir -p ./dags ./logs ./plugins ./config 
docker-compose up airflow-init -d
docker-compose down --volumes --remove-orphans 
docker-compose up --build -d
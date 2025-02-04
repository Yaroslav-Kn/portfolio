#!/bin/sh
cd airflow
mkdir -p ./dags ./logs ./plugins ./config 
docker-compose up airflow-init -d
docker-compose down --volumes --remove-orphans 
docker-compose up --build -d
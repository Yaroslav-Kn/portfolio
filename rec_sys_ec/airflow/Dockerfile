FROM apache/airflow:2.7.3-python3.10

USER root
# Обновление списка пакетов
RUN apt-get update
# Установка gcc и libgomp-dev
RUN apt-get install -y gcc
RUN apt-get install -y libgomp1
# Очистка временных файлов
RUN rm -rf /var/lib/apt/lists/*

USER airflow
# Копируем и устанавливаем требования
COPY requirements.txt ./tmp/requirements.txt 
RUN pip install -U pip
RUN pip install -r ./tmp/requirements.txt 
#!/bin/bash

# Установим провайдеры для работы с MongoDB и PostgreSQL.
pip install \
  apache-airflow-providers-mongo \
  apache-airflow-providers-postgres

# Создаем пользователя Airflow.
airflow db migrate
airflow users create \
  --username etl_user \
  --password etl_password \
  --firstname etl_user \
  --lastname etl_user \
  --role Admin \
  --email etl@example.local || true

# Создаём соединение к PostgreSQL
airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host sql \
  --conn-schema etl_database \
  --conn-login etl_user \
  --conn-password etl_password \
  --conn-port 5432 || true

# Создаём connection к MongoDB
airflow connections add mongo_default \
  --conn-type mongo \
  --conn-host nosql \
  --conn-port 27017 \
  --conn-schema etl_database \
  --conn-login etl_user \
  --conn-password etl_password \
  --conn-extra '{"authSource": "admin"}' || true

# Запускаем сам Airflow (планировщик и веб-сервер).
airflow triggerer &
airflow scheduler &
airflow dag-processor &
airflow api-server

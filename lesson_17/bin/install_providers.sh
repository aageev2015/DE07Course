#!/usr/bin/env bash
# use this script to install airflow providers

AIRFLOW_VERSION=$(airflow version | cut -d " " -f 2 | cut -d "." -f 1-3)
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing providers on Apache Airflow ${AIRFLOW_VERSION}"
echo "Your active Python environment: $(which python)"
echo "PYTHON_VERSION = ${AIRFLOW_VERSION}"
echo "CONSTRAINT_URL = ${CONSTRAINT_URL}"

echo "apache-airflow-providers-google"
pip install "apache-airflow-providers-google==8.3.0" --upgrade --constraint "${CONSTRAINT_URL}"
echo "apache-airflow-providers-apache-hdfs"
pip install "apache-airflow-providers-apache-hdfs==3.1.0" --upgrade --constraint "${CONSTRAINT_URL}"
echo "apache-airflow-providers-apache-spark"
pip install "apache-airflow-providers-apache-spark==3.0.0" --upgrade --constraint "${CONSTRAINT_URL}"
echo "apache-airflow-providers-amazon"
pip install "apache-airflow-providers-amazon==5.0.0" --upgrade --constraint "${CONSTRAINT_URL}"

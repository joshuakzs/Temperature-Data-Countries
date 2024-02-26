Project Purpose: This project's purpose is meant for me to learn and practise creating a Postgres SQL database, use Apache Airflow, ETL.
Project Overview:

1. Create a Postgres SQL database, and create tables to collect data.
2. Using a Python VE, install python packages and apache airflow
   * numpy, pandas, requests, selenium, seaborn, psycopg2
   * pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
4. Initialize airflow metadata database
   * airflow db init
6. Connect to local Postgres SQL database 
7. Using Airflow scheduler, the scripts will collect data and put them in our database tables. Using selenium or API, data will be collected hourly from the following countries:
   * Singapore
   * Malaysia
9. The Airflow DAG will be configured such that if data collected is unsuccessful, an email will be sent to me.

Possible future expansion of the project:
1. Scheduled Data Cleaning of each countries' data
2. Scheduled Merging the different countries' data
3. Scheduled Update of Data Visualisation of data
4. Adding more countries



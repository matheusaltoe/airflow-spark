from airflow import DAG
from airflow.operators import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Sales of diesel by UF and type"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-sales-of-diesel-by-uf-and-type", 
        description="This DAG runs a simple Pyspark app.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )


spark_job = BashOperator(
    task_id="spark_job_sales_of_diesel_by_uf_and_type",
    bash_command='spark-submit --master spark://spark:7077 /usr/local/spark/app/sales_of_diesel_by_uf_and_type.py /usr/local/spark/resources/data/airflow.cfg',
    name=spark_app_name,
    verbose=1,
    dag=dag)


spark_job
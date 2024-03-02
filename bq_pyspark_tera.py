from airflow import DAG
from airflow.operators.python_operator import Pythonoperator 
from datetime import datetime 
from pyspark.sql import SparkSession

def failure_email_function(context):
    dag_run = context.get("dag_run")
    task_instances = dag_run.get_task_instances()
    ti = context [ 'task_instance']
    exception_html = context.get("exception")
    log_url = ti.log_url
    mark_success_url = ti.mark_success_url
    msg = f"""The DAG run to load serverless G15 table failed. Please verify ‹br›
            Exception: ‹br›{exception_html}<br›
            Log: <a href="(log_url}">Link</a>br>
            Mark success: <a href="(mark_success_url}"›Link</a>‹br›"""
            
    subject = f"[FAILED] DAG {dag_ run.dag_id} has failed"
    send_email(to=['akila@gmail.com'], subject=subject, html_content=msg)
    
def success_email_function(context):
    dag_run = context.get('dag_run')
    task instances = dag_run.get_task_instances()
    msg ="The DAG run to load serverless G15 table has completed successfully"
    subject = f"[SUCCESS] DAG {dag_run.dag_id} has completed"
    send_email(to=['akila@gmail.com'], subject=subject, html_content=msg)
    
default_args = {
    'owner': ' CDM',
    'depends _on_past': False,
    'email_on retry'; False,
    'retries': 0,
    'retry _delay': timedelta(minutes=5)
    }
    
default_args = {
    'owner':'airflow',
    'start_date'; datetime(2022, 1, 1)
}

with DAG(
    'BQ-TO-TERADATA-DATA',
    catchup = False,
    start_date = datetime(2022, 11, 2), 
    max_active_runs=1,
    default_args=default_args, 
    schedule_interval='@once',
) as dag:

def load data():
    project_id = 'prj-id'
    dataset_name = 'bq_dataset"
    table_name = 'flight_vw'
    jdbc_url = 'jdbc:teradata://teraqa-app.com/database=data'
    driver="com.teradata.jdbc.TeraDriver"
    bucket_name = "prj-bucket-gcp"
    blob_name="credential.txt"
    cred = write_read (bucket_name, blob_name)
    user_pwd_vals=cred.decode('utf-8').split( '\r\n)
    username = user_pwd_vals[0]
    password = user_pwd_vals [1]
    
    #Set up a Spark session

    spark = SparkSession.builder.appName('bigquery_to_teradata').getOrCreate()
    
    # Set up a Bigluery client and extract data
    client = bigquery.Client (project=project_id)
    table_ref = client.dataset (dataset_name).table (table _name)
    table = client.get_table(table_ref)
    data = client.list_rows (table).to_dataframe()
    
    # Create a temporary Spark DataFrame from the extracted data
    df = spark.createDataFrame(data)
    
    # Write the data to Teradata using JDBC
    df.write \
        .format ('jdbc') \
        .option ('url', jdbc_url) \
        .option ('data', 'TEST') \
        .option ('user', username) \
        .option ('password', password) \
        .mode ('overwrite')
        .save()
        
load_data_task = PythonOperator (
    task_id='load_data',
    python_callable=load_data,on_failure_callback=failure_email_function,
    dag=dag
)

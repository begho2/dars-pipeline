import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

from functions import s3_read

from airflow.hooks.S3_hook import S3Hook

args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule': None
}

dag = DAG(os.path.basename(__file__),
          default_args=args,
          description="""
          load a sample of ae zip file and transform to postgres. Change sample size with {"limit":"1000"}
          {"limit":"0"} for no limit
          """,
          schedule_interval=None
          )

# print(os.environ.get('TEST_INPUT'))
# dir = os.environ.get('TEST_INPUT') if os.environ.get('TEST_INPUT') else "./input_data"
# files = os.listdir(dir)
# zip_files = []
# for filename in files:
#     if filename.endswith(".zip"):
#         zip_files.append(filename)

# Try hard coding
zip_files = [
    # "NIC243790_HES_APC_201499.zip",
    "NIC243790_HES_APC_201599.zip",
    "NIC243790_HES_APC_201699.zip",
    # "NIC243790_HES_APC_201799.zip",
    # "NIC243790_HES_APC_201899.zip",
    # "NIC243790_HES_APC_201999.zip",
    # "NIC243790_HES_APC_202005.zip"
]

def create_spark_operator(task, class_name, filename, limit=100):
    runtime_limit = '{{dag_run.conf["limit"] or limit}}'
    driver_memory = '{{"2g" if dag_run.conf["limit"] else "1g"}}'
    print(f"\nFound limit {runtime_limit}\n")
    print(f"\nFound driver_memory {driver_memory}\n")
    hostname="postgres"
    port="5432"
    user="airflow"
    password="airflow"
    db_name="dars"
    return SparkSubmitOperator(
        task_id=f"{task}_{filename[0:-4]}",
        name=f'{task}_{filename}',
        verbose=True,
        application="external_resources/dars-ingest-1.0-SNAPSHOT-jar-with-dependencies.jar",
        java_class=class_name,
        application_args=[f"input_data/{filename}", runtime_limit],
        # driver_memory='1g',
        conf={
            'spark.driver.extraJavaOptions':f'-DDB_HOSTNAME={hostname} -DDB_PORT={port} -DDB_USER={user} -DDB_PASSWORD={password} -DDB_NAME={db_name}'
        }
        # conf={'spark.memory.fraction': '0.1'},
    )

# def list_s3_keys():
#     # Initialize the s3 hook
#     s3_hook = S3Hook()
#     # Read the keys from s3 bucket
#     paths = s3_hook.list_keys(bucket_name='dars-raw', prefix='HES-AE')
#     print(paths)
#     s3_hook.get_key(key="HES-AE/NIC243790_HES_AE_201599.zip", bucket_name="dars-raw").download_file("./input_data/NIC243790_HES_AE_201599.zip")
    
with dag:
    for filename in zip_files:
        key = filename.strip(".zip")
        get_s3 = PythonOperator(
            task_id=f"get_{key}_from_s3",
            python_callable=s3_read.get_s3_files,
            op_kwargs={
                's3_path' : f'HES-APC/{key}.zip',
                'filename' : f'{key}.zip'
            }
        )
        setup_postgres_schema = create_spark_operator(task=f"SetupSchema", class_name='cf.ZipToPostgresSchemaMain' , filename=filename)
        push_to_postgres = create_spark_operator(task=f"InsertData", class_name='cf.ZipToPostgresInsertMain' , filename=filename, limit=100)
        delete_tmp = BashOperator(
            task_id= f'delete_{key}_from_local',
            bash_command=f'rm ~/input_data/{key}.zip'
        )
        get_s3 >> setup_postgres_schema >> push_to_postgres >> delete_tmp
        # setup_postgres_schema >> push_to_postgres

# with dag:
#     for filename in zip_files:
#         key = filename.strip(".zip")
#         list_keys = PythonOperator(
#             task_id="list_s3_keys",
#             python_callable=list_s3_keys,
#         )
#         setup_postgres_schema = create_spark_operator(task=f"SetupSchema", class_name='cf.ZipToPostgresSchemaMain' , filename=filename)
#         push_to_postgres = create_spark_operator(task=f"InsertData", class_name='cf.ZipToPostgresInsertMain' , filename=filename, limit=100)
#         list_keys >> setup_postgres_schema >> push_to_postgres
#         # setup_postgres_schema >> push_to_postgres

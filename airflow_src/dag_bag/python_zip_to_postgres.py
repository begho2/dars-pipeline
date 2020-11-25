import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from src.functions.s3_read import get_s3_files
from src.functions.hes_zip_utils import get_zip_data_generator, SEPARATOR
from src.functions.load_postgres import setup_schema, DB_PROPERTIES, PARTITION_NAME, export_zip_data_to_db

args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule': None
}

dag = DAG(
    os.path.basename(__file__),
    default_args=args,
    description="""
        load a sample of ae zip file and transform to postgres. 
        Change sample size with {"limit":"1000", "table_name": "ae"}{"limit":"0", "table_name": "ae"} for no limit
    """,
    schedule_interval=None
)

# Try hard coding
zip_files = [
    "NIC243790_HES_AE_201499.zip"
    # "NIC243790_HES_APC_201499.zip",
    # "NIC243790_HES_APC_201599.zip",
    # "NIC243790_HES_APC_201699.zip",
    # "NIC243790_HES_APC_201799.zip",
    # "NIC243790_HES_APC_201899.zip",
    # "NIC243790_HES_APC_201999.zip",
    # "NIC243790_HES_APC_202005.zip"
]
    
with dag:

    runtime_limit = '{{dag_run.conf["limit"] or limit}}'
    table_name = '{{dag_run.conf["table_name"] or test}}'
    batch_size = '{{dag_run.conf["batch_size"] or "1000"}}'
    driver_memory = '{{"2g" if dag_run.conf["limit"] else "1g"}}'
    print(f"\nFound limit {runtime_limit}\n")
    print(f"\nFound driver_memory {driver_memory}\n")

    for filename in zip_files:
        key = filename.strip(".zip")
        # get_s3 = PythonOperator(
        #     task_id=f"get_{key}_from_s3",
        #     python_callable=get_s3_files,
        #     op_kwargs={
        #         's3_path' : f'HES-AE/{key}.zip',
        #         'filename' : f'{key}.zip'
        #     }
        # )

        setup_postgres_schema = PythonOperator(
            task_id=f"SetupSchema{key}",
            python_callable=setup_schema,
            op_kwargs={
                'filename': f"hes_input/{filename}",
                'table_name': table_name,
                'DB_PROPERTIES': DB_PROPERTIES,
            }
        )

        push_to_postgres = PythonOperator(
            task_id=f"InsertData{key}",
            python_callable=export_zip_data_to_db,
            op_kwargs={
                'DB_PROPERTIES': DB_PROPERTIES,
                'table_name': table_name,
                'path': f"hes_input/{filename}",
                # 'limit': runtime_limit,
                # 'batch_size': batch_size
            }
        )

        # delete_tmp = BashOperator(
        #     task_id= f'delete_{key}_from_local',
        #     bash_command=f'rm ~/hes_input/{key}.zip'
        # )
        # get_s3 >> setup_postgres_schema >> push_to_postgres >> delete_tmp
        setup_postgres_schema >> push_to_postgres


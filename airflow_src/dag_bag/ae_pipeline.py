import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from typing_extensions import runtime

from src.functions.s3_read import get_s3_files
from src.functions.hes_zip_utils import get_zip_data_generator, SEPARATOR
from src.functions.load_postgres import setup_schema, DB_PROPERTIES, PARTITION_NAME, export_zip_data_to_db
from src.data_catalog.catalog import CATALOG
from src.data_catalog.headings import AE_HEADINGS

args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'schedule': None
}

ae_dag = DAG(
    dag_id='ae_etl',
    default_args=args,
    description="""
        load a sample of ae zip file and transform to postgres. 
    """,
    schedule_interval=None
)

ae_zip_files = list(CATALOG['HES-AE']['s3'].values())


# create and return and DAG
def create_subdag(dag_parent, filename, DB_PROPERTIES, table_name, runtime_limit, batch_size):
    key = filename.strip(".zip")
    # dag params
    dag_id_child = f"{dag_parent.dag_id}.subdag_{key}"
    default_args_copy = args.copy()

    # dag
    dag = DAG(dag_id=dag_id_child,
              default_args=default_args_copy,
              schedule_interval='@once')

    
    # operators
    get_s3 = PythonOperator(
        task_id=f"get_{key}_from_s3",
        dag=dag,
        python_callable=get_s3_files,
        op_kwargs={
            's3_path' : f'HES-AE/{filename}',
            'filename' : filename
        }
    )

    push_to_postgres = PythonOperator(
        task_id=f"insert_data_{key}",
        dag=dag,
        python_callable=export_zip_data_to_db,
        op_kwargs={
            'DB_PROPERTIES': DB_PROPERTIES,
            'table_name': table_name,
            'path': f"hes_input/{filename}",
            'limit': runtime_limit,
            'batch_size': batch_size
        }
    )

    delete_tmp = BashOperator(
        task_id= f'delete_{key}_from_local',
        dag=dag,
        bash_command=f'rm ~/hes_input/{filename}'
    )

    get_s3 >> push_to_postgres >> delete_tmp
    # push_to_postgres >> delete_tmp
    return dag

# wrap DAG into SubDagOperator
def create_subdag_operator(dag_parent, filename, DB_PROPERTIES, table_name, runtime_limit, batch_size):
    tid_subdag = f'subdag_{filename.strip(".zip")}'
    subdag = create_subdag(dag_parent, filename, DB_PROPERTIES, table_name, runtime_limit, batch_size)
    sd_op = SubDagOperator(task_id=tid_subdag, dag=dag_parent, subdag=subdag)
    return sd_op

# create SubDagOperator for each db in db_names
def create_all_subdag_operators(dag_parent, filenames, DB_PROPERTIES, table_name, runtime_limit, batch_size):
    subdags = [create_subdag_operator(dag_parent, filename, DB_PROPERTIES, table_name, runtime_limit, batch_size) for filename in filenames]
    # chain subdag-operators together
    chain(*subdags)
    return subdags
    
with ae_dag:

    table_name = "hes.ae"
    runtime_limit = 0
    batch_size = 100000

    setup_postgres_schema = PythonOperator(
        task_id=f"setup_schema_{table_name}",
        python_callable=setup_schema,
        op_kwargs={
            'headings': AE_HEADINGS,
            'table_name': table_name,
            'DB_PROPERTIES': DB_PROPERTIES
        }
    )

    sub_dags = create_all_subdag_operators(ae_dag, ae_zip_files, DB_PROPERTIES, table_name, runtime_limit, batch_size)

    setup_postgres_schema >> sub_dags[0]



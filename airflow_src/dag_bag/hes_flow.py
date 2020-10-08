import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime, timedelta

default_arguments = {
    'owner': 'Patrick',
    'email': ['pjboundy@sky.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email_on_success': True,
    'start_date': datetime(2020,6, 26)
}

hes_dag = DAG(os.path.basename(__file__), default_args=default_arguments, schedule_interval='* * 1 * *')

import os
# import boto3
#
# S3_CLIENT = boto3.client(
#     's3',
#     region_name=os.environ.get("AWS_REGION"),
#     aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
#     aws_secret_access_key=os.environ.get("AWS_SECRET_KEY")
# )
#
# def upload_s3_file(file, file_location: str) -> None:
#     S3_CLIENT.upload_file(
#         Filename="s3a://dars-ingested",
#         Bucket='dars-raw',
#         Key='HES-AE'
#     )
#
#
#
#
#
# """
# 1. Get from S3
# 2. Clean
# 3. Load to DB
# 4. Load to S3
# """
#
# read_s3 = BashOperator(
#     task_id='read_from_s3',
#     # bash_command='aws s3 sync data/truncated s3://dars-truncated',
#     bash_command='echo "test_run"',
#     dag=hes_dag
# )
#
# """
# spark-submit
# --master "local[*]"
# --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,org.postgresql:postgresql:42.2.14
# --py-files src.zip
# src/etl.py
# """
#
# ae_spark_task = SparkSubmitOperator(
#     task_id='ae_etl',
#     application='src/run/run_ae.py',
#     py_files='src.zip',
#     conn_id='spark_local',
#     packages='com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,org.postgresql:postgresql:42.2.14',
#     dag=hes_dag
# )
#
# apc_spark_task = SparkSubmitOperator(
#     task_id='apc_etl',
#     application='src/run/run_apc.py',
#     py_files='src.zip',
#     conn_id='spark_local',
#     packages='com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,org.postgresql:postgresql:42.2.14',
#     dag=hes_dag
# )
#
# cc_spark_task = SparkSubmitOperator(
#     task_id='cc_etl',
#     application='src/run/run_cc.py',
#     py_files='src.zip',
#     conn_id='spark_local',
#     packages='com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,org.postgresql:postgresql:42.2.14',
#     dag=hes_dag
# )
#
# op_spark_task = SparkSubmitOperator(
#     task_id='op_etl',
#     application='src/run/run_op.py',
#     py_files='src.zip',
#     conn_id='spark_local',
#     packages='com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2,org.postgresql:postgresql:42.2.14',
#     dag=hes_dag
# )
#
# def generate_report():
#     print("do nothin")
#
# generate_report = PythonOperator(
#     task_id='generate_report',
#     python_callable=generate_report,
#     op_kwargs={},
#     dag=hes_dag
# )
#
#
#
# # email_task = EmailOperator(
# #     task_id='email_report',
# #     to='leo.edwards@carnallfarrar.com',
# #     subject='DARS truncated run complete',
# #     html_content='Attached is the latest run report',
# #     files="../../Output.txt",
# #     dag=truncated_dag
# # )
#
# read_s3 >> ae_spark_task
# read_s3 >> apc_spark_task
# read_s3 >> cc_spark_task
# read_s3 >> op_spark_task
# ae_spark_task >> generate_report
# apc_spark_task >> generate_report
# cc_spark_task >> generate_report
# op_spark_task >> generate_report
# # generate_report >> email_task
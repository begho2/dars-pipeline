import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from operators.cf_spark_submit_operator import CfSparkSubmitOperator

args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule': None
}

dag = DAG(os.path.basename(__file__),
          default_args=args,
          description='load a sample of ae zip file and transform to parq. Change sample size with {"limit":"1000"}',
          schedule_interval=None
          )

# dir = "/Users/patrickboundy/Downloads"
print(os.environ.get('TEST_INPUT'))
dir = os.environ.get('TEST_INPUT') if os.environ.get('TEST_INPUT') else "./input_data"
filename = "NIC243790_HES_AE_201599.zip"


wait_time = 0

with dag:
    if (filename.endswith(".zip")):
        # delta = timedelta(minutes=wait_time)
        key = filename.strip(".zip")
        get_zip = DummyOperator(task_id=f"Download_{key}")
        push_to_postgres = DummyOperator(task_id=f"Load_postgres_{key}")
        postgres_validate = DummyOperator(task_id=f"Validate_postgres_{key}")
        zipToParq = CfSparkSubmitOperator(filename=filename, filelocation=dir, sample="100")
        parqValidate = DummyOperator(task_id=f"Validate_{key}.parq")
        get_zip >> zipToParq >> parqValidate >> push_to_postgres >> postgres_validate
    else:
        raise Exception(f"trying to process file that is not a zip: {filename}")
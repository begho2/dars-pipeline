import glob
import os
import unittest
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.models import DagBag, TaskInstance
from airflow.utils.dates import days_ago

from operators.cf_spark_submit_operator import CfSparkSubmitOperator

"""
Recreate this for the PythonOperator
"""
class TestDagImport(unittest.TestCase):
    def test_imports(self):
        dag_dir = f'{Path(__file__).resolve().parents[1]}/airflow_src/dag_bag'
        dags = [file for file in glob.iglob(dag_dir + '/**/*.py', recursive=True) if ~file.endswith("__init__.py")]
        # dags = [file for file in listdir_recursive(dag_dir)]
        failures = []
        print(f"for dag dir [{dag_dir}], found {dags}")

        os.environ['TEST_INPUT']=f'{Path(__file__).resolve().parents[1]}/hes_input'
        for d in dags:
            print(d)
            with self.subTest(msg=d):
                dag = DagBag(f'{Path(d)}')
                self.assertGreater((len(dag.dag_ids)),0,f'{dag} -> Failed with {dag.dag_ids}')
                self.assertEqual((len(dag.import_errors)),0,f'{dag} -> Failed with {dag.import_errors}')

    def test_spark_submit(self):
        os.environ['TEST_INPUT'] = f'{Path(__file__).resolve().parents[1]}/hes_input'
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
            # SparkSubmitOperator(task_id='spark_submit_job', application="${SPARK_HOME}/examples/src/main/pi.py")
            for f in os.listdir(dir):
                if (f.endswith(".zip")):
                    # delta = timedelta(minutes=wait_time)
                    # waiter = BashOperator(task_id=f"Wait_{wait_time}", bash_command=f"sleep {wait_time}")
                    # wait_time = wait_time + (5 * 60)
                    """
                    How to test an operator
                    """
                    zipToParq = CfSparkSubmitOperator(filename=f, filelocation=dir, sample="100")
                    ti = TaskInstance(task=zipToParq, execution_date=datetime.now())

                    # IN ORDER TO RUN THIS, YOU NEED TO AIRFLOW INITDB. ALSO, SPARK COMMAND WILL FAIL
                    # result = zipToParq.execute(ti.get_template_context())

                    # zipToParq..run()
                    # parqValidate = DummyOperator(task_id=f"Validate_{f.strip('.zip')}.parq")
                    # waiter >> zipToParq >> parqValidate

        # dag.run()


if __name__ == '__main__':
    unittest.main()

import glob
import os
import unittest
from pathlib import Path

from airflow.models import DagBag

class TestDagImport(unittest.TestCase):
    def test_imports(self):
        dag_dir = f'{Path(__file__).resolve().parents[1]}/airflow_src/dag_bag'
        dags = [file for file in glob.iglob(dag_dir + '/**/*.py', recursive=True) if ~file.endswith("__init__.py")]
        # dags = [file for file in listdir_recursive(dag_dir)]
        failures = []

        os.environ['TEST_INPUT']=f'{Path(__file__).resolve().parents[1]}/dars-ingest/hes_zips'
        for d in dags:
            print(d)
            with self.subTest(msg=d):
                dag = DagBag(f'{Path(d)}')
                self.assertGreater((len(dag.dag_ids)),0,f'{dag} -> Failed with {dag.dag_ids}')
                self.assertEqual((len(dag.import_errors)),0,f'{dag} -> Failed with {dag.import_errors}')


if __name__ == '__main__':
    unittest.main()

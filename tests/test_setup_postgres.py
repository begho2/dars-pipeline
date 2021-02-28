# import unittest
# from pathlib import Path

# from functions.db_utils import exec_on_db, select_partition_counts
# from functions.hes_zip_utils import get_zip_data_generator, SEPARATOR
# from functions.load_postgres import setup_schema, DB_PROPERTIES, PARTITION_NAME, export_zip_data_to_db


# class TestSetupPostgres(unittest.TestCase):


#     def test_schema_setup(self):
#         input_path = f'{Path(__file__).resolve().parents[1]}/hes_input/test_ae.zip'
#         # input_path = f'{Path(__file__).resolve().parents[1]}/dars-ingest/hes_zips/NIC243790_HES_AE_201599.zip'
#         header = next(get_zip_data_generator(input_path))
#         DATA_DETAILS = {
#             'table_name': "test_ae",
#             'columns': header.split(SEPARATOR) + [PARTITION_NAME],
#             'partition_column': PARTITION_NAME
#         }
#         setup_schema(DB_PROPERTIES, DATA_DETAILS)


#     def test_insert_data(self):
#         self.test_schema_setup()
#         input_path = f'{Path(__file__).resolve().parents[1]}/hes_input/test_ae.zip'
#         # input_path = f'{Path(__file__).resolve().parents[1]}/dars-ingest/hes_zips/NIC243790_HES_AE_201599.zip'
#         export_zip_data_to_db(DB_PROPERTIES, input_path, 5, 4)
#         DATA_DETAILS = {
#             'table_name': "test_ae",
#             'partition_column': PARTITION_NAME
#         }
#         records = exec_on_db(DB_PROPERTIES, DATA_DETAILS, select_partition_counts)
#         for row in records:
#             print(f"{row[0]}:{row[1]}")


#     def test_insert_data_with_bad_admidate(self):
#         self.test_schema_setup()
#         input_path = f'{Path(__file__).resolve().parents[1]}/hes_input/test_ae_bad_admidate.zip'
#         export_zip_data_to_db(DB_PROPERTIES, input_path, 5, 4)



# if __name__ == '__main__':
#     unittest.main()

# # todo
# # create in memory or test only db for unit tests without docker
# # add validation step for data in particular partitions
# # setup properties and envs
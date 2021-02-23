from src.functions.db_utils import exec_on_db, insert_values, drop_table, create_master_table, create_partitions, \
    analyze_table
from src.functions.db_utils import truncate_table, print_db_connection_details
from src.functions.hes_zip_utils import get_zip_data_generator, batch_datalines_and_process, SEPARATOR

DEBUG = False

PARTITION_NAME = "admi_partition"
PARTITION_COL_CHOICES = ("ARRIVALDATE", "ADMIDATE")
DB_PROPERTIES = {
    "hostname": "postgres",
    "port": "5432",
    "url": f"jdbc:postgresql://postgres:5433/airflow",  # os.environ.get("RDS_URL"),
    "user": "airflow",  # os.environ.get("RDS_USER"),
    "password": "airflow",  # os.environ.get("RDS_PASSWORD"),
    "schema": "public",
    "db_name": "airflow",
    "driver": "org.postgresql.Driver"
}

def setup_schema(headings, table_name, DB_PROPERTIES):
    # exec_on_db(DB_PROPERTIES, DATA_DETAILS, createDatabase)

    # header = next(get_zip_data_generator(filename))
        
    DATA_DETAILS = {
        'table_name': table_name,
        'columns': headings + [PARTITION_NAME],
        'partition_column': PARTITION_NAME,
        'db_name': 'airflow'
    }

    exec_on_db(DB_PROPERTIES, DATA_DETAILS, drop_table)
    exec_on_db(DB_PROPERTIES, DATA_DETAILS, create_master_table)
    exec_on_db(DB_PROPERTIES, DATA_DETAILS, create_partitions)
    exec_on_db(DB_PROPERTIES, DATA_DETAILS, truncate_table)


def export_zip_data_to_db(DB_PROPERTIES, table_name, path, limit=None, batch_size=None):
    print(f"exporting {path} to {DB_PROPERTIES['db_name']}")
    # table_name = path.split('/')[-1].split(".zip")[0]
    line_generator = get_zip_data_generator(path)
    header = next(line_generator)
    col_names_original = header.split(SEPARATOR)

    DATA_DETAILS = {
        "table_name": table_name,
        "columns": col_names_original + [PARTITION_NAME]
    }

    (partition_index, partition_col_name) = find_partition_column(col_names_original)

    def process_batch(batch_of_rows_as_strings):
        updated_rows_as_strings = [add_synthetic_partition_to_row(r, partition_index) for r in batch_of_rows_as_strings]
        DATA_DETAILS["rows_as_strings"] = updated_rows_as_strings
        exec_on_db(DB_PROPERTIES, DATA_DETAILS, insert_values)

    batch_datalines_and_process(line_generator, limit, batch_size, process_batch)

    exec_on_db(DB_PROPERTIES, DATA_DETAILS, analyze_table)


def find_partition_column(colNames: list) -> str:
    partition_details = next((i, col_name) for i, col_name in enumerate(colNames) if col_name in PARTITION_COL_CHOICES)
    if not partition_details:
        raise Exception(f'No matching partition columns candidate found from {",".join(colNames)}')

    print(f"Discovered column {':'.join(partition_details[1])} to use for our synthetic Partition colum on parq")
    return partition_details


def add_synthetic_partition_to_row(row_as_string, partition_index):
    row_as_array = [e.strip() for e in row_as_string.split(SEPARATOR)]
    line_arrays_with_partition = row_as_array + [create_synthetic_partition_column(row_as_array, partition_index)]
    return ",".join(line_arrays_with_partition)


def create_synthetic_partition_column(row_as_list: list, partition_index)->str:
    value = str(row_as_list[partition_index])
    try:
        return value.replace("-", "")[:-2]
    except Exception as e:
        print(f"Unable to create synthetic column value from [{value}] [{e}]")
        return "Unknown"



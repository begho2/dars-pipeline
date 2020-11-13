from functions.db_utils import exec_on_db, insert_values, drop_table, create_master_table, create_partitions, \
    analyze_table
from functions.db_utils import truncate_table, print_db_connection_details
from functions.hes_zip_utils import get_zip_data_generator, batch_datalines_and_process, SEPARATOR

DEBUG = False

PARTITION_NAME = "admi_partition"
PARTITION_COL_CHOICES = ("ARRIVALDATE", "ADMIDATE")
DB_PROPERTIES = {
    "hostname": "localhost",
    "port": "5433",
    "url": f"jdbc:postgresql://postgres:5433/dars",  # os.environ.get("RDS_URL"),
    "user": "airflow",  # os.environ.get("RDS_USER"),
    "password": "airflow",  # os.environ.get("RDS_PASSWORD"),
    "schema": "hes",
    "db_name": "dars",
    "driver": "org.postgresql.Driver"
}

# def discoverZipSchemaAndCreateInDb(path):
#     tableName = path.split("/").last.stripSuffix(".zip")
#     colNames = getColumnNames(path) + PARTITION_NAME
#     partitionCandidate = findPartitionColumn(colNames)
#     setupSchema(tableName, colNames.toList, PARTITION_NAME)

def setup_schema(DB_PROPERTIES, DATA_DETAILS):
  # exec_on_db(DB_PROPERTIES, DATA_DETAILS, createDatabase)
  exec_on_db(DB_PROPERTIES, DATA_DETAILS, drop_table)
  exec_on_db(DB_PROPERTIES, DATA_DETAILS, create_master_table)
  exec_on_db(DB_PROPERTIES, DATA_DETAILS, create_partitions)
  exec_on_db(DB_PROPERTIES, DATA_DETAILS, truncate_table)


def export_zip_data_to_db(DB_PROPERTIES, path, limit=None, batch_size=None):
    print(f"exporting {path} to {print_db_connection_details(DB_PROPERTIES)}")
    table_name = path.split('/')[-1].split(".zip")[0]
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

#
# def exec_on_db(DB_PROPERTIES, DATA_DETAILS, func):
#     import psycopg2
#     con = psycopg2.connect(dbname=DB_PROPERTIES['dbname'], user=DB_PROPERTIES['user'],
#                            password=DB_PROPERTIES['password'],
#                            host=DB_PROPERTIES['host'], port=DB_PROPERTIES['port'])
#     try:
#         func(con, DATA_DETAILS)
#     # except Exception as e:
#     #     print(f'Failed with {e}')
#     finally:
#         con.commit()
#         con.close()


# def process_batch(lines):
#     separator = "\\|"  # this is so we can use split with the optional -1 param, that ensures we preserve trailing elements
#     # option 1) get df and persist, build array and union all at the end.
#     lineArrays = [[e.trim() for e in line.split(separator)] for line in lines]
#     rows_as_strings = rowsAsString(lineArrays)
#
#     # insertIntoPostgres(tableName, colNames.toList, rowsAsString.toList)
#     exec_on_db(DB_PROPERTIES, DATA_DETAILS)
#     currTime = datetime.now().strftime("%H:%M:%S")
#     print(f"completed batch. Total count now {totalCount}. Time is {currTime}")
#
#     # extract partition candidate, manipulate it and add another column for each line

#
# def rowsAsString(lineArrays, partition_index):
#     line_arrays_with_partition = [line_array + createSyntheticPartitionColumn(line_array[partition_index]) for
#                                   line_array in lineArrays]
#     rows_as_strings = [",".join(line_array) for line_array in line_arrays_with_partition]
#     # lineArrays.map(lineArray=>{
#     #   val partitionCandidate = lineArray(partitionIndex)
#     #   val updatedLineArray = lineArray :+ createSyntheticPartitionColumn(partitionCandidate)
#     #   updatedLineArray.map(e=>s"\'${e}\'").mkString(",")
#     # })
#     is_debug = False
#     if is_debug:
#         print(f"Just read rows: [${rows_as_strings.length}]")
#
#     return rows_as_strings





# def getColumnNames(path: str):
#     print(f"Getting header from from {path}")
#     filename = get_filename(path)
#     file = ZipFile(path).open(filename)
#     bline = file.readline(10000)
#     header_line = bline.strip().decode("utf-8")
#     separator = "\\|"
#     col_names = [e.strip() for e in header_line.split(separator)]
#     return col_names




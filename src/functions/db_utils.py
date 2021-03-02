from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DEBUG = False

def validate_db_con(func):
  def exec_on_db(DB_PROPERTIES, DATA_DETAILS, func):
    import psycopg2
    con = psycopg2.connect(
      dbname=DB_PROPERTIES['db_name'], 
      user=DB_PROPERTIES['user'], 
      password=DB_PROPERTIES['password'],
      host=DB_PROPERTIES['hostname'], 
      port=DB_PROPERTIES['port']
    )
    try:
      return func(con, DATA_DETAILS)
    finally:
      con.commit()
      con.close()
  return exec_on_db

@validate_db_con
def create_database(con, DATA_DETAILS):
  # db_name = DATA_DETAILS["db_name"]
  db_name = 'airflow'

  con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
  cursor = con.cursor()
  cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
  exists = cursor.fetchone()
  if not exists:
    cursor.execute(f'CREATE DATABASE {db_name}')
    print(f"Database {db_name} created successfully")

@validate_db_con
def create_master_table(conn, DATA_DETAILS):
  columns = DATA_DETAILS["columns"]
  table_name = DATA_DETAILS["table_name"]
  partition_column = DATA_DETAILS["partition_column"]
  schema = (",\n").join([f"{c} varchar not null" for c in columns])

  create_table_sql = f"""
create table if not exists {table_name} (
    {schema}
) partition by list ({partition_column});
  """
  print(f"create table sql:")
  print(f"{create_table_sql}")
  cur = conn.cursor()
  cur.execute(create_table_sql)
  print("Table created successfully")


@validate_db_con
def drop_table(conn, DATA_DETAILS):
  table_name = DATA_DETAILS["table_name"]
  ddl = f"drop table if exists {table_name}"
  print(f"ddl: {ddl}")
  cur = conn.cursor()
  cur.execute(ddl)
  print("Table dropped succesfully")

@validate_db_con
def truncate_table(conn, DATA_DETAILS):
  table_name = DATA_DETAILS["table_name"]
  ddl = f"truncate table {table_name}"
  print(f"ddl: {ddl}")
  cur = conn.cursor()
  cur.execute(ddl)
  print("Table truncated succesfully")

@validate_db_con
def analyze_table(conn, DATA_DETAILS):
  table_name = DATA_DETAILS["table_name"]
  ddl = f"analyze {table_name}"
  print(f"ddl: {ddl}")
  cur = conn.cursor()
  cur.execute(ddl)
  print("Table analyzed succesfully")

def get_sensible_partitions()->list:
  #Want to get something like -> ("201901","201902", "DEFAULT")
  months = [f"0{m}" for m in range(1,10)] + [f"{m}" for m in range(10,13)]
  dates = [f"{year}{month}" for year in range(2015, 2021) for month in months ]
  return dates + ["DEFAULT"]

@validate_db_con
def create_partitions(conn, DATA_DETAILS):
  tableName = DATA_DETAILS["table_name"]
  def create_partition_ddl(p):
    partition_selection = "DEFAULT" if p == "DEFAULT" else f"for values in ({p})"
    return f"""
    create table if not exists {tableName}_{p}
    partition of {tableName}
    {partition_selection};
          """
  partStmts = [create_partition_ddl(p) for p in get_sensible_partitions()]

  for ddl in partStmts:
    print(f"create partition")
    print(f"ddl: {ddl}")
    cur = conn.cursor()
    cur.execute(ddl)
    print("Partition created successfully")

@validate_db_con
def insert_values(conn, DATA_DETAILS):
  tableName = DATA_DETAILS["table_name"]
  columns = DATA_DETAILS["columns"]
  rowsAsStrings = DATA_DETAILS["rows_as_strings"]
  columnLabelsAsString = ",".join(columns)

  def add_quotes_to_vals(row):
    quoted_elements=[f"'{e}'" for e in row.split(',')]
    return ",".join(quoted_elements)

  rows_with_quoted_vals = [f"({add_quotes_to_vals(row)})" for row in rowsAsStrings]
  value_list_string = (",\n").join(rows_with_quoted_vals)
  insertSql = f"INSERT INTO {tableName} ({columnLabelsAsString}) VALUES {value_list_string} "
  if (DEBUG):
    print(insertSql)

  cur = conn.cursor()
  cur.execute(insertSql)

@validate_db_con
def select_partition_counts(con, DATA_DETAILS):
  table_name = DATA_DETAILS["table_name"]
  partition_column = DATA_DETAILS["partition_column"]
  con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
  cursor = con.cursor()
  sql = f"SELECT {partition_column}, count(*) FROM {table_name} group by {partition_column}"
  print(f"Executing sql {sql}")
  cursor.execute(sql)
  records = cursor.fetchall()
  return records


def print_db_connection_details(DB_PROPERTIES):
  hostname = DB_PROPERTIES["hostname"]
  port = DB_PROPERTIES["port"]
  user = DB_PROPERTIES["user"]
  password = DB_PROPERTIES["password"]
  db_name = DB_PROPERTIES["db_name"]
  con_str = f"jdbc:postgresql://{hostname}:{port}/{db_name}"
  print(f"Postgress connection details {con_str} {user} {password}")

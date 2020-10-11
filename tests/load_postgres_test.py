import glob
import os
import unittest
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.models import DagBag, TaskInstance
from airflow.utils.dates import days_ago
from pyspark.sql import DataFrame, SparkSession

from operators.cf_spark_submit_operator import CfSparkSubmitOperator

import pyspark

number_cores = 8
memory_gb = 24
SPARK_CONFIG = (
    pyspark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.jars.packages', "org.postgresql:postgresql:42.2.14")
        # .set('spark.driver.memory', '{}g'.format(memory_gb))
)
# sc = pyspark.SparkContext(conf=conf)

def load_data(input_path):
    spark = SparkSession.builder \
        .config(conf=SPARK_CONFIG) \
        .getOrCreate()
    frame = (
        spark.read \
            .options(header=True, delimiter='|', inferSchema=True) \
            .parquet(input_path)
        # .schema(schema) \
        # .csv(CATALOG["local_raw/hes_ae_2014"])
    )
    #frame = frame.toDF(*[c.lower() for c in frame.columns])
    frame.show(n=5)
    # delete_tmp(zip_location, raw_location)
    return frame


def load_df_into_postgres(DB_PROPERTIES, df: DataFrame, table_name: str):

    from pyspark.sql import DataFrame
    db_url = DB_PROPERTIES['url']
    res = df.write.option("numPartitions", 8).jdbc(
        url=db_url,
        table=table_name,
        mode='append',
        # partitionColumn='admi_partition',
        properties=DB_PROPERTIES
    )
    print(f"done writing: {res}")

    spark = SparkSession.builder \
        .config(conf=SPARK_CONFIG) \
        .getOrCreate()

    _select_sql = f"select count(*) from {table_name}"
    df_select = spark.read.jdbc(url=db_url, table=table_name, properties=DB_PROPERTIES)
    print(df_select)
    df_select.show(20)


def validate_count_and_number_partitions(DB_PROPERTIES, table_name, expected_row_count, expected_num_partitions):

    from pyspark.sql import DataFrame
    db_url = DB_PROPERTIES['url']
    spark = SparkSession.builder \
        .config(conf=SPARK_CONFIG) \
        .getOrCreate()

    _select_sql = f"select count(*) from {table_name}"
    df_select = spark.read.jdbc(url=db_url, table=table_name, properties=DB_PROPERTIES)
    print(df_select)
    df_select.show(20)

def createPartitionCreatorFunc(table_name: str, partition_col: str):
    createFuncSql = """
        create function createPartitionIfNotExists(tableName varchar, forDate int) returns void
    as $body$
        declare partitionName text := tablename || forDate;
    begin
        -- Check if the table we need for the supplied date exists.
        -- If it does not exist...:
        if to_regclass(partitionName) is null then
            -- Generate a new table that acts as a partition for mytable:
            execute format('create table %I partition of %I for values in (%L) to (%L)', partitionName, tableName, forDate);
            -- Unfortunatelly Postgres forces us to define index for each table individually: not sure that's true
            -- execute format('create unique index on %I (forDate, key2)', tableName);
        end if;
    end;
    $body$ language plpgsql; 
        """
    import psycopg2
    con = psycopg2.connect(database="dars", user="airflow", password="airflow", host="localhost", port="5433")
    print("Database opened successfully")
    cur = con.cursor()
    cur.execute(createFuncSql)
    print("Func created successfully")
    con.commit()
    con.close()

def createMasterTableAndPartitions(DB_PROPERTIES: dict, table_name: str, df: DataFrame, partition_col: str):

    schema = ""
    not_first = False
    for c in df.columns:
        if (not_first):
            schema += ",\n"
        else:
            not_first = True
        schema += f"{c} varchar not null"
    # schema = """
    #     forDate date not null,
    #     key2 int not null,
    #     value int not null,
    # """
    # partition_col = "admi_partition"
#'alter table myTable rename to myTable_old;'
    createTableSql = f"""
create table {table_name}(
    {schema}
) partition by list ({partition_col});
    """
    print (f"create table sql:")
    print (f"{createTableSql}")
    import psycopg2
    con = psycopg2.connect(database="dars", user="airflow", password="airflow", host="localhost", port="5433")
    print("Database opened successfully")
    cur = con.cursor()
    cur.execute(createTableSql)
    print("Table created successfully")
    con.commit()
    con.close()



class TestPostgresLoad(unittest.TestCase):
    def test_load(self):
        pass

    def work_in_progress(self):
        # TODO:
        # need to upgrade postgres to same version as obelix and check it doesn't break airflow
        # complete the auto table create an auto partition create
        # add to pipeline
        # code tidying (envs, re-use etc)

        input_path=f'{Path(__file__).resolve().parents[1]}/dars-ingest/hes_output/NIC243790_HES_AE_201599.parq'

        # os.environ['TEST_INPUT']=input_path

        df: DataFrame = load_data(input_path)

        import os
        os.environ['DB_URL'] = "jdbc:postgresql://localhost:5433/"
        os.environ['DB_USER'] = "airflow"
        os.environ['DB_PASSWORD'] = "airflow"
        # DB_URL=jdbc:postgresql://dars.asdfasdfasdf`.eu-west-2.rds.amazonaws.com:5432/
        # DB_USER=asdf
        # DB_PASSWORD=asdf

        DB_PROPERTIES = {
            "url": "jdbc:postgresql://localhost:5433/dars",  # os.environ.get("RDS_URL"),
            "user": "airflow",  # os.environ.get("RDS_USER"),
            "password": "airflow",  # os.environ.get("RDS_PASSWORD"),
            "schema": "public",
            # "database": "dars",
            "driver": "org.postgresql.Driver"
        }

        table_name = "pjb5"
        createMasterTableAndPartitions(DB_PROPERTIES, table_name, df, "admi_partition")
        load_df_into_postgres(DB_PROPERTIES, df,table_name)
        validate_count_and_number_partitions(DB_PROPERTIES, table_name, 100, df.rdd.getNumPartitions())

#https://stackoverflow.com/questions/53600144/how-to-migrate-an-existing-postgres-table-to-partitioned-table-as-transparently
# https://hakibenita.com/fast-load-data-python-postgresql

if __name__ == '__main__':
    unittest.main()

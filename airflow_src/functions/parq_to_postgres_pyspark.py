from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


import glob
import os
import unittest
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.models import DagBag, TaskInstance
from airflow.utils.dates import days_ago
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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
    )
    frame.show(n=5)
    return frame

def find_partition_values(df, partition_name):
    rows = df.select(partition_name).distinct().collect()
    return [v[0] for v in rows]


def load_df_into_postgres(DB_PROPERTIES, DATA_DETAILS):
    from pyspark.sql import DataFrame
    table_name = DATA_DETAILS['table_name']
    df = DATA_DETAILS['df']
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


def execOnDb(DB_PROPERTIES, DATA_DETAILS, func):
    import psycopg2
    con = psycopg2.connect(dbname=DB_PROPERTIES['dbname'], user=DB_PROPERTIES['user'], password=DB_PROPERTIES['password'], host=DB_PROPERTIES['host'], port=DB_PROPERTIES['port'])
    try:
        func(con, DATA_DETAILS)
    # except Exception as e:
    #     print(f'Failed with {e}')
    finally:
        con.commit()
        con.close()

def createDbIfNotExist(DB_PROPERTIES: dict):
    import psycopg2
    con = psycopg2.connect(user=DB_PROPERTIES['user'],
                           password=DB_PROPERTIES['password'], host=DB_PROPERTIES['host'], port=DB_PROPERTIES['port'])
    try:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print("Database opened successfully")
        cursor = con.cursor()
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'dars'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute('CREATE DATABASE dars')
    finally:
        con.commit()
        con.close()




def createMasterTable(con, DATA_DETAILS: dict):
    # table_name: str, df: DataFrame, partition_col: str):
    df = DATA_DETAILS['df']
    table_name = DATA_DETAILS['table_name']
    partition_col = DATA_DETAILS['partition_col']
    schema = ""
    not_first = False
    for c in df.columns:
        if (not_first):
            schema += ",\n"
        else:
            not_first = True
        schema += f"{c} varchar not null"

    createTableSql = f"""
create table if not exists {table_name} (
    {schema}
) partition by list ({partition_col});
    """
    print (f"create table sql:")
    print (f"{createTableSql}")
    cur = con.cursor()
    cur.execute(createTableSql)
    print("Table created successfully")


def createPartitions(con, DATA_DETAILS: dict):
    # table_name: str, df: DataFrame, partition_col: str):
    df = DATA_DETAILS['df']
    table_name = DATA_DETAILS['table_name']
    partition_col = DATA_DETAILS['partition_col']
    partition_vals = DATA_DETAILS['partition_vals']

    cur = con.cursor()

    for partition in partition_vals:
        create_partition_sql = f"""
        create table if not exists {table_name}_{partition}
partition of {table_name}
for values in ({partition});
        """
        print (f"create partition sql:")
        print (f"{create_partition_sql}")
        cur.execute(create_partition_sql)
        print("Partition created successfully")


def createPartitionCreatorFunc(con, DATA_DETAILS: dict):
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

    cur = con.cursor()
    cur.execute(createFuncSql)
    print("Func created successfully")

def exec(filename):
    # input_path = f'{Path(__file__).resolve().parents[2]}/hes_output/NIC243790_HES_AE_201499.parq'
    input_path = f'{Path(__file__).resolve().parents[2]}/output_data/{filename}.parq'
    # input_path = 'output_data/NIC243790_HES_AE_201599.parq'

    df: DataFrame = load_data(input_path)
    print(f"Parquet Length: {df.count()}")
    partition_col = "admi_partition"
    partition_vals = find_partition_values(df, partition_col)

    is_docker = True
    port = "5432" if is_docker else "5433"

    DB_PROPERTIES = {
        "host": "postgres",
        "port": "5432",
        "url": "jdbc:postgresql://postgres:5432/dars",  # os.environ.get("RDS_URL"),
        "user": "airflow",  # os.environ.get("RDS_USER"),
        "password": "airflow",  # os.environ.get("RDS_PASSWORD"),
        "schema": "hes",
        "dbname": "dars",
        "driver": "org.postgresql.Driver"
    }

    table_name = "hes_ae_sample"
    DATA_DETAILS = {
        'df': df,
        'table_name': table_name,
        'partition_col': partition_col,
        'partition_vals': partition_vals
    }
    createDbIfNotExist(DB_PROPERTIES)
    execOnDb(DB_PROPERTIES, DATA_DETAILS, createMasterTable)
    execOnDb(DB_PROPERTIES, DATA_DETAILS, createPartitions)
    load_df_into_postgres(DB_PROPERTIES, DATA_DETAILS)
    validate_count_and_number_partitions(DB_PROPERTIES, table_name, 100, df.rdd.getNumPartitions())

if __name__ == "__main__":
    
    filename = sys.argv[1]
    print(f"FILE: {filename}")
    exec(filename)
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()

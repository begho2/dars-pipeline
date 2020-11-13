package cf

import cf.ZipToPostgres.{PARTITION_NAME, findPartitionColumn, getColumnNames}
import org.scalatest.Matchers

class ZipToPostgresTest extends org.scalatest.FunSuite
  with Matchers
  with SparkSessionWrapper {

  System.setProperty("DB_HOSTNAME","localhost")
  System.setProperty("DB_PORT","5433")
  System.setProperty("DB_USER","airflow")
  System.setProperty("DB_PASSWORD", "airflow")
  System.setProperty("DB_NAME", "dars")

  // assumes postgres server is already up and available
  test("schema setup and data insert with dummy data"){
    ZipToPostgres.setupSchema("test",List("fas","fsdf"), "fas")
    ZipToPostgres.insertIntoPostgres("test",List("fas","fsdf"), List("201901,2","201202,6"))
  }

  test("sensible date partitions are selected"){
    val parts = ZipToPostgres.getSensiblePartitions()
    println (parts)
  }

  test("End to end with test data"){
    var start = System.currentTimeMillis()
    val path = "./hes_zips/test_ae.zip"
    ZipToPostgres.DEBUG = false
    val tableName = path.split("/").last.stripSuffix(".zip")
    ZipToPostgres.discoverZipSchemaAndCreateInDb(path, tableName)(spark)
    ZipToPostgres.exportZipDataToPostgres(path,Some(5L), tableName, Some(2))(spark)
    println(s"time taken to transform zip to df and show: ${System.currentTimeMillis()-start} millis")
  }

  test("test data that has bad date partition info"){
    var start = System.currentTimeMillis()
    val path = "./hes_zips/test_ae_bad_admidate.zip"
    ZipToPostgres.DEBUG = false
    val tableName = path.split("/").last.stripSuffix(".zip")
    ZipToPostgres.discoverZipSchemaAndCreateInDb(path, tableName)(spark)
    val colNames: Array[String] = ZipToPostgres.getColumnNames(path, spark) :+ PARTITION_NAME

    val partitionCandidate = findPartitionColumn(colNames)

    ZipToPostgres.zipDataIntoPostgres(path, tableName, colNames, partitionCandidate, PARTITION_NAME, None, None)(spark)

    println(s"time taken to transform zip to df and show: ${System.currentTimeMillis()-start} millis")
  }


  test("End to end with real data"){
    val path = "./hes_zips/NIC243790_HES_AE_201599.zip"

    var start = System.currentTimeMillis()
    //    val path = "./hes_zips/test_ae.zip"
    ZipToPostgres.DEBUG = false
    val tableName = path.split("/").last.stripSuffix(".zip")
    ZipToPostgres.discoverZipSchemaAndCreateInDb(path, tableName)(spark)
    ZipToPostgres.exportZipDataToPostgres(path,Some(10000L), tableName, Some(3000))(spark)
    println(s"time taken to transform zip to df and show: ${System.currentTimeMillis()-start} millis")
  }

}
//todo.
// make tests work without postgres.
//  get tests working on github
//  option to read and print to console (dry run)
//  option to read and save sample to csv
// for a later time.
// can we do without spark
// read back into spark
// can we do all of this in python
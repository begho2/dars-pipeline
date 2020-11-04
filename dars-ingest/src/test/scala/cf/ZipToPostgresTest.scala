package cf

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
    ZipToPostgres.discoverZipSchemaAndCreateInDb(path)(spark)
    ZipToPostgres.exportZipDataToPostgres(path,Some(5L),Some(2))(spark)
    println(s"time taken to transform zip to df and show: ${System.currentTimeMillis()-start} millis")
  }

  test("End to end with real data"){
    val path = "./hes_zips/NIC243790_HES_AE_201599.zip"

    var start = System.currentTimeMillis()
    //    val path = "./hes_zips/test_ae.zip"
    ZipToPostgres.DEBUG = false
    ZipToPostgres.discoverZipSchemaAndCreateInDb(path)(spark)
    ZipToPostgres.exportZipDataToPostgres(path,Some(10000L),Some(3000))(spark)
    println(s"time taken to transform zip to df and show: ${System.currentTimeMillis()-start} millis")
  }

}
//todo. add the analyse step
// main
// airflow separaye steps
// test in airflow with docker
//push
//
// for a later time.
// can we do without spark
// read back into spark
// pgadmin is really good
// can we do faster inserts with partition logic - i on't think so
// can we do all of this in python
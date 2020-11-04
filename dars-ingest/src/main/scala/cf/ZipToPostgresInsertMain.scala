package cf

import org.apache.commons.logging.LogFactory

/**
  * Build with maven install or mvn package
  * run with
  * /usr/local/bin/spark-submit --conf 'spark.driver.extraJavaOptions=-DDB_HOSTNAME=localhost -DDB_PORT=5433 -DDB_USER=airflow -DDB_PASSWORD=airflow -DDB_NAME=dars' --class "cf.ZipToPostgresInsertMain" --master local[4] target/dars-ingest-1.0-SNAPSHOT-jar-with-dependencies.jar hes_zips/test_ae.zip 5 2
  */

object ZipToPostgresInsertMain extends SparkSessionWrapper {
    //val filename = "NIC243790_HES_AE_201599.zip"

  val log = LogFactory.getLog(getClass.getName)

  def main(args: Array[String]) = {
    println(s"Starting Postgres Data insert App with args ${args.mkString(",")}")
    val inputPath = if (args.length > 0) args(0) else throw new RuntimeException("no inputpath given")
    val limit = if (args.length > 1) args(1) else 0
    val batchSize = if (args.length > 2) args(2) else 10000
    val limitNum = limit.toString().toLong

    ZipToPostgres.printDbConnectionDetails()
    try {
      ZipToPostgres.exportZipDataToPostgres(inputPath, limitNum, batchSize.toString().toInt)(spark)

      println(s"Finished writing to postgres")
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        throw e
      }
    }
  }

}

package cf

/**
  * Build with maven install or mvn package
  * run with
DB_HOSTNAME="localhost"
DB_PORT="5433"
DB_USER="airflow"
DB_PASSWORD="airflow"
DB_NAME="dars"
DRIVER_OPTS=\'"spark.driver.extraJavaOptions=-DDB_HOSTNAME=$DB_HOSTNAME -DDB_PORT=$DB_PORT -DDB_USER=$DB_USER -DDB_PASSWORD=$DB_PASSWORD -DDB_NAME=$DB_NAME"\'
DRIVER_OPTS=\""spark.driver.extraJavaOptions=-DDB_HOSTNAME=$DB_HOSTNAME -DDB_PORT=$DB_PORT -DDB_USER=$DB_USER -DDB_PASSWORD=$DB_PASSWORD -DDB_NAME=$DB_NAME"\"
CMD=/usr/local/bin/spark-submit --conf $DRIVER_OPTS --class "cf.ZipToPostgresSchemaMain" --master local[4] target/dars-ingest-1.0-SNAPSHOT-jar-with-dependencies.jar hes_zips/test_ae.zip

  * OR
  *
  * /usr/local/bin/spark-submit --conf 'spark.driver.extraJavaOptions=-DDB_HOSTNAME=localhost -DDB_PORT=5433 -DDB_USER=airflow -DDB_PASSWORD=airflow -DDB_NAME=dars2' --class "cf.ZipToPostgresSchemaMain" --master local[4] target/dars-ingest-1.0-SNAPSHOT-jar-with-dependencies.jar hes_zips/test_ae.zip
  */

object ZipToPostgresSchemaMain extends SparkSessionWrapper {

//  val log = LogFactory.getLog(getClass.getName)

  def main(args: Array[String]) = {
    println(s"Starting Schema Create App with args ${args.mkString(",")}")
    val inputPath = if (args.length > 0) args(0) else throw new RuntimeException("no inputpath given")
    val tableName = if (args.length > 2) args(2) else throw new RuntimeException("No tablename given")
    ZipToPostgres.printDbConnectionDetails()
    try {
      val start = System.currentTimeMillis()
      ZipToPostgres.discoverZipSchemaAndCreateInDb(inputPath, tableName)(spark)

      println(s"Finished writing to postgres. Took ${(System.currentTimeMillis()-start)/1000} secs")
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        throw e
      }
    }

  }

}


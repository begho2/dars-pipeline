package cf

import org.apache.commons.logging.LogFactory

/**
  * Build with maven install or mvn package
  * run with
  * /usr/local/bin/spark-submit --class "cf.ZipToDfConverterMain" --master local[4] target/dars-ingest-1.0-SNAPSHOT.jar --files target/classes/log4j.properties#log4.properties -Dlog4j.configuration=log4j.properties 100
  */

object ZipToDfConverterMain extends SparkSessionWrapper {
    //val dir = "/Users/patrickboundy/Downloads"
    //val filename = "NIC243790_HES_AE_201599.zip"

  val log = LogFactory.getLog(getClass.getName)

  def main(args: Array[String]) = {
    log.info(s"Starting App with args ${args}")
    val limit = if (args.length > 0) args(0) else 0
    val inputPath = if (args.length > 1) args(1) else throw new RuntimeException("no inputpath given")
    val outputDir = if (args.length > 2) args(2) else throw new RuntimeException("no outputDir given")
    val inputFilename = inputPath.split("/").takeRight(1).head
    val outputPath = s"${outputDir}/${inputFilename.stripSuffix(".zip")}"
    val limitNum = limit.toString().toLong

    try {
      val df = ZipToDfConverter.exportZipToParq(inputPath, s"${outputPath}.parq", limitNum)(spark)
      log.info(s"Finished writing parq to ${outputPath}")
      df.show(false)
      ////    ColumnProfileRunner
      //    print("\n")
      print(s"Count is ${df.count()}\n")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
      case e: Throwable => {
        e.printStackTrace()
        throw e
      }
    }

  }

}


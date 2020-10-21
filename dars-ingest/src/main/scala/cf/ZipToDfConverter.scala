package cf

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object ZipToDfConverter  {
  val PARTITION_NAME = "admi_partition"

  def exportZipToParq(zipPath: String, parqLocation: String, limit: Long = Long.MaxValue)(implicit sparkSession: SparkSession) ={
    val df = get_top_n_rows(zipPath, limit)
    println(s"Writing parq to ${parqLocation} with limit of ${limit}, partitioned by ${PARTITION_NAME}")
    df.write.mode("overwrite").partitionBy(PARTITION_NAME).parquet(parqLocation)
    df
  }

  /**
    * Read first file from file in zip
    * @param path
    * @param spark
    * @return
    */
  def get_header(path: String)(implicit spark: SparkSession): String = {
    val (name: String, content: PortableDataStream) = spark.sparkContext.binaryFiles(path, 10).first()
    val zis = new ZipInputStream(content.open)
    zis.getNextEntry
    val br = new BufferedReader(new InputStreamReader(zis))
    br.readLine()
  }

  /**
    * Read first file from file in zip, and read the first n lines into a dataframe partitioned on suitable date representing admitted date
    *
    *
    * This contains lazy evaluated code. default limit returns the whole data set when an action is called on the df
    * Optionally, you can limit the data returned by providing an argument. It is not a random sample - just the first
    * n rows in the df (although it seems the data is not ordered by admitted time - which is helpful).
    */
  def get_top_n_rows(path: String, limit: Long = Long.MaxValue)(implicit spark: SparkSession): DataFrame = {
    println(s"Getting top ${limit} rows from ${path}")
    // convert zip file to a portableDataStream
    val (name: String, content: PortableDataStream) = spark.sparkContext.binaryFiles(path, 10).first()
    // wrap datastream in zipinput stream which can handle the decoding
    val zis = new ZipInputStream(content.open)
    // getNextEntry positions the stream at the start of the next entry (file) in the zip
    // We assume there is a single file (otherwise we would just put this in Stream.continually block
    zis.getNextEntry
    // Get the header row then read number of rows prescribed
    val br = new BufferedReader(new InputStreamReader(zis))
    val header = br.readLine()
    var count: Long = 1L
    val stream = limit match {
      case n: Long if n>0 => {
        Stream.continually(br.readLine()).takeWhile(row => {
          count = count +1L
          (row != null) && (count <= limit)
        })
      }
      case _ => Stream.continually(br.readLine()).takeWhile(row => row != null)
    }

    //
    val separator = "\\|" // this is so we can use split with the optional -1 param, that ensures we preserve trailing elements
    val rows = stream.map(str => {
      Row.fromSeq(str.split(separator,-1).map(_.trim))
    })
    val rdd = spark.sparkContext.makeRDD(rows.toSeq)
    val col_names = header.split(separator, -1).map(x=>x.toString().trim())
    val partitionColChoices = List("ARRIVALDATE","ADMIDATE")
    val partitionCol = partitionColChoices.collectFirst{
      case  col: String if col_names.toSet[String].contains(col) => col
    }
    if (partitionCol.isEmpty) throw new RuntimeException(s"No matching partition columns candidate found from ${col_names}")
    println(s"Discovered column ${partitionCol.get} to use for our synthetic Partition colum on parq")

    val schema = StructType(col_names.map(c => StructField(c, StringType)))
    val df = spark.createDataFrame(rdd, schema)
    df.withColumn(PARTITION_NAME, date_format(to_date(col(partitionCol.get),"yyyy-MM-dd"), "yyyyMM"))
  }

}


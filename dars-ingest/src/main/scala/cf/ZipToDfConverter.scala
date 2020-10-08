package cf

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object ZipToDfConverter  {

  def exportZipToParq(filename: String, zipLocation: String, parqLocation: String, limit: Int = 0)(implicit sparkSession: SparkSession) ={
    val df = exportZipToDf(filename, zipLocation, limit)
    df.write.mode("overwrite").partitionBy("admi_partition").parquet(parqLocation)
    df
  }

  def exportZipToDf(filename: String, zipLocation: String, limit: Int = 0)(implicit sparkSession: SparkSession) ={
    val path = s"$zipLocation/$filename"
    val r = get_rdd_from_zip(path, limit)
    val header = get_header(path)
    val df = get_df_from_rdd(r, header, limit)
    df.withColumn("admi_partition", date_format(to_date(col("ARRIVALDATE"),"yyyy-MM-dd"), "yyyyMM"))
  }

  /**
    * Opens the zip file as a PortableDataStream, and then wraps in a ZipInputStream to ensure correct decoding
    * Each new Entry on the ZipInputStream represents a new file.
    * We keep reading from that file, reading one line at a time until it is empty; then move to the next entry.
    * PortableDataStream is zipped so
    */
  def get_rdd_from_zip(path: String, limit: Int = 0)(implicit spark: SparkSession)= {
    var count = 0
    spark.sparkContext.binaryFiles(path, 10).flatMap {
      case (name: String, content: PortableDataStream) =>
        val zis = new ZipInputStream(content.open)
        Stream.continually(zis.getNextEntry)
          .takeWhile(_ != null & (if (limit>0) count<limit else true))
          .flatMap { _ =>
            count = count + 1
            val br = new BufferedReader(new InputStreamReader(zis))
            Stream.continually(br.readLine()).takeWhile(_ != null)
          }
    }
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
    * This contains lazy evaluated code. default limit returns the whole data set when an action is called on the df
    * Optionally, you can limit the data returned by providing an argument. It is not a random sample - just the first
    * n rows in the df.
    */
  def get_df_from_rdd(r: org.apache.spark.rdd.RDD[String], header: String, limit: Int=0) (implicit spark: SparkSession)= {
    val separator = "\\|" // this is so we can use split with the optional -1 param, that ensures we preserve trailing elements
    val limitPredicate = if (limit>0) (e:Tuple2[String, Long])=>e._2 < limit else (e:Any)=>true
    val col_names = header.split(separator, -1).map(x=>x.toString().trim())
    val schema = StructType(col_names.map(c => StructField(c, StringType)))
    val resultDF = spark.createDataFrame(
      r.zipWithIndex
        .filter(_._2 > 0)
        .filter(limitPredicate)
        .map{case (str, _) => Row.fromSeq(str.split(separator,-1).map(_.trim))}
      ,
      schema
    )
    resultDF
  }

  def get_filename_and_dir(filePath: String) = {
    val (dir,filenameWithLeadingSlash) = filePath.splitAt(filePath.lastIndexOf("/"))
    val inputFilename = filenameWithLeadingSlash.stripPrefix("/")
    (dir,inputFilename)
  }

}


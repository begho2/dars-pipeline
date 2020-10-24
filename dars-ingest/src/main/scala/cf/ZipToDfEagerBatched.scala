package cf

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


//ZipToDfEagerBatched.exportZipToParq("../dars-ingest/hes_zips/NIC243790_HES_AE_201599.zip","../dars-ingest/hes_output/pjb_eh",10000)(spark)

object ZipToDfEagerBatched  {
  val PARTITION_NAME = "admi_partition"

  def exportZipToParq(zipPath: String, parqLocation: String, limit: Long = Long.MaxValue, batchSize: Int = 10000, isDebug: Boolean = false)(implicit sparkSession: SparkSession) ={
    println(s"Reading zip ${zipPath} with limit [${limit}] and batch size [${batchSize}] and isDebug[${isDebug}]")
    val df = get_top_n_rows(zipPath, limit, batchSize, isDebug)
    println(s"Writing parq to ${parqLocation} with limit of ${limit}, partitioned by ${PARTITION_NAME}")

    try {
      df.write.mode("overwrite").partitionBy(PARTITION_NAME).parquet(parqLocation)
      df
    } catch {
      case e: Throwable => {
        println("Got throwable")
        println(s"${e.getMessage}")
        e.printStackTrace(System.err)
        e.printStackTrace(System.out)
        System.err.flush()
        System.out.flush()
        println("Desperate debugging complete")
        throw e
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
    * Read first file from file in zip, and read the first n lines into a dataframe partitioned on suitable date representing admitted date
    *
    *
    * This contains lazy evaluated code. default limit returns the whole data set when an action is called on the df
    * Optionally, you can limit the data returned by providing an argument. It is not a random sample - just the first
    * n rows in the df (although it seems the data is not ordered by admitted time - which is helpful).
    */
  def get_top_n_rows(path: String, limit: Long = Long.MaxValue, batchSize: Long = 10000, isDebug: Boolean = false)(implicit spark: SparkSession): DataFrame = {
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

    val dfs = mutable.ListBuffer[DataFrame]()
    var batchCount = 0
    var totalCount = 0
    var isNotFinished = true
    while (totalCount <= limit ) {
      batchCount=batchCount+1
      var count: Long = 0L
      var lines = mutable.ListBuffer[String]()
      while (count < batchSize && isNotFinished) {
        val line = br.readLine()
        count = count + 1
        totalCount = totalCount + 1
        if (line == null)
          isNotFinished = false
        else if (totalCount > limit) {
          println(s"Reached limit ${limit}")
          isNotFinished = false
        }
        else
          lines+=line

      }

      val separator = "\\|" // this is so we can use split with the optional -1 param, that ensures we preserve trailing elements
      // option 1) get df and persist, build array and union all at the end.
      val rows = lines.map(str => {
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
      val partitionedDf = df.withColumn(PARTITION_NAME, date_format(to_date(col(partitionCol.get),"yyyy-MM-dd"), "yyyyMM"))
      val cachedDf = partitionedDf.persist(StorageLevel.MEMORY_AND_DISK)
      if (isDebug)
        println(s"Just read df of size: [${cachedDf.count()}]")
      dfs.+=:(cachedDf)
      println(s"completed batch ${batchCount} with row count ${count-1}. Total count now ${totalCount}")
    }
    println(s"Merging dfs")
    dfs.reduce(_ union _)

  }

}


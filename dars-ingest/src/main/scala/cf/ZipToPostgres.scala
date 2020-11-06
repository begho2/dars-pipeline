package cf

import java.io.{BufferedReader, InputStreamReader}
import java.sql.{Connection, DriverManager, ResultSet}
import java.time.format.DateTimeFormatter
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


object ZipToPostgres  {

  var DEBUG = false

  val PARTITION_NAME = "admi_partition"

  def discoverZipSchemaAndCreateInDb(path: String)(implicit spark: SparkSession) = {
    val tableName = path.split("/").last.stripSuffix(".zip")
    val colNames: Array[String] = getColumnNames(path, spark) :+ PARTITION_NAME
    val partitionCandidate = findPartitionColumn(colNames)
    ZipToPostgres.setupSchema(tableName, colNames.toList, PARTITION_NAME)
  }
  def exportZipDataToPostgres(path: String, limit: Long = 1000000, batchSize: Int = 1000)(implicit spark: SparkSession) = {
    val tableName = path.split("/").last.stripSuffix(".zip")
    val colNames: Array[String] = getColumnNames(path, spark) :+ PARTITION_NAME
    val partitionCandidate = findPartitionColumn(colNames)
    ZipToPostgres.zipDataIntoPostgres(path, tableName, colNames, partitionCandidate, PARTITION_NAME, limit, batchSize)

    val conn = getDbConnection()
    try {
      analyzeTable(conn, tableName)
    } finally {
      conn.close()
    }
  }

  def zipDataIntoPostgres(path: String, tableName: String, colNames: Array[String], paritionCandidate: String,
                          partitionColumn: String, passedLimit: Long = 1000000, batchSize: Int = 50000)(implicit spark: SparkSession) = {
    // get index of partitionColumn so we can extract it and synthesize it
    val partitionIndex = colNames.indexOf(paritionCandidate)

    val limit = if (passedLimit == 0) Integer.MAX_VALUE else passedLimit
    println(s"Getting top ${limit} rows from ${path}")
    val (name: String, content: PortableDataStream) = spark.sparkContext.binaryFiles(path, 10).first()
    val zis = new ZipInputStream(content.open)
    zis.getNextEntry
    val br = new BufferedReader(new InputStreamReader(zis))
    var line = br.readLine()
    var totalCount = 0

    while (totalCount < limit && line != null ) {
      var count: Long = 0L
      var lines = mutable.ListBuffer[String]()
      while (totalCount <= limit && count < batchSize && line != null) {
        line = br.readLine()
        if (line != null) {
          count = count + 1
          totalCount = totalCount + 1
          lines += line
        }
      }

      def createSyntheticPartitionColumn(value: String)={
        println(s"Value: ${value}")
        value.replace("-","").substring(0,value.length-4)
      }
      val separator = "\\|" // this is so we can use split with the optional -1 param, that ensures we preserve trailing elements
      // option 1) get df and persist, build array and union all at the end.
      val lineArrays = lines.map(str => str.split(separator,-1).map(_.trim))
      // extract partition candidate, manipulate it and add another column for each line
      val rowsAsString = lineArrays.map(lineArray=>{
        val partitionCandidate = lineArray(partitionIndex)
        val updatedLineArray = lineArray :+ createSyntheticPartitionColumn(partitionCandidate)
        updatedLineArray.map(e=>s"\'${e}\'").mkString(",")
      })
      val isDebug = false
      if (isDebug)
        println(s"Just read rows: [${rowsAsString.length}]")
      insertIntoPostgres(tableName, colNames.toList, rowsAsString.toList)
      val currTime = java.time.LocalDateTime.now().format(DateTimeFormatter.ofPattern("hh:mm:ss"))
      println(s"completed batch. Total count now ${totalCount}. Time is ${currTime}")
    }
    println(s"finished inserting ${totalCount} rows into ${tableName} from ${path} with limit of ${limit} and batch size ${batchSize} ")
  }

  def findPartitionColumn(colNames: Array[String]) = {
    val partitionColChoices = List("ARRIVALDATE","ADMIDATE")
    val partitionCol = partitionColChoices.collectFirst{
      case  col: String if colNames.toSet[String].contains(col) => col
    }
    if (partitionCol.isEmpty) throw new RuntimeException(s"No matching partition columns candidate found from ${colNames}")
    println(s"Discovered column ${partitionCol.get} to use for our synthetic Partition colum on parq")
    partitionCol.get
  }

  private def getColumnNames(path: String, spark: SparkSession) = {
    println(s"Getting header from from ${path}")
    val (name: String, content: PortableDataStream) = spark.sparkContext.binaryFiles(path, 10).first()
    val zis = new ZipInputStream(content.open)
    zis.getNextEntry
    val br = new BufferedReader(new InputStreamReader(zis))
    val header = br.readLine()
    val separator = "\\|"
    val colNames = header.split(separator, -1).map(x => x.toString().trim())
    colNames
  }

  //  def setupSchema(tableName: String, columns: List[String], partitionColumn: String)(implicit spark: SparkSession)={
  def setupSchema(tableName: String, columns: List[String], partitionColumn: String)={
    createDatabase()
    val conn = getDbConnection()
    try {
      dropTable(conn, tableName )
      createMasterTable(conn, tableName, columns, partitionColumn )
      createPartitions(conn, tableName, columns, partitionColumn )
      truncateTable(conn, tableName )
    } finally {
      conn.close()
    }
  }

  def createDatabase()={
    val dbName: String = System.getProperty("DB_NAME")
    val conn = getDbConnection(Some(System.getProperty("DB_USER"))) // assume our user can access a db with same name.
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = stm.executeQuery(s"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '${dbName}'")
      if (!rs.next()) {
        println(s"creating database ${dbName}")
        val statment = conn.prepareStatement(s"create database ${dbName}")
        statment.executeUpdate()
        statment.close()
      }

    } finally {
      conn.close()
    }
  }


  def createMasterTable(conn: Connection, tableName: String, columns: List[String], partitionColumn: String) ={
    val schema = ""
    val colsSchema = columns.map((c)=>s"${c} varchar not null").mkString(",\n")

    val createTableSql = f"""
  create table if not exists ${tableName} (
      ${colsSchema}
  ) partition by list (${partitionColumn});
    """
    println(f"create table sql:")
    println(f"${createTableSql}")
    val statement = conn.prepareStatement(s"${createTableSql}")
    statement.executeUpdate()
    statement.close()
    println("Table created successfully")
  }

  def dropTable(conn: Connection, tableName: String) ={
    val ddl = f"drop table if exists ${tableName}"
    println(s"ddl: ${ddl}")
    val statement = conn.prepareStatement(s"${ddl}")
    statement.executeUpdate()
    statement.close()
    println("Table dropped succesfully")
  }

  def truncateTable(conn: Connection, tableName: String) ={
    val ddl = f"truncate table ${tableName}"
    println(s"ddl: ${ddl}")
    val statement = conn.prepareStatement(s"${ddl}")
    statement.executeUpdate()
    statement.close()
    println("Table truncated succesfully")
  }

  def analyzeTable(conn: Connection, tableName: String) ={
    val ddl = f"analyze ${tableName}"
    println(s"About to analyze table with ddl: ${ddl}")
    val statement = conn.prepareStatement(s"${ddl}")
    statement.executeUpdate()
    statement.close()
    println(s"Table ${tableName} analyzed succesfully")
  }

  def getSensiblePartitions():Seq[String] = {
    // Want to get something like ->   List[String]("201901","201902", "DEFAULT")
    val months = Range(1,10).map(m=>s"0${m}") ++ Range(10,13).map(m=>m.toString)
    val dates = Range(2015,2021).flatMap{year=>months.map(month=>s"${year}${month}")}
    return dates.:+("DEFAULT")
  }

  def createPartitions(conn: Connection, tableName: String, columns: List[String], partitionColumn: String) ={
    val schema = ""
    val partitionVals = getSensiblePartitions()
    val partStmts = partitionVals.map(p=>{
      val partSpecDdl = if (p.equalsIgnoreCase("DEFAULT")) "DEFAULT" else s"for values in (${p})"
      s"""
        create table if not exists ${tableName}_${p}
partition of ${tableName}
${partSpecDdl};
        """
    })
    partStmts.foreach(ddl=>{
      println(s"create partition sql:")
      println(s"${ddl}")
      val statement = conn.prepareStatement(s"${ddl}")
      statement.executeUpdate()
      statement.close()
      println("Partition created successfully")
    })

  }

  def printDbConnectionDetails() = {
    val hostname = System.getProperty("DB_HOSTNAME")
    val port = System.getProperty("DB_PORT")
    val user = System.getProperty("DB_USER")
    val password = System.getProperty("DB_PASSWORD")
    val dbName = System.getProperty("DB_NAME")
    val con_str = s"jdbc:postgresql://${hostname}:${port}/${dbName}"
    println(s"Postgress connection details ${con_str} ${user} ${password}")
  }

  def getDbConnection(dbNameOption: Option[String] = None) = {
    val hostname = System.getProperty("DB_HOSTNAME")
    val port = System.getProperty("DB_PORT")
    val user = System.getProperty("DB_USER")
    val password = System.getProperty("DB_PASSWORD")
    val dbName = dbNameOption match {
      case None => System.getProperty("DB_NAME")
      case Some(db: String) => db
    }
    println("Postgres connector openend")
    classOf[org.postgresql.Driver]
    val con_str = s"jdbc:postgresql://${hostname}:${port}/${dbName}"
    DriverManager.getConnection(con_str, user, password)
  }

  def insertIntoPostgres(tableName: String, columns: List[String], rowsAsStrings: List[String])={
    val conn = getDbConnection()
    try {
      insertValues(conn, tableName, columns, rowsAsStrings)
    } finally {
      conn.close()
      println("Postgres connector closed")
    }
  }

  def insertValues(conn: Connection, tableName: String, columns: List[String], rowsAsStrings: List[String])= {
    val columnLabelsAsString = columns.mkString(",")

    val value_list_string = rowsAsStrings.map(r => s"(${r})").mkString(",\n")
    val insertSql = s"INSERT INTO $tableName ($columnLabelsAsString) VALUES $value_list_string "
    if (DEBUG) {
      println(insertSql)
    }
    val statement = conn.prepareStatement(insertSql)
    statement.executeUpdate()
    statement.close()
  }

}


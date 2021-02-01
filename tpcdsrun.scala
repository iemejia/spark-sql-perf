import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
val sqlContext = spark.sqlContext

// Set:
val rootDir = "/home/ismael/datasets/tpcds_parquet/1GB"
val format = "parquet"

// Compiled from https://github.com/databricks/tpcds-kit/tree/master/tools
val dsdgenDir = "/home/ismael/projects/tpcds-kit/tools"
val scaleFactor = "1"
val databaseName = "tpcdsspark"

// Run:
val tables = new TPCDSTables(sqlContext,
    dsdgenDir = dsdgenDir, // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = true, // create the partitioned fact tables
    clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = 100) // how many dsdgen partitions to run - number of input tasks.

sql(s"DROP DATABASE $databaseName CASCADE")

// Create the specified database
sql(s"CREATE DATABASE $databaseName")
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, format)



// For CBO only, gather statistics on all columns:
tables.analyzeTables(databaseName, analyzeColumns = true)

// Run benchmarking queries

import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.Query

val tpcds = new TPCDS (sqlContext = sqlContext)
// Set:
val resultLocation = "/home/ismael/datasets/tpcds_parquet_output/1GB" // place to write results
val iterations = 1 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.

// queries.foreach (q => println(q.name))
val q3 = queries.filter (q => q.name == "q3-v2.4")
queries = q3

// Run:
sql(s"use $databaseName")
val experiment = tpcds.runExperiment(
  queries,
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)


// Retrieve results
// While the experiment is running you can use experiment.html to get a summary, or experiment.getCurrentResults to get complete current results. Once the experiment is complete, you can still access experiment.getCurrentResults, or you can load the results from disk.
// displayHTML(experiment.html)

// Get all experiments results.
val resultTable = spark.read.json(resultLocation)
resultTable.createOrReplaceTempView("sqlPerformance")
sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
// or
val specificResultTable = spark.read.json(experiment.resultPath)

You can get a basic summary by running:

experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)

experiment.getCurrentResults.withColumn("Name", substring(col("name"), 2, 100)).withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).select('Name, 'Runtime)

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.Query

// Local environment
val rootDir = "/home/ismael/datasets/tpcds_parquet_nonpartitioned/1GB"
val databaseName = "tpcdsspark1GB"
val resultLocation = "/home/ismael/datasets/tpcds_parquet_output/1GB" // place to write results

// Cluster environment
// val rootDir = "s3://tlnd-tpcds/dataset/nonpartitioned/1GB"
// val databaseName = "tpcdsspark1TB"
// val resultLocation = "s3://tlnd-tpcds/output/datasets/nonpartitioned/1GB/q3.sql" // place to write results

sql(s"DROP DATABASE $databaseName CASCADE")
sql(s"CREATE DATABASE $databaseName")

// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
val dsdgenDir = "."
val scaleFactor = "1000"
val tables = new TPCDSTables(spark.sqlContext,
    dsdgenDir = dsdgenDir, // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = false)

// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, format)

// For CBO only, gather statistics on all columns:
tables.analyzeTables(databaseName, analyzeColumns = true)

// Run benchmarking queries
val tpcds = new TPCDS (sqlContext = spark.sqlContext)

val query3Sql = "SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg\n" +
            " FROM date_dim dt, store_sales, item\n" +
            " WHERE dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
            "   AND store_sales.ss_item_sk = item.i_item_sk\n" +
            "   AND item.i_manufact_id = 128\n" +
            "   AND dt.d_moy=11\n" +
            " GROUP BY dt.d_year, item.i_brand, item.i_brand_id\n" +
            " ORDER BY dt.d_year, sum_agg desc, brand_id\n" +
            " LIMIT 100\n"

val q3 = tpcds.tpcds2_4Queries.filter (q => q.name == "q3-v2.4")

// Run:
sql(s"use $databaseName")
val experiment = tpcds.runExperiment(
  q3,
  iterations = 1,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(24*60*60)

val specificResultTable = spark.read.json(experiment.resultPath)

// You can get a basic summary by running:
experiment.getCurrentResults.withColumn("Name", substring(col("name"), 2, 100)).withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).select('Name, 'Runtime).collect()

// Run via:
// spark-shell --jars /home/hadoop/.ivy2/local/com.databricks/spark-sql-perf_2.12/0.5.1-SNAPSHOT/jars/spark-sql-perf_2.12.jar --packages org.apache.parquet:parquet-avro:1.8.2 --files /emr/projects/tpcds-kit/tools/dsdgen,/emr/projects/tpcds-kit/tools/tpcds.idx
// the files to stage via --files have to be compiled from https://github.com/databricks/tpcds-kit/tree/master/tools

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS

// local environment
val rootDir = "/home/ismael/datasets/tpcds_parquet_nonpartitioned/2GB"
val dsdgenDir = "/home/ismael/projects/tpcds-kit/tools"
val scaleFactor = "2" // 2GB
val numPartitions = 4

// cluster environment
// val rootDir = "s3://tlnd-tpcds/datasets/parquet/nonpartitioned/1TB"
// val dsdgenDir = "." // you need to staged dsdgen and tpcds.idx files via spark --files
// val scaleFactor = "1000" // 1TB
// val numPartitions = 8

// Run
val tables = new TPCDSTables(spark.sqlContext,
    dsdgenDir = dsdgenDir, // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

tables.genData(
    location = rootDir,
    format = "parquet",
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // create the partitioned fact tables
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = numPartitions) // how many dsdgen partitions to run - number of input tasks.

// Local environment
val resultLocation = "/home/ismael/datasets/tpcds_parquet_output/1GB" // place to write results

// Cluster environment
// val resultLocation = "s3://tlnd-tpcds/output/datasets/nonpartitioned/1GB/q3.sql" // place to write results

// Retrieve results
// While the experiment is running you can use experiment.html to get a summary, or experiment.getCurrentResults to get complete current results. Once the experiment is complete, you can still access experiment.getCurrentResults, or you can load the results from disk.
// displayHTML(experiment.html)

// Get all experiments results
val resultTable = spark.read.json(resultLocation)
resultTable.createOrReplaceTempView("sqlPerformance")

// spark.sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
val specificResult = spark.sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
specificResult.withColumn("Name", substring(col("name"), 2, 100)).withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).select('Name, 'Runtime).collect()

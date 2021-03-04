// local environment
val input = "s3://tlnd-tpcds/dataset/nonpartitioned/1GB/"
val output = "s3://tlnd-tpcds/output/dataset/nonpartitioned/1GB/"

// cluster environment
val input = "s3://tlnd-tpcds/dataset/nonpartitioned/1GB/"
val output = "s3://tlnd-tpcds/output/dataset/nonpartitioned/1GB/"

//TODO support s3
val sqlDir = "/home/ismael/projects/spark-sql-perf/src/main/resources/tpcds_2_4/"
val queryName = "q3.sql"

val tables = List( "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site")
for ( table <- tables ) {
    val dataset = spark.read.parquet(input + table)
    dataset.createOrReplaceTempView(table)
}

// val sql = scala.io.Source.fromFile(sqlDir + queryName).mkString
val sql = """SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
 FROM  date_dim dt, store_sales, item
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
   AND store_sales.ss_item_sk = item.i_item_sk
   AND item.i_manufact_id = 128
   AND dt.d_moy=11
 GROUP BY dt.d_year, item.i_brand, item.i_brand_id
 ORDER BY dt.d_year, sum_agg desc, brand_id
 LIMIT 100"""
val query = spark.sql(sql)
// query.explain()
// query.show()

query.write.option("header", "false").option("sep", "|").format("csv").mode(org.apache.spark.sql.SaveMode.Overwrite).save(output + queryName)

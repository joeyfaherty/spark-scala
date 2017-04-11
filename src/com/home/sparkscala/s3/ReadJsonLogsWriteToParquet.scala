package com.home.sparkscala.s3

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * reads json files at s3 location and write to parquet files in s3
  */
object ReadJsonLogsWriteToParquet {

  def main(args: Array[String]): Unit = {
    println("Starting spark driver script")

    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Read json logs and write as parquet table")
      .getOrCreate()

    val s3Path = "~/workspace/github/spark-scala/src/com/home/sparkscala/s3/*.log"
    // implicitly infers the schema based on the json structure
    val jsonDf: DataFrame = sparkSession.read.json(s3Path)
    jsonDf.createOrReplaceTempView("logsTmpView")

    val errorLogs: DataFrame = sparkSession.sql("select * from logsTmpView where loglevel=\'ERROR\'")

    errorLogs.createOrReplaceTempView("parquetTmpView")

    // write to parquet file
    errorLogs.coalesce(1).write.partitionBy("traceId").mode(SaveMode.Append).parquet("errorlogs")

    // test what was written to local
    val parquet: DataFrame = sparkSession.read.parquet("errorlogs")
    parquet.printSchema()
    parquet.show()
    println(parquet.count())
    println(parquet.columns.length)
  }

}
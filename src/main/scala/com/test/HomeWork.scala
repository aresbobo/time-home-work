package com.test

import org.apache.spark.sql.SparkSession

object HomeWork {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Homework")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    runHomeWork(spark)
    spark.stop()
  }

  private def runHomeWork(spark: SparkSession): Unit = {
    val e_df = spark.read.json("/Users/haibosun/projects/spark/time-home-work/src/main/resources/people.json")
    e_df.createOrReplaceTempView("people")
    spark.sql("SET spark.sql.planChangeLog.level=WARN")
    val query_2_1: String = "select a.name from (select name,age from people where true or age > 20 ) a where a.age<100"
    spark.sql(query_2_1).explain(true)

    val query_2_2: String = "select distinct a.name from (select name,age,1.0 x  from people where true or age > 1+19 order by x) a where a.age<100 except(select name from  people where name ='Justin')"
    spark.sql(query_2_2).explain(true)
  }
}

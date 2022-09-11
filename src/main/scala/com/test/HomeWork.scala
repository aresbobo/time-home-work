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
  // $example off:create_ds$

  private def runHomeWork(spark: SparkSession): Unit = {
    val e_df = spark.read.json("/Users/haibosun/projects/spark/time-home-work/src/main/resources/people.json")
    e_df.createOrReplaceTempView("people")
    spark.sql("SET spark.sql.planChangeLog.level=WARN")
    val query: String = "select a.name from (select name,age from people where 1 = 1 and age > 20 ) a where a.age<100"
    spark.sql(query).explain(true)

    val d_df = spark.read.json("/Users/haibosun/projects/spark/time-home-work/src/main/resources/department.json")
    e_df.createOrReplaceTempView("department")
    val query1: String = "select distinct a.name from (select * from people where age > 20  and 1 = 1) a where a.depid = 2 except select name from department where depid > 2"
    spark.sql(query1).explain(true)
  }

}

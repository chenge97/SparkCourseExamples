package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object udfs extends App{
  val spark = SparkSession.builder().appName("UDFs").config("spark.master","local").getOrCreate()

  val df = spark.read.option("header",true).option("inferSchema",true).csv("src/main/resources/data2/jorgecourse.csv")

  val incrementalFun = (salario:Double) => {
    salario * 1.3
  }

  //UDF Forma 1
  val incrementalFunUdf = udf(incrementalFun)

  val incrementalDf = df.select(col("Job Title"),col("Salary").as("Salary Antes"),incrementalFunUdf(col("Salary")).as("Salary Incremental"))
  incrementalDf.show()

  //UDF Forma 2
  df.createOrReplaceTempView("salarios")
  spark.udf.register("incrementalFunUdf",incrementalFun)

  spark.sql("SELECT `Job Title`, Salary as salario_antes, incrementalFunUdf(Salary) as Salary_despues from salarios").show

  /*
  Para hacer join con muchas columnas se puede usar el Seq
  Tipos de joins https://sparkbyexamples.com/spark/spark-sql-dataframe-join/
   */

  //Window functions
  val windowSpec = Window.partitionBy("Job Title")
  df.withColumn("salario_promedio", avg("Salary").over(windowSpec)).show()

  val windowSpec2 = Window.partitionBy("Company Name").orderBy("Rating")
  df.select(col("Company Name"), rank().over(windowSpec2).as("rank")).show()
}

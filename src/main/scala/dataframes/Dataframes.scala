package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Dataframes extends App{
  val spark = SparkSession.builder().appName("Intro to Dataframes").config("spark.master","local").getOrCreate()

  val schemaDDL = "Rating DOUBLE, Company STRING, JobTitle STRING, Salary INT, SalariesReported INT, Location String, EmploymentStatus STRING, JobRole String"
  val schema = StructType(
    Array(
      StructField("Rating",DoubleType,false),
      StructField("Company",StringType,false),
      StructField("JobTitle",StringType,false),
      StructField("Salary",IntegerType,false),
      StructField("SlariesReported",IntegerType,false),
      StructField("Location",StringType,false),
      StructField("EmploymentStatus",StringType,false),
      StructField("JobRole",StringType,false),
    )
  )
  val dataframe = spark.read.option("header",true).option("inferSchema",true).csv("src/main/resources/data2/jorgecourse.csv")
  dataframe.show()

  dataframe.show(5,false)

  //Function Take
  dataframe.take(5).foreach(println)

  //Types
  val longType = LongType

  val salaryDf = spark.read.option("header",true).schema(schema).csv("src/main/resources/data2/jorgecourse.csv")

  val salaryDf2 = spark.read.option("header",true).schema(schemaDDL).csv("src/main/resources/data2/jorgecourse.csv")

  salaryDf2.show

  //Columnas y Expresiones
  val rating = salaryDf2.col("rating")
  val company = salaryDf2.col("compaty")

  //salaryDf2.select(rating,company).show()

  import spark.implicits._

  salaryDf2.select(
    col("Rating"),
    column("JobTitle"),
    col("Salary"),
    $"Location",
    expr("JobRoles"),
  ).show

  // 2 tipos de operaciones
  // Transformaciones
  //Son los metodos que te dan como resultado otro df (orderBy, Groupby,filter etc)
  //Transformaciones NARROW
  // Donde una particion puede ser computada desde una unica instancia

  //Transformacion wide
  //Donde se necesitan multipls particiones pasandose datos entre si

  //Acciones show take count etc
}

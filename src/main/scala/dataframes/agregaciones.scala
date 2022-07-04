package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object agregaciones extends App{
  val spark = SparkSession.builder().appName("Projections, Filters and aggregations").config("spark.master","local").getOrCreate()

  val empleadosLista = Seq(
    (1,"Derick Davison",20,"Lenovo"),
    (2,"Bari Becnel",50,"Amazon"),
    (3,"Maddie Mueller",33,"Samsung"),
    (4,"Eva Emrich",23,"Amazon"),
    (5,"Katelyn Kunze",55,"LG"),
    (6,"Hue Hover",26,"Samsung"),
    (7,"Lucas Lout",57,"KIA"),
    (8,"Granville Grande",30,"Lenovo"),
    (9,"Robt Rude",39,"LG"),
    (10,"Lisha Lewin",25,"KIA"),
    (11,"Aron Atlas",52,"LG"),
    (12,"Chester Coddington",44,"KIA"),
    (13,"Diedre Dominy",31,"Amazon"),
    (14,"Evie Edgell",35,"Samsung"),
    (15,"Judy Johanson",36,"Lenovo")
  )

  val dfEmpleados = spark.createDataFrame(empleadosLista)

  // Count
  dfEmpleados.select(count("_4")).show()
  dfEmpleados.select(count("*")).show()

  //No repite count
  dfEmpleados.select(countDistinct("_4")).show()
  println(dfEmpleados.select("_4").distinct().count())

  //min y max
  dfEmpleados.select(min("_3"),max("_3")).show()

  //sum y avg
  dfEmpleados.select(sum("_3").as("suma_edad"),avg("_3").alias("promedio_edad")).show()

  dfEmpleados.selectExpr("sum(_3)").show()

  val empleadosConColumnasDf = dfEmpleados
    .withColumnRenamed("_3","edad")
    .withColumnRenamed("_1","ID")
    .withColumnRenamed("_2","empleado")
    .withColumnRenamed("_4","empresa")
  empleadosConColumnasDf.show()


  dfEmpleados.select(col("_1").as("id"),col("_2").as("empleado"),col("_3").as("edad"),col("_4").as("empresa"),(col("_3") * 2).as("edad_doble")).printSchema()

  val empleadosNewColumn = empleadosConColumnasDf.withColumn("edad_doble",col("edad") * 2)

  empleadosNewColumn.show()

  empleadosConColumnasDf.drop("empresa").show

  //Filtros

  empleadosConColumnasDf.filter(col("empresa") === "Amazon").show
  empleadosConColumnasDf.where(col("edad") >= 30).show
  empleadosConColumnasDf.where("empresa = 'Amazon'").show

  //Ejercicios:
  //1) Crear un Dataframe de autos
  /*
  * Nombre,Marca,Modelo,Vel min and Vel max
  *
  *
  * 2) Del archivos de salario
  * Traer la suma de salarios
  * Conteo de registros
  * Promedio de Rating
  * Alguna otra funcion de agregacion
  * */
}

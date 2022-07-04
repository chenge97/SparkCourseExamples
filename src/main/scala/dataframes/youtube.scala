package dataframes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object youtube extends App{
  val spark = SparkSession.builder().appName("Youtube Final Project").config("spark.master","local").getOrCreate()
  import spark.implicits._

  //val channelsInfoDfPrev = spark.read.option("header","true").option("sep","\t").option("delimeter","\t").option("inferSchema","true").csv("src/main/resources/data2/channel_numbers.csv")
  val channelsInfoDf = spark.read.option("header","true").option("sep","\t").option("delimeter","\t")
    .schema("""Id INT,_c1 STRING,Subscribers_String STRING,Subscribers_Number INT,_c2 STRING,Below_1k INT,Between_1k_5k INT,Between_5k_10k INT,Between_10k_25k INT,Between_25k_50k INT,
      Between_50k_100k INT,Between_100k_150k INT, Between_150k_200k INT, Between_200k_1m INT, Above_1m INT""").csv("src/main/resources/data2/channel_numbers.csv").drop("_c1","_c2")

  //channelsInfoDfPrev.show(truncate = false) To see why with the schema it didnt worked out
  channelsInfoDf.show(false)
  val youtubeChannelsDf = spark.read.option("header","true")
    .schema("""Id INT, Channel_name STRING,Channel_link STRING""").csv("src/main/resources/data2/youtube_channels.csv")

  youtubeChannelsDf.show(truncate = false)

  val rankWindow = Window.partitionBy("Category").orderBy($"Subscribers_Number".desc)
  val avgWindow = Window.partitionBy("Category")
  val joinedDF = youtubeChannelsDf.join(channelsInfoDf,Seq("Id"),joinType = "inner").select($"Channel_name",$"Channel_link",$"Subscribers_Number",
    when($"Subscribers_Number" < 100000,"Below100K").when($"Subscribers_Number" >= 100000 && $"Subscribers_Number" <= 999999,"Between100KAnd999999").when(
      $"Subscribers_Number" >= 1000000,"MoreOrEquals1M"
    ).otherwise("Irrelevant").as("Category"),lit("2022-07-04").as("Date_eval"))

  val joinedDfWithRankAndAvg = joinedDF.withColumn("Rank",rank().over(rankWindow)).withColumn("AvergarePerCategory",avg("Subscribers_Number").over(avgWindow))

  //You can use the func expr() to write he case statement as you would do in SQL

  val below100K = joinedDfWithRankAndAvg.where("Category = 'Below100K'")
  val between100KAnd999999 = joinedDfWithRankAndAvg.where("Category = 'Between100KAnd999999'")
  val moreOrEquals1M = joinedDfWithRankAndAvg.where("Category = 'MoreOrEquals1M'")

  below100K.show(false)
  between100KAnd999999.show(false)
  moreOrEquals1M.show(false)

  joinedDfWithRankAndAvg
    .repartition(1)
    .write
    .mode("Overwrite")
    .option("compression","gzip")
    .partitionBy("Category","Date_eval")
    .parquet("src/main/resources/output/youtube")

  val checkResult = spark.read.option("inferSchema","true").parquet("src/main/resources/output/youtube/Category=Below100K/Date_eval=2022-07-04/")
  checkResult.show(false)
}

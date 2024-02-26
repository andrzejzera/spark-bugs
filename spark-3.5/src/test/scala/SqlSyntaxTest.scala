import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp


class SqlSyntaxTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("SqlSyntaxTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  "WITH block" should "behave the same as subquery" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation1 = "/tmp/checkpoint1"
    val checkpointLocation2 = "/tmp/checkpoint2"
    FileUtils.deleteDirectory(new File(checkpointLocation1))
    FileUtils.deleteDirectory(new File(checkpointLocation2))

    val memoryStream1 = MemoryStream[ClickEvent]
    val memoryStream2 = MemoryStream[adImpressionsStatisticsEvent]

    val clicks = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    val adImpressionsStatistics = memoryStream2.toDF().withWatermark("windowTimestamp", "0 seconds")
    clicks.createOrReplaceTempView("clicks")
    adImpressionsStatistics.createOrReplaceTempView("adImpressionsStatistics")

    // Using subquery
    val aggregatedDataVer1 = spark.sql(
      """
        |SELECT
        |  cpu.window.end AS windowTimestamp,
        |  cpu.userId,
        |  cpu.numClicks,
        |  ais.numImpressions
        |FROM (SELECT
        |    window(eventTime, '1 minute') AS window,
        |    userId,
        |    count(*) AS numClicks
        |  FROM clicks
        |  GROUP BY window, userId) cpu
        |LEFT JOIN adImpressionsStatistics ais
        |  ON cpu.window.end = ais.windowTimestamp
        |  AND cpu.userId = ais.userId
        |""".stripMargin)

    // Using WITH clause
    val aggregatedDataVer2 = spark.sql(
      """
        |WITH clicks_per_user (
        |  SELECT
        |    window(eventTime, '1 minute') AS window,
        |    userId,
        |    count(*) AS numClicks
        |  FROM clicks
        |  GROUP BY window, userId
        |)
        |SELECT
        |  cpu.window.end AS windowTimestamp,
        |  cpu.userId,
        |  cpu.numClicks,
        |  ais.numImpressions
        |FROM clicks_per_user cpu
        |LEFT JOIN adImpressionsStatistics ais
        |  ON cpu.window.end = ais.windowTimestamp
        |  AND cpu.userId = ais.userId
        |""".stripMargin)

    // When
    memoryStream1.addData(Seq(
      ClickEvent(Timestamp.valueOf("2024-02-10 10:19:34"), 1),
      ClickEvent(Timestamp.valueOf("2024-02-10 10:19:38"), 1),
      ClickEvent(Timestamp.valueOf("2024-02-10 10:19:42"), 2),
      ClickEvent(Timestamp.valueOf("2024-02-10 10:20:02"), 2)
    ))
    memoryStream2.addData(Seq(
      adImpressionsStatisticsEvent(Timestamp.valueOf("2024-02-10 10:20:00"), 1, 10),
      adImpressionsStatisticsEvent(Timestamp.valueOf("2024-02-10 10:20:00"), 2, 20),
    ))

    val queryVer1 = aggregatedDataVer1.writeStream.format("memory")
      .queryName("aggregatedDataVer1")
      .option("checkpointLocation", checkpointLocation1).start()
    queryVer1.processAllAvailable()

    val queryVer2 = aggregatedDataVer2.writeStream.format("memory")
      .queryName("aggregatedDataVer2")
      .option("checkpointLocation", checkpointLocation2).start()

    // Test fails here: LeftOuter join with a streaming DataFrame/Dataset on the right and a static DataFrame/Dataset on the left is not supported
    queryVer2.processAllAvailable()

    val resultVer1 = spark.sql("SELECT * FROM aggregatedDataVer1").collect()
    val resultVer2 = spark.sql("SELECT * FROM aggregatedDataVer2").collect()

    // Then
    resultVer1 shouldEqual resultVer2
  }

  "Two nested subqueries each with window function" should "not break outer query" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[ClickEvent]

    val clicks = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    clicks.createOrReplaceTempView("clicks")

    val aggregatedData = spark.sql(
      """
        |SELECT
        |  cpu_large.large_window.end AS timestamp,
        |  avg(cpu_large.numClicks) AS avgClicksPerUser
        |FROM (SELECT
        |        window(small_window, '10 minutes') AS large_window,
        |        userId,
        |        sum(numClicks) AS numClicks
        |      FROM (SELECT
        |              window(eventTime, '1 minute') AS small_window,
        |              userId,
        |              count(*) AS numClicks
        |            FROM clicks
        |            GROUP BY window, userId) cpu_small
        |      GROUP BY window, userId) cpu_large
        |GROUP BY timestamp
        |""".stripMargin)

    val expectedSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("userId", IntegerType, nullable = false),
      StructField("numClicks", LongType, nullable = true)
    ))

    // When

    // Then
    aggregatedData.schema shouldEqual expectedSchema
  }

}

case class ClickEvent(eventTime: Timestamp, userId: Int)
case class ClickDetailEvent(eventTime: Timestamp, userId: Int, adType: String, impressionDuration: Int)
case class adImpressionsStatisticsEvent(windowTimestamp: Timestamp, userId: Int, numImpressions: Int)

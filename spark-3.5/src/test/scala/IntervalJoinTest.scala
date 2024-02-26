import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, expr, window}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp


class IntervalJoinTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("IntervalJoinTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  "Interval join between three streams (A, B, C) where join interval condition refers to A (A<->B, A<->C)" should "produce the result after MAX(interval(A<->B), interval(A<->C))" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[EventTypeWithId1]
    val memoryStream2 = MemoryStream[EventTypeWithId2]
    val memoryStream3 = MemoryStream[EventTypeWithId3]

    val data1 = memoryStream1.toDF.withWatermark("eventTime1", "0 seconds")
    val data2 = memoryStream2.toDF.withWatermark("eventTime2", "0 seconds")
    val data3 = memoryStream3.toDF.withWatermark("eventTime3", "0 seconds")

    val result = data1
      .join(data2, expr("""
        eventId1 = eventId2 AND
        eventTime2 >= eventTime1 AND
        eventTime2 <= eventTime1 + interval 3 minutes
        """), "leftOuter")
      .join(data3, expr("""
        eventId1 = eventId3 AND
        eventTime3 >= eventTime1 AND
        eventTime3 <= eventTime1 + interval 5 minutes
        """), "leftOuter")
      .groupBy(window($"eventTime1", "1 minute"))
      .agg(
        count("eventId1").as("countEventId1"),
        count("eventId2").as("countEventId2"),
        count("eventId3").as("countEventId3")
      )

    // When
    memoryStream1.addData(Seq(EventTypeWithId1(1, Timestamp.valueOf("2024-02-10 10:19:40"), 1)))
    memoryStream2.addData(Seq(EventTypeWithId2(1, Timestamp.valueOf("2024-02-10 10:22:32"), 1)))

    val query = result.writeStream.format("memory")
      .queryName("result")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventTypeWithId1(2, Timestamp.valueOf("2024-02-10 10:25:10"), 1)))
    memoryStream2.addData(Seq(EventTypeWithId2(2, Timestamp.valueOf("2024-02-10 10:25:10"), 1)))
    memoryStream3.addData(Seq(EventTypeWithId3(2, Timestamp.valueOf("2024-02-10 10:25:10"), 1)))
    query.processAllAvailable()
    val resultAfter5Min = spark.sql("SELECT window.end, countEventId1, countEventId2, countEventId3 FROM result").collect()

    memoryStream1.addData(Seq(EventTypeWithId1(2, Timestamp.valueOf("2024-02-10 10:28:10"), 1)))
    memoryStream2.addData(Seq(EventTypeWithId2(2, Timestamp.valueOf("2024-02-10 10:28:10"), 1)))
    memoryStream3.addData(Seq(EventTypeWithId3(2, Timestamp.valueOf("2024-02-10 10:28:10"), 1)))
    query.processAllAvailable()
    val resultAfter8Min = spark.sql("SELECT window.end, countEventId1, countEventId2, countEventId3 FROM result").collect()

    // Then
    val expected = Array(
      Row(Timestamp.valueOf("2024-02-10 10:20:00"), 1, 1, 0),
    )

    resultAfter5Min shouldEqual expected
    resultAfter8Min shouldEqual expected
  }

}

case class EventTypeWithId1(eventId1: Int, eventTime1: Timestamp, value1: Int)
case class EventTypeWithId2(eventId2: Int, eventTime2: Timestamp, value2: Int)
case class EventTypeWithId3(eventId3: Int, eventTime3: Timestamp, value3: Int)
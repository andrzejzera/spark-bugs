import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp


class OuterJoinTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("OuterJoinTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  "Left Outer Join operation between two input streams" should "produce the expected result" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[EventType1]
    val memoryStream2 = MemoryStream[EventType2]

    val data1 = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    val data2 = memoryStream2.toDF.withWatermark("eventTime", "0 seconds")

    val join = data1.join(data2, Seq("eventTime"), "leftOuter")

    // When
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))

    val query = join.writeStream.format("memory")
      .queryName("join")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:21:00"), 2)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    query.processAllAvailable()

    val result = spark.sql("SELECT * FROM join ORDER BY eventTime").collect()

    // Then
    val expected = Array(
      Row(Timestamp.valueOf("2024-02-10 10:20:00"), 1, 1),
      Row(Timestamp.valueOf("2024-02-10 10:21:00"), 2, null),
      Row(Timestamp.valueOf("2024-02-10 10:22:00"), 3, 3),
    )

//    Actual = Expected
//    +-------------------+------+------+
//    |eventTime          |value1|value2|
//    +-------------------+------+------+
//    |2024-02-10 10:20:00|1     |1     |
//    |2024-02-10 10:21:00|2     |NULL  |
//    |2024-02-10 10:22:00|3     |3     |
//    +-------------------+------+------+

    result shouldEqual expected
  }

  "Left Outer Join operation between three input streams" should "produce the expected result" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[EventType1]
    val memoryStream2 = MemoryStream[EventType2]
    val memoryStream3 = MemoryStream[EventType3]

    val data1 = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    val data2 = memoryStream2.toDF.withWatermark("eventTime", "0 seconds")
    val data3 = memoryStream3.toDF.withWatermark("eventTime", "0 seconds")

    val join = data1
      .join(data2, Seq("eventTime"), "leftOuter")
      .join(data3, Seq("eventTime"), "leftOuter")

    // When
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))

    val query = join.writeStream.format("memory")
      .queryName("join")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:21:00"), 2)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:21:00"), 2)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:23:00"), 4)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:24:00"), 5)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:24:00"), 5)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:24:00"), 5)))
    query.processAllAvailable()

    val result = spark.sql("SELECT * FROM join ORDER BY eventTime").collect()

    // Then
    val expected = Array(
      Row(Timestamp.valueOf("2024-02-10 10:20:00"), 1, 1, 1),
      Row(Timestamp.valueOf("2024-02-10 10:21:00"), 2, 2, null),
      Row(Timestamp.valueOf("2024-02-10 10:22:00"), 3, null, 3),
      Row(Timestamp.valueOf("2024-02-10 10:23:00"), 4, null, null),
      Row(Timestamp.valueOf("2024-02-10 10:24:00"), 5, 5, 5),
    )

    result shouldEqual expected

//    Actual
//    +-------------------+------+------+------+
//    |eventTime          |value1|value2|value3|
//    +-------------------+------+------+------+
//    |2024-02-10 10:20:00|1     |1     |1     |
//    |2024-02-10 10:21:00|2     |2     |NULL  |
//    |2024-02-10 10:22:00|3     |NULL  |3     |
//    |2024-02-10 10:24:00|5     |5     |5     |
//    +-------------------+------+------+------+

//    Expected
//    +-------------------+------+------+------+
//    |eventTime          |value1|value2|value3|
//    +-------------------+------+------+------+
//    |2024-02-10 10:20:00|1     |1     |1     |
//    |2024-02-10 10:21:00|2     |2     |NULL  |
//    |2024-02-10 10:22:00|3     |NULL  |3     |
//    |2024-02-10 10:23:00|4     |NULL  |NULL  |   <- Diff
//    |2024-02-10 10:24:00|5     |5     |5     |
//    +-------------------+------+------+------+

  }

  "Left Outer Join operation between four input streams" should "produce the expected result" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[EventType1]
    val memoryStream2 = MemoryStream[EventType2]
    val memoryStream3 = MemoryStream[EventType3]
    val memoryStream4 = MemoryStream[EventType4]

    val data1 = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    val data2 = memoryStream2.toDF.withWatermark("eventTime", "0 seconds")
    val data3 = memoryStream3.toDF.withWatermark("eventTime", "0 seconds")
    val data4 = memoryStream4.toDF.withWatermark("eventTime", "0 seconds")

    val join = data1
      .join(data2, Seq("eventTime"), "leftOuter")
      .join(data3, Seq("eventTime"), "leftOuter")
      .join(data4, Seq("eventTime"), "leftOuter")

    // When
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))
    memoryStream4.addData(Seq(EventType4(Timestamp.valueOf("2024-02-10 10:20:00"), 1)))

    val query = join.writeStream.format("memory")
      .queryName("join")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:21:00"), 2)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:21:00"), 2)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:21:00"), 2)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    memoryStream4.addData(Seq(EventType4(Timestamp.valueOf("2024-02-10 10:22:00"), 3)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:23:00"), 4)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:23:00"), 4)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:24:00"), 5)))
    memoryStream4.addData(Seq(EventType4(Timestamp.valueOf("2024-02-10 10:24:00"), 5)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:25:00"), 6)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:25:00"), 6)))
    memoryStream3.addData(Seq(EventType3(Timestamp.valueOf("2024-02-10 10:25:00"), 6)))
    memoryStream4.addData(Seq(EventType4(Timestamp.valueOf("2024-02-10 10:25:00"), 6)))
    query.processAllAvailable()

    val result = spark.sql("SELECT * FROM join ORDER BY eventTime").collect()

    // Then
    val expected = Array(
      Row(Timestamp.valueOf("2024-02-10 10:20:00"), 1, 1, 1, 1),
      Row(Timestamp.valueOf("2024-02-10 10:21:00"), 2, 2, 2, null),
      Row(Timestamp.valueOf("2024-02-10 10:22:00"), 3, null, 3, 3),
      Row(Timestamp.valueOf("2024-02-10 10:23:00"), 4, 4, null, null),
      Row(Timestamp.valueOf("2024-02-10 10:24:00"), 5, null, null, 5),
      Row(Timestamp.valueOf("2024-02-10 10:25:00"), 6, 6, 6, 6),
    )

    result shouldEqual expected

    //    Actual
    //    +-------------------+------+------+------+------+
    //    |          eventTime|value1|value2|value3|value4|
    //    +-------------------+------+------+------+------+
    //    |2024-02-10 10:20:00|     1|     1|     1|     1|
    //    |2024-02-10 10:21:00|     2|     2|     2|  NULL|
    //    |2024-02-10 10:22:00|     3|  NULL|     3|     3|
    //    |2024-02-10 10:25:00|     6|     6|     6|     6|
    //    +-------------------+------+------+------+------+

    //    Expected
    //    +-------------------+------+------+------+------+
    //    |          eventTime|value1|value2|value3|value4|
    //    +-------------------+------+------+------+------+
    //    |2024-02-10 10:20:00|     1|     1|     1|     1|
    //    |2024-02-10 10:21:00|     2|     2|     2|  NULL|
    //    |2024-02-10 10:22:00|     3|  NULL|     3|     3|
    //    |2024-02-10 10:23:00|     4|     4|  NULL|  NULL|   <- Diff
    //    |2024-02-10 10:24:00|     5|  NULL|  NULL|     5|   <- Diff
    //    |2024-02-10 10:25:00|     6|     6|     6|     6|
    //    +-------------------+------+------+------+------+

  }

  "Aggregation on input stream followed by Left Outer Join with second input stream" should "produce the expected result" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[EventType1]
    val memoryStream2 = MemoryStream[EventType2]

    // data1 is meant for raw events
    val data1 = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    // data2 is meant for data already aggregated by 1-min time window (possibly by other system)
    val data2 = memoryStream2.toDF.withWatermark("eventTime", "0 seconds")
      .withColumnRenamed("eventTime", "timestamp")
      .withColumnRenamed("value2", "count2")

    val aggregatedData = data1.groupBy(window($"eventTime", "1 minute")).agg(count("*").as("count1"))
      .withColumn("timestamp", $"window.end")
      .drop($"window")

    val join = aggregatedData
      .join(data2, Seq("timestamp"), "leftOuter")

    // When
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:19:30"), 1)))
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:19:35"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:20:00"), 30)))

    val query = join.writeStream.format("memory")
      .queryName("join")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:20:10"), 1)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:21:10"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:22:00"), 20)))
    query.processAllAvailable()

    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:22:10"), 1)))
    query.processAllAvailable()

    val result = spark.sql("SELECT * FROM join ORDER by timestamp").collect()

    // Then
    val expected = Array(
      Row(Timestamp.valueOf("2024-02-10 10:20:00"), 2, 30),
      Row(Timestamp.valueOf("2024-02-10 10:21:00"), 1, null),
      Row(Timestamp.valueOf("2024-02-10 10:22:00"), 1, 20),
    )

    result shouldEqual expected

    //    Actual
    //    +-------------------+------+------+
    //    |          timestamp|count1|count2|
    //    +-------------------+------+------+
    //    |2024-02-10 10:20:00|     2|    30|
    //    |2024-02-10 10:22:00|     1|    20|
    //    +-------------------+------+------+

    //    Expected
    //    +-------------------+------+------+
    //    |          timestamp|count1|count2|
    //    +-------------------+------+------+
    //    |2024-02-10 10:20:00|     2|    30|
    //    |2024-02-10 10:21:00|     1|  null|   <- Diff
    //    |2024-02-10 10:22:00|     1|    20|
    //    +-------------------+------+------+
  }

  "Aggregations on two different input streams followed by Left Outer Join on aggregated results" should "produce the expected result" in {
    val localSpark = spark
    import localSpark.implicits._
    implicit val sparkContext = spark.sqlContext

    // Given
    val checkpointLocation = "/tmp/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))

    val memoryStream1 = MemoryStream[EventType1]
    val memoryStream2 = MemoryStream[EventType2]

    val data1 = memoryStream1.toDF.withWatermark("eventTime", "0 seconds")
    val data2 = memoryStream2.toDF.withWatermark("eventTime", "0 seconds")

    val aggregatedData1 = data1.groupBy(window($"eventTime", "1 minute")).agg(count("*").as("count1"))
    val aggregatedData2 = data2.groupBy(window($"eventTime", "1 minute")).agg(count("*").as("count2"))

    val join = aggregatedData1
      .join(aggregatedData2, Seq("window"), "leftOuter")

    // When
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:19:30"), 1)))
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:19:35"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:19:43"), 1)))
    memoryStream1.addData(Seq(EventType1(Timestamp.valueOf("2024-02-10 10:20:10"), 1)))
    memoryStream2.addData(Seq(EventType2(Timestamp.valueOf("2024-02-10 10:20:10"), 1)))

    val query = join.writeStream.format("memory")
      .queryName("join")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()

    val result = spark.sql("SELECT window.end, count1, count2 FROM join").collect()

    // Then
    val expected = Array(
      Row(Timestamp.valueOf("2024-02-10 10:20:00"), 2, 1),
    )

    result shouldEqual expected

    //    Actual
    //    +-------------------+------+------+
    //    |end                |count1|count2|
    //    +-------------------+------+------+
    //    +-------------------+------+------+

    //    Expected
    //    +-------------------+------+------+
    //    |end                |count1|count2|
    //    +-------------------+------+------+
    //    |2024-02-10 10:20:00|2     |1     |   <- Diff
    //    +-------------------+------+------+
  }
}

case class EventType1(eventTime: Timestamp, value1: Int)
case class EventType2(eventTime: Timestamp, value2: Int)
case class EventType3(eventTime: Timestamp, value3: Int)
case class EventType4(eventTime: Timestamp, value4: Int)

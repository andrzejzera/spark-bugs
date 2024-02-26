# spark-bugs
## spark-3.5

This projects contains a set of tests that aim to reveal bugs of Structured Streaming operations in Spark 3.5. 
These tests were collected into three groups:
1. [OuterJoinTest](src/test/scala/OuterJoinTest.scala): showing outer join operations returning incorrect results
2. [IntervalJoinTest](src/test/scala/IntervalJoinTest.scala): showing interval join operations returning correct results but after a longer time than expected
3. [SqlSyntaxTest](src/test/scala/SqlSyntaxTest.scala): showing inconsistency between the same operations expressed in Scala API vs. Spark SQL
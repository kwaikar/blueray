package edu.utd.security.blueray

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.junit.After
import org.junit.Before
import org.junit.Test

class StreamingTest {
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  def testSparkStreaming(sc: SparkContext, filePath: String, valueToBeBlocked: String, newValue: String, valueNotBlocked: String) = {

    sc.setLocalProperty("PRIVILEDGE", Util.encrypt("ADMIN"));

    var inputFile = sc.textFile(filePath)

    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/stream/", Util.encrypt("ADMIN"), valueToBeBlocked);
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.sparkContext.getConf.set("spark.streaming.fileStream.minRememberDuration", "624000")

    var (lines, testCasePassed) = streamFile(sc, ssc, policy, valueToBeBlocked, newValue, valueNotBlocked)
    println(":" + lines.count())
    ssc.start()
    ssc.awaitTermination()
  }
  private def streamFile(sc: SparkContext, ssc: org.apache.spark.streaming.StreamingContext, policy: edu.utd.security.blueray.Policy, valueToBeBlocked: String, newValue: String, valueNotBlocked: String) = {

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat]("hdfs://localhost/stream/", defaultFilter(_), newFilesOnly = false).map(_._2.toString)
    var testCasePassed = false;
    lines.foreachRDD(rdd =>
      {
        if (rdd.collect().length != 0) {
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
          GenericTests.rdd_BlockLii(sc, rdd, true, valueToBeBlocked, newValue, valueNotBlocked);
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
          GenericTests.rdd_BlockAll(sc, rdd, true, valueToBeBlocked, newValue)
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
          AccessMonitor.deRegisterPolicy(policy);
          GenericTests.rdd_BlockNone(sc, rdd, true, valueToBeBlocked, newValue);

          ssc.stop();
        }

      })
    (lines, testCasePassed)
  }

}
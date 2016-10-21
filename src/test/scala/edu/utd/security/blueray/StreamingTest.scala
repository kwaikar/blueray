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
  var sc: SparkContext = _;
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    sc = new SparkContext(conf)
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }
  @Test
  def testSparkStreaming() = {

    sc.setLocalProperty("PRIVILEDGE", Util.encrypt("ADMIN"));

    var inputFile = sc.textFile("hdfs://localhost/user/user_stream.csv")

    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/stream/", Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.sparkContext.getConf.set("spark.streaming.fileStream.minRememberDuration", "624000")

    var (lines, testCasePassed) = streamFile(ssc, policy)
    println(":" + lines.count())
    ssc.start()
    ssc.awaitTermination()
  } 
  def streamFile(ssc: org.apache.spark.streaming.StreamingContext, policy: edu.utd.security.blueray.Policy) = {

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat]("hdfs://localhost/stream/", defaultFilter(_), newFilesOnly = false).map(_._2.toString)
    var testCasePassed = false;
    lines.foreachRDD(rdd =>
      {
        if (rdd.collect().length != 0) {
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
          GenericTests.rdd_BlockLii(sc, rdd, true);
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
          GenericTests.rdd_BlockAll(sc, rdd, true)
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
          AccessMonitor.deRegisterPolicy(policy);
          GenericTests.rdd_BlockNone(sc, rdd, true);
          ssc.stop();
        }

      })
    (lines, testCasePassed)
  }
 
}
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
import org.apache.spark.sql.SQLContext
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

    // sc.setLogLevel("DEBUG")
    sc.setLocalProperty("PRIVILEDGE", Util.encrypt("ADMIN"));

    var inputFile = sc.textFile("hdfs://localhost/user/user_stream.csv")

    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/stream/", Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);
    // required so tht it can read exiting files
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.sparkContext.getConf.set("spark.streaming.fileStream.minRememberDuration", "624000")

    /*ssc.fileStream[LongWritable, Text, TextInputFormat]("hdfs://localhost/stream/",  defaultFilter: Path => Boolean, false).map(_._2.toString)*/
    var (lines, testCasePassed) = streamFile(ssc, policy) /*.textFileStream("hdfs://localhost/stream/")*/
    println(":" + lines.count())
    ssc.start()
    ssc.awaitTermination()
  }

  def streamFile(ssc: org.apache.spark.streaming.StreamingContext, policy: edu.utd.security.blueray.Policy) = {

    /*ssc.fileStream[LongWritable, Text, TextInputFormat]("hdfs://localhost/stream/",  defaultFilter: Path => Boolean, false).map(_._2.toString)*/
    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat]("hdfs://localhost/stream/", defaultFilter(_), newFilesOnly = false).map(_._2.toString)
    /*.textFileStream("hdfs://localhost/stream/")*/
    var testCasePassed = false;
    lines.foreachRDD(rdd =>
      {
        if (rdd.collect().length != 0) {
          assertDataSetSize(2, sc, rdd)
          sc.setLocalProperty(("PRIVILEDGE"), edu.utd.security.blueray.Util.encrypt("ADMIwN"));
          assertDataSetSize(3, sc, rdd)
          sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
          assertDataSetSize(2, sc, rdd)
          AccessMonitor.deRegisterPolicy(policy);
          assertDataSetSize(3, sc, rdd)
          testCasePassed = true;
          println("test case status: "+ testCasePassed)
          ssc.stop();
        }

      })
    (lines, testCasePassed)
  }

  def assertDataSetSize(count: Int, sc: SparkContext, inputFile: RDD[String]) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    assert(count == inputFile.collect().size)
    assert(count == inputFile.count(), "Count method testing")
    assert(count == inputFile.take(3).size, "take(3)  testing")
    assert(count == inputFile.takeSample(false, count, 0).size, "takeSample testing")
    println(inputFile.flatMap(_.split("\n")).countByValue().size + "-----------------------")
    assert(inputFile.flatMap(_.split("\n")).countByValue().size == count, "countByValue ")
    inputFile.foreach(println)
    assert(count == inputFile.map(x => (x(1), 1)).reduceByKey(_ + _).collect().size, "reduceByKey ")
    assert(count == inputFile.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
    assert(count == inputFile.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
    assert(inputFile.collect()(0).size == inputFile.first().size, "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(count == coalescedFile.count(), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(count == savedFile.count(), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }
}
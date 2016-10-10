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

/**
 * Unit Test class for testing AccessAuthorization functionality.
 */
class AccessAuthorizerTest {

  var sc: SparkContext = _;

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
  def testUtil() = {

    assert(Util.decrypt(Util.encrypt("Hello")) == "Hello");
  }
  @Test
  def testSpark() = {
    sc.setLogLevel("ERROR")
    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);

    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    assertDataSetSize(3, sc, inputFile);
    sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
    assertDataSetSize(2, sc, inputFile);
    AccessMonitor.deRegisterPolicy(policy);
    assertDataSetSize(3, sc, inputFile);
    println("")
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

  @Test
  def testForShell() {
    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", edu.utd.security.blueray.Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);
    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    assertDataSetSize(3, sc, inputFile);
    sc.setLocalProperty(("PRIVILEDGE"), edu.utd.security.blueray.Util.encrypt("ADMIN"));
    inputFile.collect().foreach(println)
    assertDataSetSize(2, sc, inputFile)

    //     val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost/sream/user_small.csv"), sc.hadoopConfiguration)
    //fs.delete(new org.apache.hadoop.fs.Path("hdfs://localhost/stream/"))

  }
  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }
}
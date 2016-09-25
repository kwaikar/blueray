package edu.utd.security.blueray

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.junit.Test

/**
 * Unit Test class for testing AccessAuthorization functionality.
 */
class AccessAuthorizerTest {
  @Test
  def testDriver() = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    var policy = new Policy("hdfs://localhost/user/user_small.csv", "Lii");
    AccessAuthorizationManager.registerPolicy(policy);
    assertDataSetSize(2, sc);
    AccessAuthorizationManager.deRegisterPolicy(policy);
    assertDataSetSize(3, sc);
    println("")

  }

  def assertDataSetSize(count: Int, sc: SparkContext) = {
    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    val currentMillis = System.currentTimeMillis;

    assert(count == inputFile.collect().size)
    assert(count == inputFile.count(), "Count method testing")
    assert(count == inputFile.take(3).size, "take(3)  testing")
    assert(count == inputFile.takeSample(false, count, 0).size, "takeSample testing")
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
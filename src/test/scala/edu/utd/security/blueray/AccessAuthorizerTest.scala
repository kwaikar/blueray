package edu.utd.security.blueray

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.junit.Test

/**
 * Unit Test class for testing AccessAuthorization functionality.
 */
class AccessAuthorizerTest {
  //@Test
  def testDriver() = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", "Lii");
    edu.utd.security.blueray.AccessAuthorizationManager.registerPolicy(policy);
    /*   policy = new Policy("hdfs://localhost/user/user.json", "Lii");
    AccessAuthorizationManager.registerPolicy(policy);
   */
    assertDataSetSize(2, sc);
    AccessAuthorizationManager.deRegisterPolicy(policy);
    assertDataSetSize(3, sc);
    println("")

  }
  @Test
  def testSparkSQL()=
  {
    
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //  sc.setLogLevel("DEBUG")

    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user.json", "Lii");   
    AccessAuthorizationManager.registerPolicy(policy);
    val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
     dfs.select("id").show()
  }
  
  
  //@Test
  def testSparkStreaming()={
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    val sc = new SparkContext(conf)
      sc.setLogLevel("DEBUG")
    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    
    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", "Lii");
    edu.utd.security.blueray.AccessAuthorizationManager.registerPolicy(policy);
    
     val ssc = new StreamingContext(sc, Seconds(2))
     
    val lines = ssc.textFileStream("hdfs://localhost/user/user_small.csv")
    val words = lines.flatMap(_.split("\n"))
    // Count each word in each batch
    val pairs = words.map(word => (1, 1))
    println(pairs.count().toString())
    val wordCounts = pairs.reduceByKey(_ + _)
    println("--->"+wordCounts.count().toString())
    wordCounts.print()
 ssc.start()
   
    ssc.awaitTermination()
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

  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }
}
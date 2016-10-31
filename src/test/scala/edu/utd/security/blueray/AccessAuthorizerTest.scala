package edu.utd.security.blueray

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
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
  def executeTestCases() {
    val valueToBlock = "Lii";
    val newValue = "------";
    testSpark("hdfs://localhost/user/user_small.csv",valueToBlock, newValue);
    new SQLTest().testSparkSQL(sc,"hdfs://localhost/user/user.json",valueToBlock, newValue);
     new StreamingTest().testSparkStreaming(sc,"hdfs://localhost/user/user_small.csv",valueToBlock, newValue);
  }

  private def testSpark(filePath:String,valueToBlock: String, newValue: String) = {
    sc.setLogLevel("ERROR")
    var policy = new edu.utd.security.blueray.Policy(filePath, Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);

    var inputFile = sc.textFile(filePath)
    sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
    GenericTests.rdd_BlockLii(sc, inputFile, true, "Lii", "------");
    sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
    GenericTests.rdd_BlockAll(sc, inputFile, true, "Lii", "------")
    sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
    AccessMonitor.deRegisterPolicy(policy);
    GenericTests.rdd_BlockNone(sc, inputFile, true, "Lii", "------");
  }

  private def testForShell() {
    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", edu.utd.security.blueray.Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);
    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    sc.setLocalProperty(("PRIVILEDGE"), edu.utd.security.blueray.Util.encrypt("ADMIN"));
    inputFile.collect().foreach(println)

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
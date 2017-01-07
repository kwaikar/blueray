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

 // @Test
  def testUtil() = {
    assert(Util.decrypt(Util.encrypt("Hello")) == "Hello");
    val sb = "00460-027-0120";

    // val regex="""(\d{3}-\d{3}-\d{4})""".r;
    val regex = "460-027-0120".r
    var replaceMent: StringBuilder = new StringBuilder();
    if (regex.findFirstIn(sb) != None && regex.findFirstIn(sb).get.length() > 0) {
      for (i <- 0 to regex.findFirstIn(sb).get.length()) {
        replaceMent.append("-")
      }
    }

    println("CLASS==" + sb.toString().trim() + ":" + regex.findAllIn(sb).length + " : " + regex.replaceAllIn(sb, replaceMent.toString()))
    val valueToBlock = "Lii";
    val valueNotBlocked = "saki U.";
    val newValue = "------";
    val filePath = "hdfs://localhost/user/user_small.csv";

    val valueToBlock1 = """(\d{3}-\d{3}-\d{4})""";
    println("===>" + valueToBlock1)
    println("-----------@#---------" + System.getProperty("user.name", ""))
    // sc.setLogLevel("DEBUG");
    // var policy = new edu.utd.security.blueray.Policy(filePath, edu.utd.security.blueray.Util.encrypt("ADMIN"), valueToBlock);
    // edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);

    /*var inputFile = sc.textFile(filePath)
    //sc.setLocalProperty(("PRIVILEDGE"), edu.utd.security.blueray.Util.encrypt("ADMIN"));
     println(inputFile.collect().size + " =======> " + inputFile.collect().mkString);*/

  }
  

  //@Test
  def executeSimpleBlockTestCase() {
    val valueToBlock = "Lii";
    val valueNotBlocked = "saki U.";
    val newValue = "------";
    testSpark("hdfs://localhost/user/user_small.csv", valueToBlock, newValue, valueNotBlocked);
    // new SQLTest().testSparkSQL(sc, "hdfs://localhost/user/user.json", valueToBlock, newValue, valueNotBlocked);
    //new StreamingTest().testSparkStreaming(sc,"hdfs://localhost/user/user_small.csv",valueToBlock, newValue,valueNotBlocked);
  }

  // @Test
  def executePhoneNumberBlockTestCase() {
    val valueToBlock = "460-027-0120";
    val valueNotBlocked = "460-028-0120";
    val newValue = "------";
    testSpark("hdfs://localhost/user/user_phone.csv", valueToBlock, newValue, valueNotBlocked);
    new SQLTest().testSparkSQL(sc, "hdfs://localhost/user/user_phone.json", valueToBlock, newValue, valueNotBlocked);
    //  new StreamingTest().testSparkStreaming(sc,"hdfs://localhost/user/user_phone.csv",valueToBlock, newValue,valueNotBlocked);
  }

  //  @Test
  def executeAllPhoneNumberBlockTestCase() {
    val valueToBlock = """(\d{3}-\d{3}-\d{4})""";
    val valueNotBlocked = "460-0a8-0120";
    val newValue = "------";
    testSpark("hdfs://localhost/user/user_all_phones.csv", valueToBlock, newValue, valueNotBlocked);
    new SQLTest().testSparkSQL(sc, "hdfs://localhost/user/user_all_phones.json", valueToBlock, newValue, valueNotBlocked);
    //  new StreamingTest().testSparkStreaming(sc, "hdfs://localhost/user/user_phone.csv", valueToBlock, newValue, valueNotBlocked);
  }

  private def testSpark(filePath: String, valueToBlock: String, newValue: String, valueNotBlocked: String) = {
    var policy = new edu.utd.security.blueray.Policy(filePath, Util.encrypt("ADMIN"), valueToBlock);
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);

    var inputFile = sc.textFile(filePath)
    sc.setLocalProperty(("USER"), ("kanchan"));
    GenericTests.rdd_BlockLii(sc, inputFile, true, valueToBlock, newValue, valueNotBlocked);
    sc.setLocalProperty(("USER"), ("SomeRANDOMSTRIng"));
    GenericTests.rdd_BlockAll(sc, inputFile, true, valueToBlock, newValue)
    sc.setLocalProperty(("USER"), ("ADMIN"));
    AccessMonitor.deRegisterPolicy(policy);
    GenericTests.rdd_BlockNone(sc, inputFile, true, valueToBlock, newValue);
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

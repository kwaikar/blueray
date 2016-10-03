import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.junit.Test

import edu.utd.security.blueray.AccessMonitor
import edu.utd.security.blueray.Util
import org.junit.Before
import org.apache.spark.api.java.JavaSparkContext
import org.junit.After

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

  def main(args: Array[String]): Unit = {
    testSparkSQL();
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
    assertDataSetSize(3, sc);
    sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
    assertDataSetSize(2, sc);
    AccessMonitor.deRegisterPolicy(policy);
    assertDataSetSize(3, sc);
    println("")
  }

  def assertDataSetSize(count: Int, sc: SparkContext) = {
    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
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

  @Test
  def testSparkSQL() =
    {

      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));

      var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user.json", Util.encrypt("ADMIN"), "Lii");
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
      assert(dfs.select("id").collect().length == 2)
      println("done")
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
      assert(dfs.select("id").collect().length == 3)

      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      AccessMonitor.deRegisterPolicy(policy);
      assert(dfs.select("id").collect().length == 3)

    }

  @Test
  def testSparkStreaming() = {
    // sc.setLogLevel("DEBUG")
    sc.setLocalProperty("PRIVILEDGE", Util.encrypt("ADMIN"));

    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")

    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);

    val ssc = new StreamingContext(sc, Seconds(2))

    val lines = ssc.textFileStream("hdfs://localhost/user/user_small.csv")
    val words = lines.flatMap(_.split("\n"))
    // Count each word in each batch
    val pairs = words.map(word => (1, 1))
    println(pairs.count().toString())
    val wordCounts = pairs.reduceByKey(_ + _)
    println("--->" + wordCounts.count().toString())
    wordCounts.print()
    ssc.start()

    ssc.stop()
  }

  @Test
  def testForShell() {
    var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user_small.csv", Util.encrypt("ADMIN"), "Lii");
    edu.utd.security.blueray.AccessMonitor.enforcePolicy(policy);
    assertDataSetSize(3, sc);
    sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
    var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
    inputFile.collect().foreach(println)
    assertDataSetSize(2, sc);

  }
  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }
}
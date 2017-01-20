package edu.utd.security.mondrian

import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.After
import org.junit.Before
import org.junit.Test

class MondrianTest {
  var sc: SparkContext = _;

  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    conf.set("POLICY_FILE_PATH","hdfs://localhost/blueray/empty_policies.csv");
    sc = new SparkContext(conf)
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

 // @Test
  def testMondrian() = {
     sc.setLogLevel("ERROR");
    Mondrian.kanonymize("hdfs://localhost/user/adult.data2.txt", getClass.getResource("/metadata.xml").getPath, "/home/kanchan/op.txt", 3);

    val testCaseInputFile = scala.io.Source.fromURL(getClass.getResource("/mondrian_test_result.txt"))
    val expectedString = try testCaseInputFile.mkString finally testCaseInputFile.close()

    val outputFile = scala.io.Source.fromFile("/home/kanchan/op.txt/part-00000")
    val outputString = try outputFile.mkString finally outputFile.close()
    println("|"+expectedString.trim()+"|")
    println("=|"+outputString.trim()+"|=")
    assert(Mondrian.getDiscernabilityMetric().equals(434.0));
    assert(expectedString.trim().equals(outputString.trim()))

  }

}
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
import org.apache.spark.sql.SQLContext

/**
 * Unit Test class for testing AccessAuthorization functionality.
 */
class BlockingTest {

  var sc: SparkContext = _;

  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    sc = SparkContext.getOrCreate(conf)
    // sc.setLocalProperty("BLUERAY_POLICIES_PATH","hdfs://localhost/blueray/empty_policies.csv");
    //("POLICYMANAGER_END_POINT","http://10.176.147.70:8084/bluerayWebapp")
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

  //@Test
  def testBocking() = {
    val filePath="hdfs://localhost/user/adult.2lines.csv";
    sc.setLogLevel("ERROR");
    println("Starting")
    var inputFile = sc.textFile(filePath)
    println("Plain =>"+ inputFile.collect().mkString)
    assert(inputFile.collect().mkString.equals("--,------,19,Black--,----,19,Black"));
    val sqlContext = new SQLContext(sc)
    val dfs = sqlContext.read.json(filePath)
    println("DF ==>" + dfs.collect().mkString);
    assert(dfs.collect().mkString.equals("--,------,19,Black--,----,19,Black"));
    println("RDD ==>" + dfs.rdd.collect().mkString);
    assert( dfs.rdd.collect().mkString.equals("--,------,19,Black--,----,19,Black"));
    
    val fileName="hdfs://localhost/user/Blocking_Aspect_output";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
   println(  fs.delete(new org.apache.hadoop.fs.Path(fileName), true) )
    
    dfs.rdd.saveAsTextFile("hdfs://localhost/user/Blocking_Aspect_output");
  }
   
  
 // @Test
  def testGeneralizing() = {
    val filePath="hdfs://localhost/user/adult.2lines.csv";
    sc.setLogLevel("ERROR");
    println("Starting")
    var inputFile = sc.textFile(filePath)
    println("Plain =>"+ inputFile.collect().mkString)
    val sqlContext = new SQLContext(sc)
    
    
    val fileName="hdfs://localhost/user/Blocking_Aspect_output";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
   println(  fs.delete(new org.apache.hadoop.fs.Path(fileName), true) )
    val filePath2="hdfs://localhost/user/adult.2lines.csv";
    val dfs3 = sqlContext.read.json(filePath2)
   inputFile.saveAsTextFile("hdfs://localhost/user/Blocking_Aspect_output");

    
    val dfs = sqlContext.read.json(filePath)
    val dfs2 = sqlContext.read.json(filePath)
    
    println("DF ==>" + dfs.collect().mkString.trim()+"| ");
    println("RDD ==>" + dfs2.rdd.collect().mkString+"| ");
     assert(inputFile.collect().mkString.trim().equals("*,38111.0_38120.0,19,Black*,37880.0_37890.0,31,Amer-Indian-Eskimo"));
     assert(dfs.collect().mkString.trim().equals("[*,38111.0_38120.0,1][*,37880.0_37890.0,31,Amer-Indian]"));
     assert(dfs2.rdd.collect().mkString.trim().equals("[*,38111.0_38120.0,1][*,37880.0_37890.0,31,Amer-Indian]"));

      }
   
  
  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }
}

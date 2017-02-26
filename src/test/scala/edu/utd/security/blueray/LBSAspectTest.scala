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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

/**
 * Unit Test class for testing AccessAuthorization functionality.
 */
class LBSAspectTest {

  var sc: SparkContext = _;

  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    sc = SparkContext.getOrCreate(conf)
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

  @Test
  def testGeneralizing() = {
    val filePath = "hdfs://localhost/user/adult.1lineGen.csv";
    sc.setLogLevel("ERROR");
    println("Starting")
    var inputFile = sc.textFile(filePath)
    println("Plain =>"+ inputFile.collect().mkString)
/*val values = inputFile.flatMap(_.split("\n")).map(_.split(",")).map(x=>((x(2),x(3)),1));
values.reduceByKey(_+_).take(5).foreach(println);*/
    /*
    sc.setLocalProperty("USER", "kanchan");
    val customSchema = StructType(Array(
      StructField("gender", StringType, true),
      StructField("zip", StringType, true),
      StructField("age", StringType, true),
      StructField("race", StringType, true)))

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.schema(customSchema).csv(filePath)
    df.groupBy("age").count().show()*/

    /*    val sqlContext = new SQLContext(sc)
  
    
    
    val fileName="hdfs://localhost/user/Blocking_Aspect_output";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
   println(  fs.delete(new org.apache.hadoop.fs.Path(fileName), true) )
    val filePath2="hdfs://localhost/user/adult.1line.csv";
    val dfs3 = sqlContext.read.json(filePath2)
   inputFile.saveAsTextFile("hdfs://localhost/user/Blocking_Aspect_output");

    
    val dfs = sqlContext.read.json(filePath)
    val dfs2 = sqlContext.read.json(filePath)
    
    println("DF ==>" + dfs.collect().mkString.trim()+"| ");
    println("RDD ==>" + dfs2.rdd.collect().mkString+"| ");
     assert(inputFile.collect().mkString.trim().equals("*,38111.0_38120.0,19,Black*,37880.0_37890.0,31,Amer-Indian-Eskimo"));
     assert(dfs.collect().mkString.trim().equals("[*,38111.0_38120.0,1][*,37880.0_37890.0,31,Amer-Indian]"));
     assert(dfs2.rdd.collect().mkString.trim().equals("[*,38111.0_38120.0,1][*,37880.0_37890.0,31,Amer-Indian]"));*/

  }

  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }
}

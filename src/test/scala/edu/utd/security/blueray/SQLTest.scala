package edu.utd.security.blueray

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.junit.After
import org.junit.Before
import org.junit.Test

class SQLTest {
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
  def testSparkSQL() =
    {
      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user.json", Util.encrypt("ADMIN"), "Lii");
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
      
      dfs.select("id").collect().foreach(println)
      assert(dfs.select("id").collect().length == 2)
      dfs.filter(!_.mkString.contains("Lii"));
      dfs.collect().foreach(println)
      assert(dfs.select("age").collect().length == 2)
      assert(dfs.count() == 2)
      assert(dfs.groupBy("age").count() == 2)
      val currentMillis = System.currentTimeMillis;

      val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
      dfs.write.format("json").save(fileName)
    println("==========================>"+fileName)
  
      var fileSaved = sc.textFile(fileName)
      println("==============wd============>"+fileName)
      fileSaved.collect().foreach(println);
      println("============dcd==============>")
      assert(2 == fileSaved.count(), "saved testing")

      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
      assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

      //assertDataSetSize(2, sc, dfs.rdd);
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
      //assertDataSetSize(3, sc, dfs.rdd );
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      AccessMonitor.deRegisterPolicy(policy);
      //      assertDataSetSize(3, sc, dfs.rdd);
    }

  //@Test
  def testSparkSQLToRDDVersion() =
    {
      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user.json", Util.encrypt("ADMIN"), "Lii");
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
      dfs.select("id").collect().foreach(println)
      println("=============----------------->>>>>" + dfs.select("id").collect().length)
      sc.setLogLevel("DEBUG")
      assert(dfs.select("id").collect().length == 2)
      assertDataSetSize(2, sc, dfs.rdd);
      println("=============----------------->>>>>" + dfs.select("id").count())
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
      assert(dfs.select("id").collect().length == 3)
      assertDataSetSize(3, sc, dfs.rdd);
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      AccessMonitor.deRegisterPolicy(policy);
      assert(dfs.select("id").collect().length == 3)
      assertDataSetSize(3, sc, dfs.rdd);
      println("==========================>")
    }

  def assertDataSetSize(count: Int, sc: SparkContext, inputFile: RDD[Row]) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    assert(count == inputFile.collect().size)
    assert(count == inputFile.count(), "Count method testing")
    assert(count == inputFile.take(3).size, "take(3)  testing")
    assert(count == inputFile.takeSample(false, count, 0).size, "takeSample testing")
    inputFile.foreach(println)
    // assert(count == inputFile.map(x => (x(1), 1)).reduceByKey(_ + _).collect().size, "reduceByKey ")
    //assert(count == inputFile.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
    // assert(count == inputFile.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
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
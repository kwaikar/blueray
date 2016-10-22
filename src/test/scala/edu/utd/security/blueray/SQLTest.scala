package edu.utd.security.blueray

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
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
        

      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
          df_BlockNone(sc, dfs)
      AccessMonitor.enforcePolicy(policy);
      GenericTests.df_BlockLii(sc, dfs)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMON"));
      GenericTests.df_BlockAll(sc, dfs)
      AccessMonitor.deRegisterPolicy(policy);
      df_BlockNone(sc, dfs)

    } 
  
  def df_BlockNone(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame) = {
    val fileName = "hdfs://localhost/user/user" + System.currentTimeMillis() + ".json";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)

    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------")
    assert(dfs.select("id").collect().length == 3)
    assert(dfs.select("id").collect().mkString.contains("Lii"))
    assert(dfs.select("id").collect().mkString.contains("jane"))
    assert(dfs.select("id").collect().mkString.contains("saki"))
    assert(!dfs.select("id").collect().mkString.contains("------"));
    assert(dfs.select("age").collect().mkString.contains("23"));
    assert(dfs.select("age").collect().mkString.contains("22"));
    assert(dfs.select("age").collect().mkString.contains("21"));
    dfs.filter(!_.mkString.contains("Lii"));
    dfs.collect().foreach(println)
    assert(dfs.select("age").collect().length == 3)
    println("===>>>>>>>>>>>>>=" + dfs.groupBy("age").count().count())
    assert(dfs.groupBy("age").count().count() == 3)
    dfs.write.format("json").save(fileName)

    val fileSaved = sc.textFile(fileName)
    println("==============wd============>" + fileName)
    fileSaved.collect().foreach(println);
    println("============dcd==============>")
    assert(3 == fileSaved.count(), "saved testing")

    assert(fileSaved.collect().mkString.contains("Lii"))
    assert(fileSaved.collect().mkString.contains("jane"))
    assert(fileSaved.collect().mkString.contains("saki"))
    assert(!fileSaved.collect().mkString.contains("------"));

    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))
  }
  @Test
  def testSparkSQLToRDDVersion() =
    {
      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user.json", Util.encrypt("ADMIN"), "Lii");
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
    //  GenericTests.rdd_BlockLii(sc, dfs.rdd, false);
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
     GenericTests. rdd_BlockAll(sc, dfs.rdd, false)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      AccessMonitor.deRegisterPolicy(policy);
      GenericTests.rdd_BlockNone(sc, dfs.rdd, false);
    }

}
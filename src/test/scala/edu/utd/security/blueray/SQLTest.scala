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
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
      GenericTests.df_BlockLii(sc, dfs)

      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMON"));
     GenericTests.df_BlockAll(sc, dfs)
      
      AccessMonitor.deRegisterPolicy(policy);
      GenericTests.df_BlockNone(sc, dfs)

    }


  // @Test
  def testSparkSQLToRDDVersion() =
    {
      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      var policy = new edu.utd.security.blueray.Policy("hdfs://localhost/user/user.json", Util.encrypt("ADMIN"), "Lii");
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json("hdfs://localhost/user/user.json")
      GenericTests.rdd_BlockLii(sc, dfs.rdd, false);
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
      GenericTests.rdd_BlockAll(sc, dfs.rdd, false)

      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      AccessMonitor.deRegisterPolicy(policy);
      assert(dfs.select("id").collect().length == 3)
      GenericTests.rdd_BlockNone(sc, dfs.rdd, false);
      println("==========================>")
    }

}
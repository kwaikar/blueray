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

  def testSparkSQL(sc: SparkContext, filePath: String, valueToBlock: String, newValue: String, valueNotBlocked: String) =
    {
      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      var policy = new edu.utd.security.blueray.Policy(filePath, Util.encrypt("ADMIN"), valueToBlock);
      val dfs = sqlContext.read.json(filePath)
      GenericTests.df_BlockNone(sc, dfs, valueToBlock, newValue)
      AccessMonitor.enforcePolicy(policy);
      GenericTests.df_BlockID(sc, dfs, valueToBlock, newValue, valueNotBlocked)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMON"));
      GenericTests.df_BlockAll(sc, dfs, valueToBlock, newValue)
      AccessMonitor.deRegisterPolicy(policy);
      GenericTests.df_BlockNone(sc, dfs, valueToBlock, newValue)
      testSparkSQLToRDDVersion(sc, filePath, valueToBlock, newValue, valueNotBlocked);
    }

  private def testSparkSQLToRDDVersion(sc: SparkContext, filePath: String, valueToBlock: String, newValue: String, valueNotBlocked: String) =
    {
      val sqlContext = new SQLContext(sc)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      var policy = new edu.utd.security.blueray.Policy(filePath, Util.encrypt("ADMIN"), valueToBlock);
      AccessMonitor.enforcePolicy(policy);
      val dfs = sqlContext.read.json(filePath)
      GenericTests.rdd_BlockLii(sc, dfs.rdd, false, valueToBlock, newValue, valueNotBlocked);
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("SomeRANDOMSTRIng"));
      GenericTests.rdd_BlockAll(sc, dfs.rdd, false, valueToBlock, newValue)
      sc.setLocalProperty(("PRIVILEDGE"), Util.encrypt("ADMIN"));
      AccessMonitor.deRegisterPolicy(policy);
      GenericTests.rdd_BlockNone(sc, dfs.rdd, false, valueToBlock, newValue);
    }

}
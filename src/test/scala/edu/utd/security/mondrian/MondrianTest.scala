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
    sc = new SparkContext(conf)
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

  @Test
  def testMondrian() = {
      Mondrian.kanonymize(4);
    
  }
  
}
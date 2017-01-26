package edu.utd.security.lbs

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.After
import org.junit.Before

class LBSTest {
  var sc: SparkContext = _;

  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    conf.set("POLICY_FILE_PATH", "hdfs://localhost/blueray/empty_policies.csv");
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
    val lbs = new LBS();
    lbs.lbs("hdfs://localhost/user/adult.data2.txt", getClass.getResource("/metadata.xml").getPath, "/home/kanchan/op.txt", 3);
    val dataReader = new DataReader(sc);
    val metadataVal = dataReader.readMetadata(getClass.getResource("/metadata.xml").getPath);
    assert(!lbs.isGLeafNode(lbs.getFirstDataLine()._2));
    var newMap = new HashMap[Int, String]();
    for (i <- 0 to metadataVal.numColumns() - 1) {

      if (metadataVal.getMetadata(i).get.getColType() == 's') {
        newMap.put(i, metadataVal.getMetadata(i).get.getRootCategory().value());
      } else {
        newMap.put(i, metadataVal.getMetadata(i).get.getMin() + "_" + metadataVal.getMetadata(i).get.getMax());
      }
      println("Putting+ " + i + " - " + newMap.get(i).get)
    }

    assert(lbs.isGLeafNode(newMap));

  }

}
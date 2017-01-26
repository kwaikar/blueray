package edu.utd.security.lbs

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.apache.spark.rdd.RDD

class LBSTest {
  var sc: SparkContext = _;
  var metadataVal: Metadata = null;
  var record: (Long, scala.collection.mutable.Map[Int, String]) = null
  var linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;

  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
    conf.set("POLICY_FILE_PATH", "hdfs://localhost/blueray/empty_policies.csv");
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR");
    sc.setLocalProperty("USER", "kanchan");
    val dataReader = new DataReader(sc);
    linesRDD = dataReader.readDataFile("hdfs://localhost/user/adult.data2.txt", true);
    linesRDD.cache();
    val metadata = LBS.Metadata.getInstance(sc, dataReader, getClass.getResource("/metadata.xml").getPath);
    record = linesRDD.first();
    metadataVal = dataReader.readMetadata(getClass.getResource("/metadata.xml").getPath);
    LBS.lbs("hdfs://localhost/user/adult.data2.txt", getClass.getResource("/metadata.xml").getPath, "/home/kanchan/op.txt", 3);
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

  //@Test
  def testLBSIsLeafNodeFunction() {

    assert(!LBS.isGLeafNode(record._2));
    var newMap = getTopMostGeneralization();
    assert(LBS.isGLeafNode(newMap));
  }

  def getTopMostGeneralization() = {
    var newMap = new HashMap[Int, String]();
    for (i <- 0 to metadataVal.numColumns() - 1) {
      if (metadataVal.getMetadata(i).get.getColType() == 's') {
        newMap.put(i, metadataVal.getMetadata(i).get.getRootCategory().value());
      } else {
        newMap.put(i, metadataVal.getMetadata(i).get.getMin() + "_" + metadataVal.getMetadata(i).get.getMax());
      }
      // println("Putting+ " + i + " - " + newMap.get(i).get)
    }
    newMap
  }
  // @Test
  def testInformationLoss() {
    println(record._2.mkString(","));
    //   LBS.lbs("hdfs://localhost/user/adult.data2.txt", getClass.getResource("/metadata.xml").getPath, "/home/kanchan/op.txt", 3);
    assert((41.54091679442631 == LBS.getInformationLoss(record._2)));
  }

  // @Test
  def testGetMaximulInformationLoss() {
    val loss = LBS.getMaximulInformationLoss();
    println("==>" + loss);
    assert(17.8622848986764 == loss);
  }
  @Test
  def testPublisherBenefit() {
    assert(LBS.getTotalCount() == 92)
    var ben = LBS.getPublishersBenefit(record._2, new LBSParameters(4, 1200, 2000, 10));
    println("Record ==>" + ben);

    ben = LBS.getPublishersBenefit(getTopMostGeneralization(), new LBSParameters(4, 1200, 2000, 10));
    println("Top ==>" + ben);

    for (child <- LBS.getChildren(record._2)) {
      println(child)
      assert(LBS.isRecordASuperSetOfRecordB(record._2, child));
    }
    //
    // assert(17.8622848986764== ben);
  }
}
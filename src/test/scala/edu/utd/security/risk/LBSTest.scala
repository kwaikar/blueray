package edu.utd.security.risk

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.After
import org.junit.Before
import org.junit.Test

class LBSTest {
  var sc: SparkContext = _;
  var metadataVal: Metadata = null;
  var record: (Long, scala.collection.mutable.Map[Int, String]) = null
  var linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
  
  @Before
  def setUp() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]");
    conf.set("POLICY_FILE_PATH", "hdfs://localhost/blueray/empty_policies.csv");
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR");
    sc.setLocalProperty("USER", "kanchan");
    val dataReader = new DataReader(sc);
    linesRDD = dataReader.readDataFile("hdfs://localhost/user/adult.data2.txt", true);
    linesRDD.cache();
    metadataVal = dataReader.readMetadata("hdfs://localhost//user/metadata_lbs.xml");
    LBS.setup("hdfs://localhost/user/adult.data2.txt", "hdfs://localhost//user/metadata_lbs.xml", "/home/kanchan/op.txt", new LBSParameters(4, 1200, 2000, 10));
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

  //@Test
  def testLBSIsLeafNodeFunction() {

    assert(!LBS.isGLeafNode(record._2,"hdfs://localhost//user/metadata_lbs.xml"));
    var newMap = getTopMostGeneralization();
    assert(LBS.isGLeafNode(newMap,"hdfs://localhost//user/metadata_lbs.xml"));
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
   @Test
  def testInformationLoss() {
//    assert((41.54091679442631 == LBS.getInformationLoss(record._2)));
  }

  // @Test
  def testGetMaximulInformationLoss() {
    val loss = LBS.Metadata.getMaximulInformationLoss(sc,"hdfs://localhost//user/metadata_lbs.xml",linesRDD);
    println("==>" + loss);
    assert(17.8622848986764 == loss);
  }
 // @Test
  def testPublisherBenefit() {
    assert(LBS.Metadata.getTotalCount(sc,linesRDD) == 92)
    var ben = LBS.getPublishersBenefit(record._2, new LBSParameters(4, 1200, 2000, 10),"hdfs://localhost//user/metadata_lbs.xml",linesRDD);
    println("Record ==>" + ben);

    ben = LBS.getPublishersBenefit(getTopMostGeneralization(), new LBSParameters(4, 1200, 2000, 10),"hdfs://localhost//user/metadata_lbs.xml",linesRDD);
    println("Top ==>" + ben);

  }
 // @Test
  def testSuperSets()
  {
      for (child <- LBS.getChildren(record._2,"hdfs://localhost//user/metadata_lbs.xml")) {
      println(child)
      assert(LBS.isRecordASuperSetOfRecordB( child,record._2,"hdfs://localhost//user/metadata_lbs.xml"));
    }
  }
  //@Test
  def testRiskOfStrategy()
  {
    println(LBS.getRiskOfStrategy(record._2,"hdfs://localhost//user/metadata_lbs.xml",linesRDD));
    assert (LBS.getRiskOfStrategy(record._2,"hdfs://localhost//user/metadata_lbs.xml",linesRDD) ==1)
    println(LBS.getRiskOfStrategy(getTopMostGeneralization(),"hdfs://localhost//user/metadata_lbs.xml",linesRDD) +" + "+(1.0/92));
    assert(LBS.getRiskOfStrategy(getTopMostGeneralization(),"hdfs://localhost//user/metadata_lbs.xml",linesRDD) == (1.0/92))
    
     for (child <- LBS.getChildren(record._2,"hdfs://localhost//user/metadata_lbs.xml")) {
      println(child)
      println("==>"+LBS.getRiskOfStrategy( child,"hdfs://localhost//user/metadata_lbs.xml",linesRDD))
      assert(LBS.getRiskOfStrategy( child,"hdfs://localhost//user/metadata_lbs.xml",linesRDD)==1);
    }
    
  }
  
}
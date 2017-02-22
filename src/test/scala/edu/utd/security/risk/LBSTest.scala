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
    val dataReader = new DataReader();
    linesRDD = dataReader.readDataFile(sc,"hdfs://localhost/user/adult.data2.txt", true);
    linesRDD.cache();
    metadataVal = LBSWithoutAspect.Metadata.getInstance(sc).value;
    LBSWithoutAspect.setup("hdfs://localhost/user/adult.data2.txt", "/home/kanchan/metadata.xml", "/home/kanchan/op.txt", new LBSParameters(4, 1200, 2000),true,50);
  }
  @After
  def destroy() {

    sc.stop();
    sc = null;
  }

  //@Test
  def testLBSIsLeafNodeFunction() {

    assert(!LBSWithoutAspect.isGLeafNode(record._2));
    var newMap = getTopMostGeneralization();
    assert(LBSWithoutAspect.isGLeafNode(newMap));
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
    assert((41.54091679442631 == LBSWithoutAspect.getInformationLoss(record._2)));
  }

   //@Test
  def testGetMaximulInformationLoss() {
    val loss = LBSWithoutAspect.Metadata.getMaximulInformationLoss(sc);
    println("==>" + loss);
    assert(17.8622848986764 == loss);
  }
 // @Test
  def testPublisherBenefit() {
    assert(LBSWithoutAspect.Metadata.getTotalCount(sc,linesRDD) == 92)
    var ben = LBSWithoutAspect.getPublishersBenefit(record._2, new LBSParameters(4, 1200, 2000) /*,linesRDD*/);
    println("Record ==>" + ben);

    ben = LBSWithoutAspect.getPublishersBenefit(getTopMostGeneralization(), new LBSParameters(4, 1200, 2000) /*,linesRDD*/);
    println("Top ==>" + ben);

  }
 // @Test
  def testSuperSets()
  {
      for (child <- LBSWithoutAspect.getChildren(record._2)) {
      println(child)
    //  assert(LBSWithoutAspect.isRecordASuperSetOfRecordB( child,record._2));
    }
  }
  //@Test
  def testRiskOfStrategy()
  {
    println(LBSWithoutAspect.getRiskOfStrategy(record._2,LBSWithoutAspect.Metadata.getInstance(sc)/*,linesRDD*/));
    assert (LBSWithoutAspect.getRiskOfStrategy(record._2,LBSWithoutAspect.Metadata.getInstance(sc)/*,linesRDD*/) ==1)
    println(LBSWithoutAspect.getRiskOfStrategy(getTopMostGeneralization(),LBSWithoutAspect.Metadata.getInstance(sc)/*,linesRDD*/) +" + "+(1.0/92));
    assert(LBSWithoutAspect.getRiskOfStrategy(getTopMostGeneralization(),LBSWithoutAspect.Metadata.getInstance(sc)/*,linesRDD*/) == (1.0/92))
    
     for (child <- LBSWithoutAspect.getChildren(record._2)) {
      println(child)
      println("==>"+LBSWithoutAspect.getRiskOfStrategy( child,LBSWithoutAspect.Metadata.getInstance(sc)/*,linesRDD*/))
      assert(LBSWithoutAspect.getRiskOfStrategy( child,LBSWithoutAspect.Metadata.getInstance(sc)/*,linesRDD*/)==1);
    }
    
  }
  
}
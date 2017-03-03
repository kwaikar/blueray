package edu.utd.security.risk

import scala.annotation.migration
import scala.annotation.varargs
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.runtime.universe
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import edu.utd.security.mondrian.DataWriter
import edu.utd.security.risk.LBSAlgorithm

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
object LBSAndLSH {

  var sc: SparkContext = null;

  def getSC(): SparkContext =
    {
      sc
    }

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("Program variables expected : <SPARK_MASTER> <HDFS_Data_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <adversaryAttackCost> <USE_LSH(true/false)> <LSH_NUM_NEIGHBORS>")
    } else {
      sc = SparkSession
        .builder.appName("LBS").master(args(0)).getOrCreate().sparkContext;
      sc.setLogLevel("ERROR");
      setup(args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7).toBoolean, args(8).toInt)
    }
  }

  def setup(hdfsFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: Boolean, numNeighbours: Int) {
    var linesRDD = new DataReader().readDataFile(getSC(), hdfsFilePath, true).cache();
    lbs(outputFilePath, linesRDD, useLSH, lbsParam, numNeighbours);
  }
  def lbs(outputFilePath: String, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], useLSH: Boolean, lbsParam: LBSParameters, numNeighbours: Int) {

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH) {
      val output = lsh(linesRDD, lbsParam, numNeighbours)
      rdds = output._3
      totalPublisherPayOff = output._1
      totalAdvBenefit = output._2
    } else {

      val i = 29779;
      val algorithm = new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam, LBSMetadata.getPopulation(), LBSMetadata.getZip());

      val optimalRecord = algorithm.findOptimalStrategy(linesRDD.lookup(i.longValue())(0));
      println(optimalRecord);
      /*  val output = linesRDD.map({case (x,y)=>(x,new    LBSAlgorithm(LBSMetadata.getInstance(),lbsParam,LBSMetadata.getPopulation(),LBSMetadata.getZip()).findOptimalStrategy(y))}).sortByKey().values;
      val publisherBenefit=output.map({case(x,y,z)=>(x)}).mean();
      val advBenefit=output.map({case(x,y,z)=>(y)}).mean();
      val records=output.map({case(x,y,z)=>(z)});
       
      println("Avg PublisherPayOff found: " +publisherBenefit)
      println("Avg AdversaryBenefit found: " +advBenefit)
      new DataWriter(getSC()).writeRDDToAFile(outputFilePath, records.map(x=>x.toSeq.sortBy(_._1).map(_._2).mkString(",")));
*/

    }

  }

  def lsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int): (Double, Double, List[RDD[(Int, String)]]) = {

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;
    var counter = 0;
    var predicted = 0;

    /**
     * Build LSHmodel
     */
    val metadata = LBSMetadata.getInstance();
    val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata));

    val inputToModel = linesRDD.map({
      case (x, y) => ({
        val row = LSHUtil.extractRow(metadata, columnStartCounts.value, y, true)
        (x.intValue(), Vectors.dense(row))
      })
    }).collect().toSeq

    val dataFrame = new SQLContext(sc).createDataFrame(inputToModel).toDF("id", "keys");
    val key = Vectors.dense(1.0, 0.0)
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(30.0)
      .setNumHashTables(5)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(dataFrame)
    val txModel = model.transform(dataFrame)
    txModel.show();

    var keysRemaining = linesRDD.map({ case (x, y) => (x, true) }).collect().toMap;
    val linesRDDBroadcast = sc.broadcast(linesRDD.collect());
    while (keysRemaining != null && keysRemaining.size > 0) {

      // Pick up a key.
      val key = keysRemaining.last._1

      val currentRecord = linesRDD.lookup(key)(0);
      val algorithm = new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam, LBSMetadata.getPopulation(), LBSMetadata.getZip());

      val optimalRecord = algorithm.findOptimalStrategy(currentRecord)
      predicted = predicted + 1;
      totalPublisherPayOff += optimalRecord._1;
      totalAdvBenefit += optimalRecord._2;
      val arr = optimalRecord._3.toArray
      val nonQuasiMap = linesRDD.lookup(key)(0)
      var quasiGeneralizedMap = scala.collection.mutable.Map[Int, String]();
      for (column <- metadata.getQuasiColumns()) {
        quasiGeneralizedMap.put(column.getIndex(), optimalRecord._3.get(column.getIndex()).get.trim());
      }

      val outputMap = (nonQuasiMap ++= quasiGeneralizedMap).toArray.sortBy(_._1).map(_._2);
      list += ((key.intValue(), outputMap.mkString(",")));
      keysRemaining = keysRemaining - key;
      val neighborsRow = model.approxNearestNeighbors(txModel, Vectors.dense(LSHUtil.extractRow(metadata, columnStartCounts.value, currentRecord, true)), numNeighborsVal.value).collectAsList().asInstanceOf[java.util.List[Row]]
      val nebMap = sc.parallelize(neighborsRow.toArray.asInstanceOf[Array[Row]]).map({ case (row) => (row(0).asInstanceOf[Int].longValue(), (row(1).asInstanceOf[DenseVector]).values) });
      val neighbors = nebMap.filter({
        case (x, y) => {
          if (keysRemaining.contains(x)) {

            val map = LSHUtil.extractReturnObject(metadata, columnStartCounts.value, y)
            var neighborIsSubSet = true;
            breakable {
              for (column <- metadata.getQuasiColumns()) {

                val genHierarchyValue = optimalRecord._3.get(column.getIndex()).get.trim()
                val neighborValue = map.get(column.getIndex()).get.trim();

                if (genHierarchyValue != neighborValue) {
                  if (column.getColType() == 's') {
                    val genCategory = column.getCategory(genHierarchyValue);
                    if (!genCategory.children.contains(neighborValue)) {
                      neighborIsSubSet = false;
                      break;
                    }
                  } else {
                    val minMax1 = LSHUtil.getMinMax(genHierarchyValue);
                    val minMax2 = LSHUtil.getMinMax(neighborValue);
                    //println("Mismatched :  " + minMax1 + " " + minMax2)
                    if (minMax1._1 > minMax2._1 || minMax1._2 < minMax2._2) {
                      neighborIsSubSet = false;
                      break;
                    }
                  }
                }
              }
            }
            neighborIsSubSet;
          } else {
            false;
          }
        }
      });
      val keys = neighbors.keys.collect();
      keysRemaining = keysRemaining -- keys;
      counter += keys.size
      println("Neighbours found : " + counter + " " + "keys Remaining  =>" + (keysRemaining.size) + "-->" + quasiGeneralizedMap.mkString(","))
      totalPublisherPayOff += keys.size * optimalRecord._1;
      totalAdvBenefit += keys.size * optimalRecord._2;

      val neighboursOriginal = linesRDD.filter({ case (x, y) => keys.contains(x) });
      val newNeighBours = neighboursOriginal.map({
        case (x, y) => {

          var newY: scala.collection.mutable.Map[Int, String] = new scala.collection.mutable.HashMap[Int, String]();
          newY ++= y;
          for (i <- metadata.getQuasiColumns()) {
            newY.remove(i.getIndex())
          }
          newY ++= quasiGeneralizedMap;
          (x.intValue(), newY.toArray.sortBy(_._1).map(_._2).mkString(","))
        }
      });
      rdds :+= newNeighBours;
    }

    val vl = getSC().parallelize(list.toList)
    rdds = rdds :+ vl;
    println("Number of predictions done :" + predicted + " : neighbours: " + counter);
    return (totalPublisherPayOff, totalAdvBenefit, rdds);
  }

}
package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.AccumulatorParam
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import edu.utd.security.mondrian.DataWriter
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.DenseVector

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
      val output = linesRDD.map({ case (x, y) => (x, new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam, LBSMetadata.getPopulation(), LBSMetadata.getZip()).findOptimalStrategy(y)) }).sortByKey().values;
      val publisherBenefit = output.map({ case (x, y, z) => (x) }).mean();
      val advBenefit = output.map({ case (x, y, z) => (y) }).mean();
      val records = output.map({ case (x, y, z) => (z) });

      println("Avg PublisherPayOff found: " + publisherBenefit)
      println("Avg AdversaryBenefit found: " + advBenefit)
      new DataWriter(getSC()).writeRDDToAFile(outputFilePath, records.map(x => x.toSeq.sortBy(_._1).map(_._2).mkString(",")));

    }

  }

  object ListAccumulator extends AccumulatorParam[List[Long]] {

    def zero(init: List[Long]): List[Long] = {
      return init
    }

    def addInPlace(l1: List[Long], l2: List[Long]): List[Long] = {
      l1 ::: l2
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
        // println("YY=>"+y+"|");
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

    val valuesGeneralized = sc.accumulator(List(0L))(ListAccumulator);
    val neighbours = sc.longAccumulator("neighbours");

    var outputMap: List[RDD[(Double, Double, Long, String)]] = List();
    val rddsPartitioned = linesRDD.randomSplit((List.fill((linesRDD.count/10).toInt)(1.0)).toArray, 0);

    for (rddPartition <- rddsPartitioned) {
      var keysMapped = valuesGeneralized.value;
      val output = rddPartition.filter({ case (x, y) => (!keysMapped.contains(x)) }).flatMap({
        case (x, y) =>
          {
            var output: List[(Double, Double, Long, scala.collection.mutable.Map[Int, String])] = List();
            val strategy = new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam, LBSMetadata.getPopulation(), LBSMetadata.getZip()).findOptimalStrategy(y)
          
            valuesGeneralized += List(x);
            
            println("strategy:" + strategy);
            output = output :+ (strategy._1, strategy._2, x, strategy._3);
            output
          }
      });
      outputMap = outputMap :+ output.map({case(x,y,z,t)=>(x,y,z,t.toArray.sortBy(_._1).map(_._2).mkString(","))})
      
      for(singleOutput<- output.collect())
      {
        
     
            keysMapped = valuesGeneralized.value;
            val key = Vectors.dense(LSHUtil.extractRow(LBSMetadata.getInstance(), columnStartCounts.value, singleOutput._4, true));

            val neighborsRow = model.approxNearestNeighbors(txModel, key, numNeighborsVal.value).collectAsList().asInstanceOf[java.util.List[Row]]
            val nebMap = neighborsRow.toArray.asInstanceOf[Array[Row]].map({ case (row) => (row(0).asInstanceOf[Int].longValue(), (row(1).asInstanceOf[DenseVector]).values) });
            var quasiGeneralizedMap = scala.collection.mutable.Map[Int, String]();
            for (column <- metadata.getQuasiColumns()) {
              quasiGeneralizedMap.put(column.getIndex(), singleOutput._4.get(column.getIndex()).get.trim());
            }
            
            val neighbors =  nebMap.filter({ case (x, y) => (!keysMapped.contains(x)) }).map({ case (x, y) => (x, LSHUtil.extractReturnObject(metadata, columnStartCounts.value, y)) }).filter({
              case (x, y) => {
                var neighborIsSubSet = true;
                breakable {
                  for (column <- metadata.getQuasiColumns()) {

                    val genHierarchyValue = singleOutput._4.get(column.getIndex()).get.trim()
                    val neighborValue = y.get(column.getIndex()).get.trim();

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
                        if (minMax1._1 > minMax2._1 || minMax1._2 < minMax2._2) {
                          neighborIsSubSet = false;
                          break;
                        }
                      }
                    }
                  }
                }
                neighborIsSubSet;
              }
            });
            println("neighbors: ");
            outputMap = outputMap :+ sc.parallelize(neighbors).map({
              case (z, t) => {

                for (i <- metadata.getQuasiColumns()) {
                  t.remove(i.getIndex())
                }
                t ++= quasiGeneralizedMap;
                valuesGeneralized += List(z);
                neighbours.add(1);
                
            println(t.toArray.sortBy(_._1).map(_._2).mkString(","));
                (singleOutput._1,singleOutput._2, z,t.toArray.sortBy(_._1).map(_._2).mkString(","));
              }
            });
      }
          println("Number of predictions done :" + neighbours.value );

    }
    val output = sc.union(outputMap).sortBy(_._3)
    println("Number of predictions done :" + neighbours.value + " : count: " + output.count() + " Oriignal Count " + linesRDD.count());
    return (totalPublisherPayOff, totalAdvBenefit, rdds);
  }

}
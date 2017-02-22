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

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
object LBSWithoutAspect {

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("Program variables expected : <HDFS_Data_file_path> <PREDICT_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <adversaryAttackCost> <USE_LSH(true/false)> <LSH_NUM_NEIGHBORS>")
    } else {
      sc.setLogLevel("ERROR");
      setup(args(0), args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7).toBoolean, args(8).toInt)
    }
  }
  val sc = SparkSession
    .builder.appName("LBS").master("local[4]").getOrCreate().sparkContext;
  def getSC(): SparkContext =
    {
      sc
    }

  /**
   * Using following singleton to retrieve/broadcast metadata variables.
   */
  object Metadata {
    @volatile private var metadata: Broadcast[Metadata] = null;
    @volatile private var totalDataCount: Broadcast[Long] = null;
    def getInstance(sc: SparkContext): Broadcast[Metadata] = {
      if (metadata == null) {
        synchronized {
          if (metadata == null) {
            val data = Source.fromFile("/data/kanchan/metadata_exp.xml").getLines().mkString("\n");
            val metadataVal = new DataReader().readMetadata(data);
            metadata = sc.broadcast(metadataVal)
          }
        }
      }
      metadata
    }
    @volatile private var population: scala.collection.mutable.Map[(String, String, Double, Double), Double] = null;
    def getNumMatches(sc: SparkContext, key: scala.collection.mutable.Map[Int, String]): Int = {
      println("Key : " + key)
      if (population == null) {
        synchronized {
          if (population == null) {
            val data = Source.fromFile("/data/kanchan/dict.csv").getLines();
            val sortedZipList = Source.fromFile("/data/kanchan/sortedziplist").getLines().map(x => (x.split(",")(0).trim(), x.split(",")(1).trim())).toMap;
            population = scala.collection.mutable.Map[(String, String, Double, Double), Double]();
            for (line <- data) {
              val split = line.split(",");
              population.put((split(0).trim(), split(1).trim(), split(2).trim().toDouble, sortedZipList.get(split(3).trim()).get.toDouble), (split(4).trim().toDouble));
            }
          }
        }
      }
      println(population.size + " " + population.head);
      if (key != null) {
        val metadata = Metadata.getInstance(sc).value;
        var numMatches: Double = 0;
        //println(metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves + ":" +  metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves + ":" + LSHUtil.getMinMax(key.get(2).get) + ":" +  LSHUtil.getMinMax(key.get(1).get))

        for (genderStr <- metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves) //321
        {
          for (raceStr <- metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves) {
            val ageRange = LSHUtil.getMinMax(key.get(2).get);
            for (age <- ageRange._1.toInt to ageRange._2.toInt) {
              val zipRange = LSHUtil.getMinMax(key.get(1).get);
              for (zipCode <- zipRange._1.toInt to zipRange._2.toInt) {
                val gender = genderStr.replaceAll("Male", "0").replaceAll("Female", "1");
                val race = raceStr.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "1").replaceAll("Amer-Indian-Eskimo", "2").replaceAll("Other", "3").replaceAll("Black", "4");

                //   println(race + ":" + gender + ":" + age + ":" + zipCode + population.get((race, gender, age.toDouble, zipCode.toDouble)))

                if (population.get((race, gender, age.toDouble, zipCode.toDouble)) != None) {
                  //   println(gender + ":" + race + ":" + age + ":" + zipCode + population.get((race, gender, age.toDouble, zipCode.toDouble)).get)
                  numMatches += population.get((race, gender, age.toDouble, zipCode.toDouble)).get;
                }
              }
            }
          }
        }
        return numMatches.toInt;
      } else {
        return population.size;
      }
    }

    def getTotalCount(sc: SparkContext , linesRDD : RDD[(Long, scala.collection.mutable.Map[Int, String])]): Broadcast[Long] = {
      if (totalDataCount == null) {
        val totalDataCountVal = linesRDD.count();
        totalDataCount = sc.broadcast(totalDataCountVal)

      }
      return totalDataCount;
    }

    var maximumInfoLoss: Broadcast[Double] = null;

    def getMaximulInformationLoss(sc: SparkContext): Broadcast[Double] =
      {
        if (maximumInfoLoss == null) {
          val metadata = getInstance(sc);
          var maximumInfoLossValue: Double = 0.0;
          for (column <- metadata.value.getQuasiColumns()) {
            //println(i + ":" + Math.log(1.0 / metadata.value.getMetadata(i).get.depth()));
            /*val denom = Metadata.getTotalCount(sc/* , linesRDD */).value*/
            //println("NumUnique: " + column.getName() + "::" + column.getNumUnique())
            maximumInfoLossValue += (-Math.log(1.0 / column.getNumUnique()));
          }
          maximumInfoLoss = sc.broadcast(maximumInfoLossValue);
        }
        return /*maximumInfoLoss*/sc.broadcast(getNumMatches(sc,null).toDouble );
      }
  }
/*  var linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
*/
  def setup(hdfsFilePath: String, predictionFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: Boolean, numNeighbours: Int) {
   var linesRDD = new DataReader().readDataFile(getSC(), hdfsFilePath, true).cache();

    val metadata = Metadata.getInstance(getSC());
    lbs(outputFilePath , linesRDD , useLSH, lbsParam, numNeighbours);
  }
  def lbs(outputFilePath: String, linesRDD : RDD[(Long, scala.collection.mutable.Map[Int, String])], useLSH: Boolean, lbsParam: LBSParameters, numNeighbours: Int) {

    val metadata = Metadata.getInstance(getSC());
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
      /*      val cartesian = linesRDD.cartesian(linesRDD)
      println("cartesianed"+cartesian.count())
  val op=cartesian.groupByKey();
      println("grouped"+op.count())
      val op2 = op.map({case(x,y)=>{
        println("parallelizing "+x)
     (x,sc.parallelize( y.toSeq))  
      }
      }); 
      val lbsParamVal = sc.broadcast(lbsParam);
      val linesBroadCast = sc.broadcast(linesRDD.collect());
      val opts = op2.map({
        case (x,y) => {

          val optimalRecord = findOptimalStrategy(x._2, lbsParamVal.value, y);
          println((x._1) + " " + optimalRecord._1 + "_" + optimalRecord._2 + " =>" + optimalRecord._3);
          optimalRecord;
        }
      });
      println("final  " + opts.count())
      println(opts.map(_._1).mean() + " " + opts.map(_._2).mean());*/

      // for (i <- 0 to Metadata.getTotalCount(getSC()/* , linesRDD */).value.intValue() - 1) {
      val i = 29779;
      val optimalRecord = findOptimalStrategy(linesRDD.lookup(i.longValue())(0), lbsParam/* , linesRDD */)
      totalPublisherPayOff += optimalRecord._1;

      totalAdvBenefit += optimalRecord._2;
      val arr = optimalRecord._3.toArray
      println((i + 1) + " " + optimalRecord._1 + "_" + optimalRecord._2);
      println(arr.sortBy(_._1).map(_._2).mkString(","));
      list += ((i, arr.sortBy(_._1).map(_._2).mkString(",")));
      if (i % 3000 == 2999) {
        val vl = getSC().parallelize(list.toList)
        rdds = rdds :+ vl;
        list = ListBuffer();
      }
      //   }
    }
    val vl = getSC().parallelize(list.toList)
    rdds = rdds :+ vl;
    new DataWriter(getSC()).writeRDDToAFile(outputFilePath, getSC().union(rdds).sortBy(_._1).map(_._2));

    println("Avg PublisherPayOff found: " + (totalPublisherPayOff / Metadata.getTotalCount(getSC() , linesRDD ).value))
    println("Avg AdversaryBenefit found: " + (totalAdvBenefit / Metadata.getTotalCount(getSC() , linesRDD ).value))
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
    val metadata = Metadata.getInstance(getSC());
    val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata.value));

    val inputToModel = linesRDD.map({
      case (x, y) => ({
        val row = LSHUtil.extractRow(metadata.value, columnStartCounts.value, y, true)
        (x.intValue(), Vectors.dense(row))
      })
    }).collect().toSeq

    val dataFrame = new SQLContext(sc).createDataFrame(inputToModel).toDF("id", "keys");
    val key = Vectors.dense(1.0, 0.0)
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(dataFrame)
    val txModel = model.transform(dataFrame)
    txModel.show();
    /**
     * Build Neighbour Map
     */ /*
    val neighborMap = linesRDD.map({
      case (x, y) => {
        val vector =Vectors.dense(LSHUtil.extractRow(metadata.value, columnStartCounts.value, y, true));
        println("Checking: "+vector+ " -"+modelBroadCast.value)
        val neighbors = model.approxNearestNeighbors(txModel, vector, numNeighborsVal.value).collectAsList().asInstanceOf[java.util.List[Row]];
        println("neighbours found "+neighbors.size() )
        var map = scala.collection.mutable.Map[Long, Array[Double]]();
        var itr = neighbors.iterator();
        while (itr.hasNext()) {
          val row = itr.next();
          map.put(row.get(0).asInstanceOf[Int].longValue(),  (row.get(1).asInstanceOf[DenseVector]).values);
        }
        println("map ->"+map);
        (x, map);
      }
    }).cache();*/

    var keysRemaining = linesRDD.map({ case (x, y) => (x, true) }).collect().toMap;
    val linesRDDBroadcast = sc.broadcast(linesRDD.collect());
    while (keysRemaining != null && keysRemaining.size > 0) {

      // Pick up a key.
      val key = keysRemaining.last._1

      val currentRecord = linesRDD.lookup(key)(0);
      val optimalRecord = findOptimalStrategy(currentRecord, lbsParam/*, linesRDD*/)
      predicted = predicted + 1;
      totalPublisherPayOff += optimalRecord._1;
      totalAdvBenefit += optimalRecord._2;
      val arr = optimalRecord._3.toArray
      val nonQuasiMap = linesRDD.lookup(key)(0)
      var quasiGeneralizedMap = scala.collection.mutable.Map[Int, String]();
      for (column <- metadata.value.getQuasiColumns()) {
        quasiGeneralizedMap.put(column.getIndex(), optimalRecord._3.get(column.getIndex()).get.trim());
      }

      val outputMap = (nonQuasiMap ++= quasiGeneralizedMap).toArray.sortBy(_._1).map(_._2);
      list += ((key.intValue(), outputMap.mkString(",")));
      keysRemaining = keysRemaining - key;
      val neighborsRow = model.approxNearestNeighbors(txModel, Vectors.dense(LSHUtil.extractRow(metadata.value, columnStartCounts.value, currentRecord, true)), numNeighborsVal.value).collectAsList().asInstanceOf[java.util.List[Row]]
      val nebMap = sc.parallelize(neighborsRow.toArray.asInstanceOf[Array[Row]]).map({ case (row) => (row(0).asInstanceOf[Int].longValue(), (row(1).asInstanceOf[DenseVector]).values) });
      val neighbors = nebMap.filter({
        case (x, y) => {
          if (keysRemaining.contains(x)) {

            val map = LSHUtil.extractReturnObject(metadata.value, columnStartCounts.value, y)
            var neighborIsSubSet = true;
            breakable {
              for (column <- metadata.value.getQuasiColumns()) {

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
          for (i <- metadata.value.getQuasiColumns()) {
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

  def getPublishersBenefit(g: scala.collection.mutable.Map[Int, String], lbsParams: LBSParameters/* , linesRDD : RDD[(Long, scala.collection.mutable.Map[Int, String])]*/): Double =
    {

      println("Publisher Benefit" + lbsParams.getMaxPublisherBenefit() + "* ( 1.0 - " + getInformationLoss(g) + "/" + Metadata.getMaximulInformationLoss(getSC()).value + " =" + lbsParams.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g) / Metadata.getMaximulInformationLoss(getSC()).value)));
      return lbsParams.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g) / Metadata.getMaximulInformationLoss(getSC()).value));
    }

  def getInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;
      val metadata = Metadata.getInstance(getSC());
      for (column <- metadata.value.getQuasiColumns()) {
        var count: Long = 0;
        val value = g.get(column.getIndex()).get.trim()
        if (column.getColType() == 's') {
          val children = column.getCategory(value);
      /*    if (children.leaves.length != 0) {
            //   println(value + " _ " + (-Math.log(1.0 / children.leaves.length)));
            infoLoss += (-Math.log(1.0 / children.leaves.length));
          }*/
          /* if (value != children.value) {
            count = linesRDD.filter({ case (x, y) => { children.childrenString.contains(y.get(column.getIndex()).get) } }).count();
            infoLoss += (-Math.log(1.0 / count));
          }*/
        } else {
          val minMax = LSHUtil.getMinMax(value);
          /*if (minMax._1 != minMax._2) {
            // println(value + " _ " + (-Math.log(1.0 / (1 + minMax._2 - minMax._1))))
            infoLoss += (-Math.log(1.0 / (1 + minMax._2 - minMax._1)));
          }*/
          /*   if (minMax._1 != minMax._2) {
            if ((minMax._1 == column.getMin() && (minMax._2 == column.getMax()))) {

              count = Metadata.getTotalCount(getSC()/* , linesRDD */).value;
              infoLoss += (-Math.log(1.0 / count));

            } else {
              count = linesRDD.filter({ case (x, y) => { (y.get(column.getIndex()).get.toDouble >= minMax._1 && y.get(column.getIndex()).get.toDouble <= minMax._2) } }).count();
              infoLoss += (-Math.log(1.0 / count));
            }
          }*/
        }
      }
      println("Total infoLoss for " + g + " =" + infoLoss);
      return infoLoss;
    }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   * Should return a number between 0 and 1 - 1 when only single record (self) exists.
   */
  def getRiskOfStrategy(a: scala.collection.mutable.Map[Int, String], metadata: Broadcast[Metadata]/* , linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] */): Double =
    { /*
      val aVal = sc.broadcast(a);
      val matchingPopulationGroupSize = linesRDD.map({
        case (x, b) => {
          */
      /**
       * This method checks whether record B is equal to or subset of record A with respect to Quasi-Identifiers.
       */ /*
          var status = true;
          breakable {
            for (column <- metadata.value.getQuasiColumns()) {
              val value1 = aVal.value.get(column.getIndex()).get.trim()
              val value2 = b.get(column.getIndex()).get.trim()

              if (!(value1 == value2)) {
                if (column.getColType() == 's') {
                  val children = column.getCategory(value1).childrenString
                  //println("Checking " + value2 + " in ->" + value1 + "_" + children + " : " + children.contains(value2))
                  if (!children.contains(value2)) {
                    //println("returning false")
                    status = false;
                    break;
                  }
                } else {
                  val minMax1 = LSHUtil.getMinMax(value1);
                  val minMax2 = LSHUtil.getMinMax(value2);
                  //println("Checking " + value1 + " in ->" + value2)
                  if (minMax2._1 < minMax1._1 || minMax2._2 > minMax1._2) {
                    status = false;
                    break;
                  }
                }
              }
            }
          }
          if (status) { (1) } else { (0) };
        }
      });*/
      //val sum = matchingPopulationGroupSize.reduce(_ + _)
      val sum = Metadata.getNumMatches(sc, a);
      println("Risk of Strategy: " + sum + " | " + (1.0 / sum))
      return (1.0 / sum);
    }

  def findOptimalStrategy(top: scala.collection.mutable.Map[Int, String], lbsParam: LBSParameters/*/* , linesRDD */: RDD[(Long, scala.collection.mutable.Map[Int, String])]*/): (Double, Double, scala.collection.mutable.Map[Int, String]) = {
    //println("starting ::+:")

    var publisherPayOff: Double = -1;
    var adversaryBenefit: Double = -1;

    val metadata = Metadata.getInstance(getSC()).value;
    var genStrategy = top;
    while (!isGLeafNode(genStrategy)) {
      //println(":0::")
      adversaryBenefit = getRiskOfStrategy(genStrategy, Metadata.getInstance(getSC())/* , linesRDD */) * lbsParam.getPublishersLossOnIdentification(); // adversaryBenefit = publisherLoss.

      // println(":1::")
      publisherPayOff = getPublishersBenefit(genStrategy, lbsParam/* , linesRDD */) - adversaryBenefit;

      // println("::2:("+publisherPayOff+")")
      if (adversaryBenefit <= lbsParam.getRecordCost()) {
        println("::2:(" + publisherPayOff + ")")
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      //      println("Publisher Payoff " + publisherPayOff + ": " + genStrategy);
      var currentStrategy = genStrategy;
      val children = getChildren(genStrategy);

      for (child <- children) {
        //     println("children:::"+child)
        val childAdvBenefit = getRiskOfStrategy(child, Metadata.getInstance(getSC())/*/* , linesRDD */*/) * lbsParam.getPublishersLossOnIdentification();
        println("childAdvBenefit" + childAdvBenefit);
        val childPublisherPayoff = getPublishersBenefit(child, lbsParam/*/* , linesRDD */*/) - childAdvBenefit;
        println("Child payoff " + childPublisherPayoff + "->" + "|" + (childPublisherPayoff >= publisherPayOff) + "___" + child)

        if (childPublisherPayoff >= publisherPayOff) {
          //       println("Assigning values " + childPublisherPayoff + "->" + child)
          currentStrategy = child;
          adversaryBenefit = childAdvBenefit;
          publisherPayOff = childPublisherPayoff;
        }
      }
      if (currentStrategy == genStrategy) {
        //// println("Selected "+currentStrategy);
        println("Parent Payoff is better than any of the children payoff" + publisherPayOff);
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      genStrategy = currentStrategy;
    }
    println("Outside return payoff" + publisherPayOff);
    return (publisherPayOff, adversaryBenefit, genStrategy);
  }

  /**
   * This method returns true of input map corresponds to the bottommost level in lattice.
   */
  def isGLeafNode(map: scala.collection.mutable.Map[Int, String]): Boolean =
    {
      val metadata = Metadata.getInstance(getSC());
      for (column <- metadata.value.getQuasiColumns()) {
        val value = map.get(column.getIndex()).get.trim()
        if (!value.equalsIgnoreCase("*")) {
          if (column.getColType() == 's') {
            if (!value.equalsIgnoreCase(column.getRootCategory().value())) {
              return false;
            }
          } else {
            if (value.contains("_")) {
              val range = value.split("_");
              if (!(range(0).toDouble == column.getMin() && (range(1).toDouble == column.getMax()))) {
                return false;
              }
            } else {
              return false;
            }
          }
        }
      }
      return true;
    }

  /**
   * This method returns the list of immediate children from lattice for the given entry.
   */
  def getChildren(g: scala.collection.mutable.Map[Int, String]): List[scala.collection.mutable.Map[Int, String]] =
    {
      /**
       * Iterate over each attribute, generalize the value one step up at a time, accumulate and return the list.
       */
      val list = ListBuffer[scala.collection.mutable.Map[Int, String]]();
      val metadata = Metadata.getInstance(getSC());

      /**
       * Create child for lattice on each column one at a time.
       */

      for (column <- metadata.value.getQuasiColumns()) {
        var copyOfG = g.clone();
        val value = g.get(column.getIndex()).get.trim()
        val parent = column.getParentCategory(value).value();
        if (parent != value) {
          //println(value +" : "+parent) 
          copyOfG.put(column.getIndex(), column.getParentCategory(value).value().trim());
          list += copyOfG;
        }
      }
      return list.toList;
    }

}
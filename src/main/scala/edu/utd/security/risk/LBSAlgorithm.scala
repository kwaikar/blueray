package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
/**
 * This class implements algorithm from following paper
 * Title = "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
class LBSAlgorithm(metadata: Metadata, lbsParameters: LBSParameters, population: Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]], zip: List[Int], hashedPopulation: Map[(String, String,Double,Double), Int] ) extends Serializable {
  var V = lbsParameters.V();
  var L = lbsParameters.L();
  var C = lbsParameters.C();

  /**
   * Helper method which converts input row into Map of Index,Value and executes the findOriginalOptimalStrategy method.
   */
  def findOptimalStrategy(text: String): String =
    {
      val record = scala.collection.mutable.Map[Int, String]();
      val split = text.split(",")
      for (i <- 0 to split.length - 1) {
        record.put(i, split(i));
      }
      val strategy = findOptimalStrategy(record.toMap);
      println("Optimal Strategy found with Attack ==>" + strategy)
      return strategy._3;
    }

  /**
   * Helper method which executes the findOriginalOptimalStrategy method and returns the output in
   * String format.
   */
  def findOptimalStrategy(top:  Map[Int, String]): (Double, Double, String) = {
    val strategy = findOriginalOptimalStrategy(top);
    return (strategy._1, strategy._2, strategy._3.toArray.sortBy(_._1).map(_._2).mkString(","))
  }
  /**
   * This method takes the top of the lattice as the entry point and executes LAttice-Based-Search algorithm
   * until ideal generalization level is found.
   */
  def findOriginalOptimalStrategy(top: Map[Int, String]): (Double, Double, Map[Int, String]) = {

    var Um: Double = -1;
    var LPiG: Double = -1;

    var g = top;
    LPiG = L * Pi(g);
    var Gm = g;
    while (!isGLeafNode(g)) {

      if (LPiG <= C) {
        /**
         * Because of extremely low risk, adversary has no monetary incentive, hence wont attack.
         */
        return (Vg(g), 0.0, g);
      }

      Um = Vg(g) - LPiG;
      Gm = g;
      /**
       * Compute Publisher benefit for each of the child.
       */
      val maxChildren = getChildren(g).map(Gc => {
        var LPiGc = L * Pi(Gc);
        if (LPiGc > C) {
          ((Vg(Gc) - LPiGc), LPiGc, Gc);
        } else {
          (Vg(Gc), 0.0, Gc);
        }
      }).filter(_._1 >= Um);

      /**
       * Parent payoff is better than any of the child's payoff, Return parent.
       */
      if (maxChildren.isEmpty) {
        return (Um, LPiG, g);
      } else {
        /**
         * Select most optimal child, explore child's descendants.
         */
        val child = maxChildren.maxBy(_._1)
        Um = child._1
        LPiG = child._2
        Gm = child._3
      }
      g = Gm;
    }
    // println("Outside return payoff" + Um + " : " + g);
    return (Um, LPiG, g);
  }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   * Should return a number between 0 and 1 - 1 when only single record (self) exists.
   */
  def Pi(g: Map[Int, String]): Double =
    {
      val sum = getNumMatches(g);
      if (sum == 0) {
        return 1;
      } else {
        return (1.0 / sum);
      }
    }
  /**
   * This method calculates the Information loss. The information loss is calculated by individually calculating
   * loss for each of the attribute and then adding all of them
   */
  def IL(g: Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;

      for (column <- metadata.getQuasiColumns()) {
        var count: Long = 0;
        val value = g.get(column.getIndex()).get.trim()
        if (column.getColType() == 's') {
          val children = column.getCategory(value);
          if (children.leaves.length != 0) {
            infoLoss += (-Math.log10(1.0 / children.leaves.length));
          }
        } else {
          val minMax = LSHUtil.getMinMax(value);
          if (column.getName().trim().equalsIgnoreCase("age")) {
            infoLoss += (-Math.log10(1.0 / (1 + minMax._2 - minMax._1)));
          } else {
            val zipInfoLoss = zip.filter { x => x >= minMax._1 && x <= minMax._2 }.size;
            infoLoss += (-Math.log10(1.0 / (zipInfoLoss)));
          }
        }
      }
      return infoLoss;
    }
  /**
   * Calculate Publisher benefit.
   */
  def Vg(g: Map[Int, String]): Double =
    {
      val infoLoss = IL(g);
      val maxInfoLoss = metadata.getMaximulInformationLoss();
      val loss = V * (1.0 - (infoLoss / maxInfoLoss));
      return loss;
    }
  val map = scala.collection.mutable.Map[String, Int]();
  /**
   * In order to calculate risk of the record, we need to know total population size within the dataset
   * This function returns total count of entries found in the population.
   */
  def getNumMatches(key: Map[Int, String]): Int =
    {
      if (key != null) {
        var i:Int=0;
        var genders = metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves;
        if (genders == None || genders.size == 0) {
          genders = genders.+:(key.get(0).get);
          i=i+1;
        }  
        var races = metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves;
        if (races == None || races.size == 0) {
          races = races.+:(key.get(3).get);
          i=i+1;
        } 
        if(i==2 && !key.get(2).get.contains("_") && !key.get(1).get.contains("_") )
        {
          val value = hashedPopulation.get((key.get(3).get,key.get(0).get,key.get(2).get.toDouble,key.get(1).get.toDouble));
          if(value!=None)
          {
            return value.get;
          }
        }
        val ageRange = LSHUtil.getMinMax(key.get(2).get);
        val zipRange = LSHUtil.getMinMax(key.get(1).get);
        val keyForMap = races.mkString(",").trim() + "|" + genders.mkString(",").trim() + "|" + ageRange._1 + "_" + ageRange._2 + "|" + zipRange._1 + "_" + zipRange._2;
        /**
         * Once keys have been identified, check map in order to retrieve population size.
         */
        var valueFromMap = map.get(keyForMap);
        if (valueFromMap == None) {
          val numMatches = (for {
            a <- races
            b <- genders
          } yield (a, b)).map(x => {
            val populationNum = population.get(x._1, x._2);
            
            if (populationNum != None) {
              var sum = 0.0;
              var itr = populationNum.get.subMap(ageRange._1.toInt, true, ageRange._2.toInt, true).values().iterator();
              while (itr.hasNext()) {
                var itr2 = itr.next().subMap(zipRange._1.toInt, true, zipRange._2.toInt, true).values().iterator();
                while (itr2.hasNext()) {
                  sum += itr2.next();
                }
                itr2 = null;
              }
              itr = null;
              sum
            } else {
              0;
            }
          }).reduce(_ + _).toInt;
          map.put(keyForMap, numMatches);
          return numMatches;
        } else {
          return valueFromMap.get;
        }
      } else {
        return population.size;
      }
    }
  /**
   * This method returns the list of immediate children from lattice for the given entry.
   */
  def getChildren(g: Map[Int, String]): List[Map[Int, String]] =
    {
      /**
       * Iterate over each attribute, generalize the value one step up at a time, accumulate and return the list.
       */
      val list = ListBuffer[Map[Int, String]]();
      /**
       * Create child for lattice on each column one at a time.
       */
      for (column <- metadata.getQuasiColumns()) {
        val copyOfG = collection.mutable.Map[Int,String]() ++= g

        val value = g.get(column.getIndex()).get.trim()
        val parent = column.getParentCategory(value).value();
        if (parent != value) {
          copyOfG.put(column.getIndex(), column.getParentCategory(value).value().trim());
          list += copyOfG.toMap;
        }
      }
      return list.toList;
    }

  /**
   * This method returns true of input map corresponds to the bottommost level in lattice.
   */
  def isGLeafNode(map: Map[Int, String]): Boolean =
    {
      for (column <- metadata.getQuasiColumns()) {
        val value = map.get(column.getIndex()).get.trim()
        if (!value.equalsIgnoreCase("*")) {
          if (column.getColType() == 's') {
            if (!value.equalsIgnoreCase(column.getRootCategory().value())) {
              return false;
            }
          } else {
            if (!value.contains("_") || (value.contains("_") && !value.equalsIgnoreCase(column.MINMAX))) {
              return false;
            }
          }
        }
      }
      return true;
    }
}

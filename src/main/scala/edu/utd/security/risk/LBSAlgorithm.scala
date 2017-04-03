package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class LBSAlgorithm(metadata: Metadata, lbsParameters: LBSParameters) extends Serializable{
  var V = lbsParameters.V();
  var L = lbsParameters.L();
  var C = lbsParameters.C();

  def findOptimalStrategy(text: String): String =
    {
      val record = scala.collection.mutable.Map[Int, String]();
      val split = text.split(",")
      for (i <- 0 to split.length - 1) {
        record.put(i, split(i));
      }
      val strategy = findOptimalStrategy(record);
      println("Optimal Strategy found with Attack ==>" + strategy)
      return strategy._3;
    }
  
  def findOptimalStrategy(top: scala.collection.mutable.Map[Int, String]): (Double, Double, String) = {
   val strategy =findOriginalOptimalStrategy(top);
   return (strategy._1,strategy._2,strategy._3.toArray.sortBy(_._1).map(_._2).mkString(","))
  }
  def findOriginalOptimalStrategy(top: scala.collection.mutable.Map[Int, String]): (Double, Double, scala.collection.mutable.Map[Int, String]) = {

    var Um: Double = -1;
    var LPiG: Double = -1;

    var g = top;
    LPiG = L * Pi(g);
    var Gm = g;
    while (!isGLeafNode(g)) {

      if (LPiG <= C) {
       // println("adversaryBenefit<=lbsParam.getRecordCost() " + Vg(g) + "_" + LPiG)
        return (Vg(g), 0.0, g);
      }
      Um = Vg(g) - LPiG;
      Gm = g;
      val maxChildren = getChildren(g).map(Gc => {
        var LPiGc = L * Pi(Gc);
        if (LPiGc > C) {
          ((Vg(Gc) - LPiGc), LPiGc, Gc);
        } else {
          (Vg(Gc), 0.0, Gc);
        }
      }).filter(_._1 >= Um);

      if (maxChildren.isEmpty) {
     //   println("Parent Payoff is better than any of the children payoff" + Um);
        return (Um, LPiG, g);
      } else {
        val child = maxChildren.maxBy(_._1)
       // println("Selecting child " + child)
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
  def Pi(g: scala.collection.mutable.Map[Int, String]): Double =
    {

      val sum = getNumMatches(g);
      if (sum == 0) {
        return 1;
      } else {
        return (1.0 / sum);
      }
    }

    def IL(g: scala.collection.mutable.Map[Int, String]): Double =
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
            val zipInfoLoss = LBSMetadata.getZip().filter { x => x >= minMax._1 && x <= minMax._2 }.size;
            infoLoss += (-Math.log10(1.0 / (zipInfoLoss)));
          }

        }
      }
      return infoLoss;
    }
  def Vg(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      val infoLoss = IL(g);
      val maxInfoLoss = metadata.getMaximulInformationLoss();
      val loss = V * (1.0 - (infoLoss / maxInfoLoss));
      return loss;
    }
 
    def getNumMatches(key: scala.collection.mutable.Map[Int, String]): Int =
    {

      if (key != null) {

        var genders = List[String]();
        var races = List[String]();
        val gendersPar = metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves;
        if (gendersPar == None || gendersPar.size == 0) {
          genders = genders.+:(key.get(0).get.replaceAll("Male", "1").replaceAll("Female", "0"));
        } else {
          genders = gendersPar.map(_.replaceAll("Male", "1").replaceAll("Female", "0"));
        }

        val racesPar = metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves;
        if (racesPar == None || racesPar.size == 0) {
          races = races.+:(key.get(3).get.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1"));
        } else {
          races = racesPar.map(_.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1"));
        }
        val ageRange = LSHUtil.getMinMax(key.get(2).get);
        val zipRange = LSHUtil.getMinMax(key.get(1).get);
        val keyForMap = races.mkString(",").trim() + "|" + genders.mkString(",").trim() + "|" + ageRange._1 + "_" + ageRange._2 + "|" + zipRange._1 + "_" + zipRange._2;
        val numMatches = (for {
          a <- races
          b <- genders
        } yield (a, b)).map(x => {
          val populationNum = LBSMetadata.getPopulation().get(x._1, x._2);
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
        return numMatches;
      } else {
        return  LBSMetadata.getPopulation().size;
      }
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
      /**
       * Create child for lattice on each column one at a time.
       */
      for (column <- metadata.getQuasiColumns()) {
        var copyOfG = g.clone();
        val value = g.get(column.getIndex()).get.trim()
        val parent = column.getParentCategory(value).value();
        if (parent != value) {
          copyOfG.put(column.getIndex(), column.getParentCategory(value).value().trim());
          list += copyOfG;
        }
      }
      return list.toList;
    }

  /**
   * This method returns true of input map corresponds to the bottommost level in lattice.
   */
  def isGLeafNode(map: scala.collection.mutable.Map[Int, String]): Boolean =
    {
      for (column <- metadata.getQuasiColumns()) {
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
}

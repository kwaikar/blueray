package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext

class LBSAlgorithm(metadata: Metadata, lbsParameters: LBSParameters, population: scala.collection.mutable.Map[(String, String, Double, Double), Double], zipList: List[Int]) {
  var maximumInfoLoss: Double = 0;
  var V = lbsParameters.V();
  var L = lbsParameters.L();
  var C = lbsParameters.C();

  def getMaximulInformationLoss(): Double =
    {
      if (maximumInfoLoss == 0) {
        for (column <- metadata.getQuasiColumns()) {
          maximumInfoLoss += (-Math.log10(1.0 / column.getNumUnique()));
        }

      }
      return maximumInfoLoss;
    }
  def findOptimalStrategy(text: String): String =
    {
      val record = scala.collection.mutable.Map[Int, String]();
      val split = text.split(",")
      for (i <- 0 to split.length - 1) {
        record.put(i, split(i));
      }
      val strategy = findOptimalStrategy(record);
      println("Optimal Strategy found with Attack ==>" + strategy)
      return strategy._3.toArray.sortBy(_._1).map(_._2).mkString(",");
    }
  def findOptimalStrategy(top: scala.collection.mutable.Map[Int, String]): (Double, Double, scala.collection.mutable.Map[Int, String]) = {

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
        println("Parent Payoff is better than any of the children payoff" + Um);
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
            val zipInfoLoss = zipList.filter { x => x >= minMax._1 && x <= minMax._2 }.size;
            infoLoss += (-Math.log10(1.0 / (zipInfoLoss)));
          }

        }
      }
      return infoLoss;
    }

  def Vg(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      val infoLoss = IL(g);
      val maxInfoLoss = getMaximulInformationLoss();
      val loss = V * (1.0 - (infoLoss / maxInfoLoss));
      return loss;
    }
  def getNumMatches(key: scala.collection.mutable.Map[Int, String]): Int =
    {

      if (key != null) {

        val genders = metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves.+:(key.get(0).get).map(_.replaceAll("Male", "1").replaceAll("Female", "0"));

        val races = metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves.+:(key.get(3).get).map(_.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1"));
        val ageRange = LSHUtil.getMinMax(key.get(2).get);
        val age = (ageRange._1.toInt to ageRange._2.toInt)
        val zipRange = LSHUtil.getMinMax(key.get(1).get);
        val zip = (zipRange._1.toInt to zipRange._2.toInt)

        val combinations =  for {
          a <- races
          b <- genders
          c <- age
          d <- zip
        } yield (a, b, c, d);

        var numMatches = combinations.map(x => {
          val populationNum = population.get(x._1, x._2, x._3, x._4);
          if (populationNum != None) {
            populationNum.get;
          } else {
            0;
          }
        }).reduce(_ + _).toInt;
        return numMatches;
      } else {
        return population.size;
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

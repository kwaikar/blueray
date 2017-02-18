package edu.utd.security.risk

import scala.collection.mutable.ListBuffer

class LBSAlgorithm(metadata: Metadata, lbsParam: LBSParameters, population: scala.collection.mutable.Map[(String, String, Double, Double), Double]) {
  var maximumInfoLoss: Double = 0;

  def getMaximulInformationLoss(): Double =
    {
      if (maximumInfoLoss == 0) {
        for (column <- metadata.getQuasiColumns()) {
          maximumInfoLoss += (-Math.log(1.0 / column.getNumUnique()));
        }
      }
      return maximumInfoLoss /* getNumMatches(null).toDoubl*/ ;
    }
  def findOptimalStrategy(text: String): String =
    {
      val record = scala.collection.mutable.Map[Int, String]();
      val split = text.split(",")
      for (i <- 0 to split.length - 1) {
        record.put(i, split(i));
      }
      val strategy = findOptimalStrategy(record);
      println("==>" + strategy)
      return strategy._3.toArray.sortBy(_._1).map(_._2).mkString(",");
    }
  def findOptimalStrategy(record: scala.collection.mutable.Map[Int, String]): (Double, Double, scala.collection.mutable.Map[Int, String]) = {
    //println("starting ::+:")

    var publisherPayOff: Double = -1;
    var adversaryBenefit: Double = -1;

    var genStrategy = record;
    while (!isGLeafNode(genStrategy)) {
      //println(":0::")
      adversaryBenefit = getRiskOfStrategy(genStrategy) * lbsParam.getPublishersLossOnIdentification(); // adversaryBenefit = publisherLoss.

      // println(":1::")
      publisherPayOff = getPublishersBenefit(genStrategy) - adversaryBenefit;

      //println("::2:("+publisherPayOff+")"+adversaryBenefit)
      if (adversaryBenefit <= lbsParam.getRecordCost()) {
        //println("::2:(" + publisherPayOff + ")")
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      //      println("Publisher Payoff " + publisherPayOff + ": " + genStrategy);
      var currentStrategy = genStrategy;
      val children = getChildren(genStrategy);

      for (child <- children) {
             //println("-----------------------------------------")
        val childAdvBenefit = getRiskOfStrategy(child) * lbsParam.getPublishersLossOnIdentification();
        //println("childAdvBenefit" + childAdvBenefit);
        val childPublisherPayoff = getPublishersBenefit(child) - childAdvBenefit;
        //println("Child payoff " + childPublisherPayoff + "->" + "|" + (childPublisherPayoff >= publisherPayOff) + "___" + child)

        if (childPublisherPayoff >= publisherPayOff) {
          //       println("Assigning values " + childPublisherPayoff + "->" + child)
          currentStrategy = child;
          adversaryBenefit = childAdvBenefit;
          publisherPayOff = childPublisherPayoff;
        }
      }
      if (currentStrategy == genStrategy) {
        //// println("Selected "+currentStrategy);
        //println("Parent Payoff is better than any of the children payoff" + publisherPayOff);
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      genStrategy = currentStrategy;
    }
    //println("Outside return payoff" + publisherPayOff);
    return (publisherPayOff, adversaryBenefit, genStrategy);
  }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   * Should return a number between 0 and 1 - 1 when only single record (self) exists.
   */
  def getRiskOfStrategy(a: scala.collection.mutable.Map[Int, String]): Double =
    {

      val sum = getNumMatches(a);
      //println("Risk of Strategy: " + sum + " | " + (1.0 / sum))
      if (sum == 0) {
        return 1;
      } else {
        return (1.0 / sum);
      }
    }

  def getInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;
      for (column <- metadata.getQuasiColumns()) {
        var count: Long = 0;
        val value = g.get(column.getIndex()).get.trim()
        if (column.getColType() == 's') {
          val children = column.getCategory(value);
          if (children.leaves.length != 0) {
            //   println(value + " _ " + (-Math.log(1.0 / children.leaves.length)));
            infoLoss += (-Math.log(1.0 / children.leaves.length));
          }
          else
          {
            infoLoss += (-Math.log(1.0 ));
          }
        } else {
          val minMax = LSHUtil.getMinMax(value);
          if (minMax._1 != minMax._2) {
            // println(value + " _ " + (-Math.log(1.0 / (1 + minMax._2 - minMax._1))))
            infoLoss += (-Math.log(1.0 / (1 + minMax._2 - minMax._1)));
          }
          else
          {
            infoLoss += (-Math.log(1.0 ));
          }
        }
      }
      //println("Total infoLoss for " + g + " =" + infoLoss);
      return infoLoss;
    }

  def getPublishersBenefit(g: scala.collection.mutable.Map[Int, String]): Double =
    {
     // println("Publisher Benefit" + lbsParam.getMaxPublisherBenefit() + "* ( 1.0 - " + getInformationLoss(g) + "/" + getMaximulInformationLoss() + " =" + lbsParam.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g) / getMaximulInformationLoss())));
      return lbsParam.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g) / getMaximulInformationLoss()));
    }
  def getNumMatches(key: scala.collection.mutable.Map[Int, String]): Int =
    {
    println(key)
      val genders = metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves;
      val races = metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves;
      if (key != null) {
        var numMatches: Double = 0;
        for (genderStr <- genders.+:(key.get(0).get)) //321
        {
          for (raceStr <- races.+:(key.get(3).get)) {
            
            val ageRange = LSHUtil.getMinMax(key.get(2).get);
            for (age <- ageRange._1.toInt to ageRange._2.toInt) {
              
              val zipRange = LSHUtil.getMinMax(key.get(1).get);
              for (zipCode <- zipRange._1.toInt to zipRange._2.toInt) {
                
                val gender = genderStr.replaceAll("Male", "0").replaceAll("Female", "1");
                val race = raceStr.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1");
                   println(race + " :" + gender + " +" + age + "== " + zipCode + " ==>" + population.get((race, gender, age.toDouble, zipCode.toDouble)));

                if (population.get((race, gender, age.toDouble, zipCode.toDouble)) != None) {
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

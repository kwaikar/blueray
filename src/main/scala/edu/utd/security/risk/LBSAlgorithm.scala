package edu.utd.security.risk

import scala.collection.mutable.ListBuffer

class LBSAlgorithm(metadata: Metadata, lbsParameters: LBSParameters, population: scala.collection.mutable.Map[(String, String, Double, Double), Double],zipList:List[Int]) {
  var maximumInfoLoss: Double = 0;
var V = lbsParameters.V();
var L = lbsParameters.L();
var C = lbsParameters.C();

  def getMaximulInformationLoss(): Double =
    {
      if (maximumInfoLoss == 0) {
        println("Calculating Maximum Information Loss")
        for (column <- metadata.getQuasiColumns()) {
          println("Info Loss for column "+column.getName() + ":[" + column.getNumUnique() + " unique values ](-Math.log10(1.0 /"+column.getNumUnique()+") = " + (-Math.log10(1.0 / column.getNumUnique())));
          maximumInfoLoss += (-Math.log10(1.0 / column.getNumUnique()));
        }
       println("Setting total maximumInformationLoss : " + maximumInfoLoss)

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
    while (!isGLeafNode(g)) {

      LPiG = L* Pi(g) ; 
      Um = Vg(g) - LPiG;
      println(g+ " "+Um);
      
      if (LPiG <= C) {
        println("adversaryBenefit<=lbsParam.getRecordCost() " + Vg(g) + "_" + LPiG)
        return (Vg(g), LPiG, g);
      }

      var Gm = g;

      for (Gc <-  getChildren(g)) {
        
        val LPiGc =  L*Pi(Gc) ;
        var childPublisherPayoff=0.0;
        if(LPiGc>C)
        { 
          childPublisherPayoff = Vg(Gc) - LPiGc;
        }
        else
        {
           childPublisherPayoff = Vg(Gc)
        } 
         println(Gc+ " "+childPublisherPayoff);
        
      //  println(Gc + ": " + childPublisherPayoff + "_" + LPiGc + ": (childPublisherPayoff >= publisherPayOff)=" + (childPublisherPayoff >= Um))

        if (childPublisherPayoff >= Um) {
          println("Selecting child : " + Gc)
          Gm = Gc;
          LPiG = LPiGc;
          Um = childPublisherPayoff;
        }
      }
      if (Gm == g) {
        println("Parent Payoff is better than any of the children payoff" + Um);
        return (Um, LPiG, g);
      }
      g = Gm;
    }
    println("Outside return payoff" + Um + " : "+g);
    return (Um, LPiG, g);
  }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   * Should return a number between 0 and 1 - 1 when only single record (self) exists.
   */
  def Pi(g: scala.collection.mutable.Map[Int, String]): Double =
    {

      val sum = getNumMatches(g);
     println("Strategy: " + g + " num_entries: " + sum + " Risk=> " + (1.0 / sum))
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
            println("("+column.getName()+") IL+= "+ (-Math.log10(1.0 / children.leaves.length)));
            infoLoss += (-Math.log10(1.0 / children.leaves.length));
          }
        } else {
          val minMax = LSHUtil.getMinMax(value);
          if(column.getName().trim().equalsIgnoreCase("age"))
          {
            println("("+column.getName()+") IL+="+(-Math.log10(1.0 / (1 + minMax._2 - minMax._1))));
             infoLoss += (-Math.log10(1.0 / (1 + minMax._2 - minMax._1)));
          }
          else
          {   
             println("("+column.getName()+") IL+=:"+zipList.filter { x => x>= minMax._1 && x<=minMax._2}+ " ===>"+zipList.filter { x => x>= minMax._1 && x<=minMax._2}.size)
            infoLoss += (-Math.log10(1.0 / (1 + minMax._2 - minMax._1)));
          }
          
        }
      }
      println("Total infoLoss for " + g + " =" + infoLoss);
      return infoLoss;
    }

  def Vg(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      val infoLoss = IL(g);
      val maxInfoLoss = getMaximulInformationLoss();
      val loss = V * (1.0 - (infoLoss / maxInfoLoss));
       println("Publisher Benefit" + V + "* ( 1.0 - " + infoLoss + "/" + maxInfoLoss + " =" + loss);
      return loss;
    }
  def getNumMatches(key: scala.collection.mutable.Map[Int, String]): Int =
    {

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

                val gender = genderStr.replaceAll("Male", "1").replaceAll("Female", "0");
                val race = raceStr.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1");

                if (population.get((race, gender, age.toDouble, zipCode.toDouble)) != None) {
              //   println("(" + race + ", " + gender + ", " + age + " " + zipCode + ") :" + population.get((race, gender, age.toDouble, zipCode.toDouble)));
                 numMatches += population.get((race, gender, age.toDouble, zipCode.toDouble)).get;
                }
              }
            }
          }
        }
       println(key + " Matches : " + numMatches)
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

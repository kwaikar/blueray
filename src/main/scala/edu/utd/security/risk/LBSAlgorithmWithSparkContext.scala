package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import java.util.Collection
import java.util.Arrays

class LBSAlgorithmWithSparkContext(zips:Broadcast[List[Int]],population : Broadcast[scala.collection.mutable.Map[(String, String, Double), java.util.TreeMap[Double, Double]]],metadata: Metadata, lbsParameters: LBSParameters) extends LBSAlgorithm(metadata: Metadata, lbsParameters: LBSParameters) {

   
override  def IL(g: scala.collection.mutable.Map[Int, String]): Double =
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
            val zipInfoLoss =zips.value.filter { x => x >= minMax._1 && x <= minMax._2 }.size;
            infoLoss += (-Math.log10(1.0 / (zipInfoLoss)));
          }

        }
      }
      return infoLoss;
    }
 
  override def getNumMatches(key: scala.collection.mutable.Map[Int, String]): Int =
    {

      if (key != null) {

        val genders = metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves.+:(key.get(0).get).map(_.replaceAll("Male", "1").replaceAll("Female", "0"));

        val races = metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves.+:(key.get(3).get).map(_.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1"));
        val ageRange = LSHUtil.getMinMax(key.get(2).get);
        val age = (ageRange._1.toInt to ageRange._2.toInt)
        val zipRange = LSHUtil.getMinMax(key.get(1).get);
        val zip = (zipRange._1.toInt to zipRange._2.toInt) 

     /* val numMatches=   population.value.filterKeys(x=>(zipRange._1<=x._4 && zipRange._2>=x._4  && ageRange._1<=x._3 && ageRange._2>=x._3 && races.contains(x._1) && genders.contains(x._2))).values.sum.toInt;*/
        val combinations = ( for {
          a <- races
          b <- genders
          c <- age
        } yield (a, b, c))
        
        
    val numMatches=  combinations.map(x => {
          val populationNum = population.value.get(x._1, x._2, x._3) ;
          if (populationNum != None) {
            var sum=0.0;
            val list =populationNum.get.subMap(zipRange._1.toInt, zipRange._2.toInt+1).values();
            var itr =list.iterator();
            while(itr.hasNext())
            {
              sum+=itr.next();
            }
            sum
          } else {
            0;
          }
        }).reduce(_ + _).toInt;
        return numMatches;
      } else {
        return  population.value.size;
      }
    }
 
}

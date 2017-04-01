package edu.utd.security.risk

import java.util.Collection

import org.apache.spark.broadcast.Broadcast

class LBSAlgorithmWithSparkContext(zips: Broadcast[List[Int]], population: Broadcast[scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]]], metadata: Broadcast[Metadata], lbsParameters: LBSParameters) extends LBSAlgorithm(metadata.value: Metadata, lbsParameters: LBSParameters) {

  override def IL(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;

      for (column <- metadata.value.getQuasiColumns()) {
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
            val zipInfoLoss = zips.value.filter { x => x >= minMax._1 && x <= minMax._2 }.size;
            infoLoss += (-Math.log10(1.0 / (zipInfoLoss)));
          }

        }
      }
      return infoLoss;
    }

  override def getNumMatches(key: scala.collection.mutable.Map[Int, String]): Int =
    {

      if (key != null) {

        var genders = List[String]();
        var races = List[String]();
        val gendersPar = metadata.value.getMetadata(0).get.getCategory(key.get(0).get).leaves;
        if (gendersPar == None || gendersPar.size == 0) {
          genders = genders.+:(key.get(0).get.replaceAll("Male", "1").replaceAll("Female", "0"));
        } else {
          genders = gendersPar.map(_.replaceAll("Male", "1").replaceAll("Female", "0"));
        }

        val racesPar = metadata.value.getMetadata(3).get.getCategory(key.get(3).get).leaves;
        if (racesPar == None || racesPar.size == 0) {
          races = races.+:(key.get(3).get.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1"));
        } else {
          races = racesPar.map(_.replaceAll("White", "0").replaceAll("Asian-Pac-Islander", "2").replaceAll("Amer-Indian-Eskimo", "3").replaceAll("Other", "4").replaceAll("Black", "1"));
        }
        val ageRange = LSHUtil.getMinMax(key.get(2).get);
        val zipRange = LSHUtil.getMinMax(key.get(1).get);
        /*      val url = "http://cloudmaster3:8084/bluerayWebapp/getNumPopulation?races=" + races.mkString(",").trim() + "&genders=" + genders.mkString(",").trim() + "&ages=" + ageRange._1 + "_" + ageRange._2 + "&zips=" + zipRange._1 + "_" + zipRange._2;
        if (url.contains("races=&genders")) {

          println(key.get(0).get + " " + metadata.getMetadata(0).get.getCategory(key.get(0).get) + " | " + metadata.getMetadata(0).get.getCategory(key.get(0).get).leaves+ " ==?"+genders);

          println(key.get(3).get + " " + metadata.getMetadata(3).get.getCategory(key.get(3).get).leaves+"===>"+races);

          println(url);
        }
        val output = Source.fromURL(url).mkString;
        if (output.trim().toInt < 0) {
          println("ERROR---------------- " + url + " " + output);
        }*/
        /* return output.trim().toInt;*/

        val keyForMap = races.mkString(",").trim() + "|" + genders.mkString(",").trim() + "|" + ageRange._1 + "_" + ageRange._2 + "|" + zipRange._1 + "_" + zipRange._2;
        var cnt =LBSMetadataWithSparkContext.numMatchesMap.get(keyForMap);
        if(cnt!=None)
        {
          return cnt.get;
        }
        val numMatches = (for {
          a <- races
          b <- genders
        } yield (a, b)).map(x => {
          val populationNum = population.value.get(x._1, x._2);
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
        LBSMetadataWithSparkContext.numMatchesMap.put(keyForMap, numMatches);
        return numMatches;
      } else {
        return population.value.size;
      }
    }

}

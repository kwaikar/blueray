package edu.utd.security.risk

import scala.io.Source
import java.util.TreeMap

object LBSMetadata {
  @volatile private var metadata: Metadata = null;
  @volatile private var totalDataCount: Long = 0L;
  def getInstance(): Metadata = {
    if (metadata == null) {
      synchronized {
        if (metadata == null) {
          val data = Source.fromFile("/data/kanchan/metadata_exp.xml").getLines().mkString("\n");
          metadata = new DataReader().readMetadata(data);
        }
      }
    }
    metadata
  }
  @volatile private var population: Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]] = null;
  @volatile private var hashedPopulation: Map[(String, String, Double, Double), Int] = null;

  @volatile private var zipList: List[Int] = null;
  def getPopulation(): Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]] = {
    if (population == null) {
      synchronized {
        if (population == null) {
          val data = Source.fromFile("/data/kanchan/dict.csv").getLines();
          val sortedZipList = Source.fromFile("/data/kanchan/sortedziplist").getLines().map(x => (x.split(",")(0).trim(), x.split(",")(1).trim())).toMap;
          zipList = sortedZipList.values.map(x => x.trim().toInt).toList;
          val pop = scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]]();
          val hashedPop = scala.collection.mutable.Map[(String, String, Double, Double), Int]();

          for (line <- data) {
            val split = line.split(",");
            val race = split(0).trim().replaceAll("0", "White").replaceAll("2", "Asian-Pac-Islander").replaceAll("3", "Amer-Indian-Eskimo").replaceAll("4", "Other").replaceAll("1", "Black");
            val gender = split(1).trim().replaceAll("1", "Male").replaceAll("0", "Female");
            val age = split(2).trim().toDouble;
            val zip = sortedZipList.get(split(3).trim()).get.toDouble;
            val population = split(4).trim().toDouble.intValue();
            hashedPop.put((race, gender, age, zip), population);
            var mapOfKeys = pop.get((race, gender));
            if (mapOfKeys == None) {
              mapOfKeys = Some(new java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]());
            }

            var mapOfAges: TreeMap[Double, Int] = mapOfKeys.get.get(age);
            if (mapOfAges == null) {
              mapOfAges = new java.util.TreeMap[Double, Int]();
            }
            mapOfAges.put(zip, population);

            mapOfKeys.get.put(age, mapOfAges);
            pop.put((race, gender), mapOfKeys.get);
          }
          if (population == null) {
            population = pop.toMap;
            hashedPopulation = hashedPop.toMap;
          }
        }
      }
    }

    return population;
  }
  def getHashedPopulation(): Map[(String, String, Double, Double), Int] =
    {
      getPopulation();
      return hashedPopulation;
    }
  def getZip(): List[Int] =
    {
      getPopulation();
      return zipList;
    }

}
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
  @volatile private var population: scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]] = null;
  @volatile private var zipList: List[Int] = null;
  def getPopulation(): scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]] = {
    if (population == null) {
      synchronized {
        if (population == null) {
          val data = Source.fromFile("/data/kanchan/dict.csv").getLines();
          val sortedZipList = Source.fromFile("/data/kanchan/sortedziplist").getLines().map(x => (x.split(",")(0).trim(), x.split(",")(1).trim())).toMap;
          zipList = sortedZipList.values.map(x => x.trim().toInt).toList;
          val pop = scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]]();
          for (line <- data) {
            val split = line.split(",");
            var mapOfKeys = pop.get((split(0).trim(), split(1).trim()));
            if (mapOfKeys == None) {
              mapOfKeys = Some(new java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]());
            }

            var mapOfAges: TreeMap[Double, Double] = mapOfKeys.get.get(split(2).trim().toDouble);
            if (mapOfAges == null) {
              mapOfAges = new java.util.TreeMap[Double, Double]();
            }
            mapOfAges.put(sortedZipList.get(split(3).trim()).get.toDouble, split(4).trim().toDouble);

            mapOfKeys.get.put(split(2).trim().toDouble, mapOfAges);
            pop.put((split(0).trim(), split(1).trim()), mapOfKeys.get);
          }
          if (population == null) {
            population = pop;
          }
        }

        return population;
      }
    } else {

      return population;
    }
  }
  def getZip(): List[Int] =
    {
      getPopulation();
      return zipList;
    }

}
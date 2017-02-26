package edu.utd.security.risk

import scala.io.Source

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
    @volatile private var population: scala.collection.mutable.Map[(String, String, Double, Double), Double] = null;
    @volatile private var zipList: List[Int] = null;
    def getPopulation(): scala.collection.mutable.Map[(String, String, Double, Double), Double] = {
      if (population == null) {
        synchronized {
          if (population == null) {
            val data = Source.fromFile("/data/kanchan/dict.csv").getLines();
            val sortedZipList = Source.fromFile("/data/kanchan/sortedziplist").getLines().map(x => (x.split(",")(0).trim(), x.split(",")(1).trim())).toMap;
            zipList = sortedZipList.values.map(x=>x.trim().toInt).toList;
            population = scala.collection.mutable.Map[(String, String, Double, Double), Double]();
            for (line <- data) {
              val split = line.split(",");
              population.put((split(0).trim(), split(1).trim(), split(2).trim().toDouble, sortedZipList.get(split(3).trim()).get.toDouble), (split(4).trim().toDouble));
            }
          }
        }
      }
      println(population.size + " " + population.head);
       
      return population;
    }
    def getZip():List[Int]=
    {
      getPopulation();
      return zipList;
    }

}
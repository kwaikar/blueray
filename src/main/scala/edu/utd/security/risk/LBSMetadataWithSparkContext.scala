package edu.utd.security.risk

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import java.util.TreeMap

object LBSMetadataWithSparkContext 
{
  
    var numMatchesMap: scala.collection.mutable.Map[String, Integer] = scala.collection.mutable.Map[String, Integer]();

    @volatile private var metadata: Broadcast[Metadata] = null;
    @volatile private var totalDataCount: Long = 0L;
    def getInstance(sc:SparkContext): Broadcast[Metadata] = {
      if (metadata == null) {
        synchronized {
          if (metadata == null) {
            val data = Source.fromFile("/data/kanchan/metadata_exp.xml").getLines().mkString("\n");
            metadata = sc.broadcast( new DataReader().readMetadata(data));
          }
        }
      }
      metadata
    }
    @volatile private var population: Broadcast[scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]]] = null;
    @volatile private var zipList: Broadcast[List[Int]] = null;
    def getPopulation(sc:SparkContext): Broadcast[scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]]] = {
      if (population == null) {
        synchronized {
          if (population == null) {
            val data = Source.fromFile("/data/kanchan/dict.csv").getLines();
            val sortedZipList = Source.fromFile("/data/kanchan/sortedziplist").getLines().map(x => (x.split(",")(0).trim(), x.split(",")(1).trim())).toMap;
            zipList = sc.broadcast(sortedZipList.values.map(x=>x.trim().toInt).toList);
            val pop =scala.collection.mutable.Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]]();
            for (line <- data) {
              val split = line.split(",");
              var mapOfKeys =pop.get((split(0).trim(), split(1).trim()));
              if(mapOfKeys==None)
              {
                mapOfKeys = Some( new java.util.TreeMap[Double, java.util.TreeMap[Double, Double]]());
              }

              var mapOfAges : TreeMap[Double, Double] =mapOfKeys.get.get(split(2).trim().toDouble);
              if(mapOfAges==null)
              {
                mapOfAges =new java.util.TreeMap[Double, Double](); 
              }
              mapOfAges.put(sortedZipList.get(split(3).trim()).get.toDouble,split(4).trim().toDouble);
              
              mapOfKeys.get.put(split(2).trim().toDouble, mapOfAges);
              pop.put((split(0).trim(), split(1).trim()),mapOfKeys.get);
            }
            population = sc.broadcast(pop);
          }
        }
      }
       
      return population;
    }
    def getZip(sc:SparkContext):Broadcast[List[Int]]=
    {
      getPopulation(sc);
      return zipList;
    }

}
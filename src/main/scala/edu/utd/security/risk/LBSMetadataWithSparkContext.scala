package edu.utd.security.risk

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import java.util.TreeMap
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.Accumulator
import org.apache.spark.ml.linalg.Vectors

object LBSMetadataWithSparkContext {

  var numMatchesMap: scala.collection.immutable.Map[String, Integer] = scala.collection.immutable.Map[String, Integer]();

  @volatile private var metadata: Broadcast[Metadata] = null;
  @volatile private var totalDataCount: Long = 0L;
  def getInstance(sc: SparkContext): Broadcast[Metadata] = {
    if (metadata == null) {
      synchronized {
        if (metadata == null) {
          val data = Source.fromFile("/data/kanchan/metadata_exp.xml").getLines().mkString("\n");
          metadata = sc.broadcast(new DataReader().readMetadata(data));
        }
      }
    }
    metadata
  }
  @volatile private var population: Broadcast[Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]]] = null;
  @volatile private var hashedPopulation: Broadcast[ Map[(String, String, Double, Double), Int]] = null;

  @volatile private var zipList: Broadcast[List[Int]] = null;
  def getPopulation(sc: SparkContext): Broadcast[Map[(String, String), java.util.TreeMap[Double, java.util.TreeMap[Double, Int]]]] = {
    if (population == null) {
      synchronized {
        if (population == null) {
          val data = Source.fromFile("/data/kanchan/dict.csv").getLines();
          val sortedZipList = Source.fromFile("/data/kanchan/sortedziplist").getLines().map(x => (x.split(",")(0).trim(), x.split(",")(1).trim())).toMap;
          zipList = sc.broadcast(sortedZipList.values.map(x => x.trim().toInt).toList);
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
            population = sc.broadcast(pop.toMap);
            hashedPopulation = sc.broadcast(hashedPop.toMap);
          }
        }
      }
    }

    return population;
  }
  def getHashedPopulation(sc: SparkContext): Broadcast[Map[(String, String, Double, Double), Int]] =
    {
      getPopulation(sc);
      return hashedPopulation;
    }
  def getZip(sc: SparkContext): Broadcast[List[Int]] =
    {
      getPopulation(sc);
      return zipList;
    }
}
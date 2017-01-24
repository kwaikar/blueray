package edu.utd.security.mondrian

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import edu.utd.security.blueray.Util
import edu.utd.security.common.Metadata
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq
import scala.xml.XML

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
object LBS {
 
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR");
    lbs(args(0), args(1), args(2), args(3).toInt);
  }
   

  var metadataFilePath: String = null;
  var dataReader: DataReader = null;
  var sc: SparkContext = SparkSession
    .builder.appName("LBS").master("local[2]").getOrCreate().sparkContext;

  /**
   * Using following singleton to retrieve/broadcast metadata variables.
   */
  object Metadata {
    @volatile private var metadata: Broadcast[Metadata] = null;
    def getInstance(sc: SparkContext, dataReader: DataReader, filePath: String): Broadcast[Metadata] = {
      if (metadata == null) {
        synchronized {
          if (metadata == null) {

            val metadataVal = dataReader.readMetadata(filePath);
            metadata = sc.broadcast(metadataVal)
          }
        }
      }
      metadata
    }
  }

  def lbs(hdfsDataFilePath: String, metadataFilePath: String, outputFilePath: String, k: Int) {

    val dataReader = new DataReader(sc);

    val linesRDD = dataReader.readDataFile(hdfsDataFilePath, true);
    linesRDD.cache();
    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    val record = linesRDD.first();
   val optimalRecord= findOptimalStrategy(record)
   
    writeOutputToFile(rdds, outputFilePath);
    sc.stop();
  }
  

  def findOptimalStrategy(lineRDD: (Long, Map[Int, String])):(Long, Map[Int, String])= {

    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    var output = lineRdd;
  if()
  {
    
  }
  return output;
  }
  }
  
}
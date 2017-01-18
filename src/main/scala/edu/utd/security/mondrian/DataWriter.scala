package edu.utd.security.mondrian

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * This class is responsible for writing data .
 */
class DataWriter (sc: SparkContext) {
 
  def writeRDDToAFile(filePath:String,rdd:RDD[String])
  {
     /**
     * Use coalese to prevent output being written to multiple partition files.
     */
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(filePath), sc.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(filePath))) {
      fs.delete(new org.apache.hadoop.fs.Path(filePath));
    }

    rdd.coalesce(1, true).saveAsTextFile(filePath)
    println("Output written to file: " + filePath);
  }
}
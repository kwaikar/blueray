package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object InfoLossCalculator {

  
  def main(args: Array[String]): Unit = {

  var sc: SparkContext = SparkSession
    .builder.appName("Mondrian").master(args(0)).getOrCreate().sparkContext;
    if (args.length < 3) {
      println("Program variables expected :<SPARK_MASTER_URL> <HDFS_Data_file_path> <PUBLISHER_BENEFIT>")
    } else {
      sc.setLogLevel("ERROR");
      val linesRDD = new DataReader().readDataFile(sc, args(1), 40).cache();
      val totalIL = linesRDD.map(_._2).map(x=>IL(x)).mean();
      println("Total IL "+100*(totalIL/getMaximulInformationLoss())+ " Benefit with no attack: "+ args(2).toInt*(1-(totalIL/getMaximulInformationLoss())));
    }
  }

   def getMaximulInformationLoss(): Double =
    {
     var maximumInfoLoss = 0.0;
      val metadata = LBSMetadata.getInstance();
        for (column <- metadata.getQuasiColumns()) {
          maximumInfoLoss += (-Math.log10(1.0 / column.getNumUnique()));
        }
      return maximumInfoLoss;
    }
  def IL(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;
      val metadata = LBSMetadata.getInstance();
      val zipList = LBSMetadata.getZip();
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
            val infoLossForZip = zipList.filter { x => x >= minMax._1 && x <= minMax._2 }.size
            infoLoss += (-Math.log10(1.0 / (infoLossForZip)));
          }

        }
      }
     // println("Total infoLoss for " + g + " =" + infoLoss);
      return infoLoss;
    }

}

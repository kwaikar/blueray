package edu.utd.security.risk

import scala.annotation.varargs
import scala.reflect.runtime.universe

import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object LSH {

  val sc = SparkSession.builder.appName("LBS").master("local[4]").getOrCreate().sparkContext;

  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc);
    lsh(sqlContext, args(0));
  }
  def lsh(sqlContext: SQLContext, hdfsFilePath: String) {
    val linesRDD = new DataReader(sc).readDataFile(hdfsFilePath, true).cache();
    val metadata = LBS.Metadata.getInstance(sc);
    val rows = LBSUtil.getMinimalDataSet(metadata.value, linesRDD);
    val op = rows.map({
      case (x, y) => ({
        val columnStartCounts = LBSUtil.getColumnStartCounts(metadata.value);
        println("y->" + y)
        val row = LBSUtil.extractRow(metadata.value, columnStartCounts, y, true)
        (x.intValue(), Vectors.dense(row))
      })
    })
    val inputToModel = op.collect().toSeq;
    val dataFrame = sqlContext.createDataFrame(inputToModel).toDF("id", "keys");

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(dataFrame)
    val txModel = model.transform(dataFrame)

    val columnStartCounts = LBSUtil.getColumnStartCounts(metadata.value);
    val neighbors = model.approxNearestNeighbors(txModel, Vectors.dense(LBSUtil.extractRow(metadata.value, columnStartCounts, (rows.take(7)(6))._2, true)), 40).collectAsList().asInstanceOf[java.util.List[Row]];
    println();
    println("Neighbors for following entry are:");
    for (i <- 0 to neighbors.size() - 1) {
      val neighbor = neighbors.get(i);
      val output = LBSUtil.extractReturnObject(metadata.value, columnStartCounts, (neighbor.get(1).asInstanceOf[DenseVector]).values);
      println(neighbor.get(0)+" ->" +output)
    }

  }

}
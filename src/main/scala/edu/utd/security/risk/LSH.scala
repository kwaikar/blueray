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
    sc.setLogLevel("ERROR")
    lsh(sqlContext, args(0));
  }
  def lsh(sqlContext: SQLContext, hdfsFilePath: String) {
    val linesRDD = new DataReader().readDataFile(sc,hdfsFilePath, true).cache();
    val metadata = LBSWithoutAspect.Metadata.getInstance(sc);
    val rows = LSHUtil.getMinimalDataSet(metadata.value, linesRDD, false);

    val linesZipped = linesRDD.map((_._2)).zipWithIndex().map { case (map, index) => (index, map) }.cache();

    val op = rows.map({
      case (x, y) => ({
        val columnStartCounts = LSHUtil.getColumnStartCounts(metadata.value);
        val row = LSHUtil.extractRow(metadata.value, columnStartCounts, y, true)
        //println(x.intValue()+"-->"+ Vectors.dense(row)+"=="+LSHUtil.extractReturnObject(metadata.value, columnStartCounts, row))
        println(x.intValue()+ " :"+row.mkString(","))
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

    val columnStartCounts = LSHUtil.getColumnStartCounts(metadata.value);
    val rowsToIterate = rows.take(20);
    for (i <- 0 to 19) {
      val row = (LSHUtil.extractRow(metadata.value, columnStartCounts, (rowsToIterate(i))._2, true))
      println("Row: " + rowsToIterate(i))
      val neighbors = model.approxNearestNeighbors(txModel, Vectors.dense(row), 10).collectAsList().asInstanceOf[java.util.List[Row]];

      for (i <- 0 to neighbors.size() - 1) {
        val neighbor = neighbors.get(i);
        val output = LSHUtil.extractReturnObject(metadata.value, columnStartCounts, (neighbor.get(1).asInstanceOf[DenseVector]).values);
        println(neighbor.get(0).asInstanceOf[Int]+ " - "+output);
      }
    }
  }

}
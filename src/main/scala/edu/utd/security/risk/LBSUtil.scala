package edu.utd.security.risk

import scala.collection.mutable.ListBuffer

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

object LBSUtil {
  def getMinMax(value: String): (Double, Double) = {
    if (value.contains("_")) {
      val range = value.split("_");
      val min = range(0).toDouble
      val max = range(1).toDouble
      (min, max)
    } else {
      (value.toDouble, value.toDouble)
    }
  }
  def getColumnStartCounts(metadata: Metadata): Array[Int] = {
    var nextStartCount = 0;
    var columnStartCounts = ListBuffer[Int]();
    var index = 0;
    for (column <- metadata.getQuasiColumns()) {
      //println("->" + column.getIndex() + " " + column.getName());

    }
    //println("-->Size " + columnStartCounts.size+"-"+metadata.getQuasiColumns().size);
    for (column <- metadata.getQuasiColumns()) {
      //println("==>" + ":" + nextStartCount)
      columnStartCounts += nextStartCount;
      if (column.getColType() == 's') {

        nextStartCount = nextStartCount + column.getNumUnique();

      } else {

        nextStartCount = nextStartCount + 1;
      }
      //println("New NextStartCound" + nextStartCount + " index " + index +"="+ columnStartCounts(index))
      index = index + 1;
    }
    //println("Returning "+columnStartCounts.toArray)
    return columnStartCounts.toArray;
  }

  def getTotalNewColumns(metadata: Metadata): Int = {
    var totalCounts = 0;
    for (column <- metadata.getQuasiColumns()) {
      if (column.getColType() == 's') {
        totalCounts = totalCounts + column.getNumUnique();
      } else {
        totalCounts = totalCounts + 1;
      }
    }
    return totalCounts;
  }
  def getMinimalDataSet(metadata: Metadata, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): RDD[(Long, scala.collection.mutable.Map[Int, String])] = {
    val list = ListBuffer[Row]();
    val columns = ListBuffer[Int]();

    for (i <- 0 to metadata.numColumns() - 1) {
      val column = metadata.getMetadata(i).get;
      if (!column.getIsQuasiIdentifier()) {
        columns += column.getIndex();
        //println("blocking column : "+column.getIndex())
      }
    }
    linesRDD.map({
      case (x, y) => ({
        var newY = y;
        for (i <- columns) {
          newY.remove(i)
        }
        (x, newY)
      })
    });
  }

  def extractRow(metadata: Metadata, columnStartCounts: Array[Int], values: scala.collection.mutable.Map[Int, String], normalize: Boolean): Array[Double] = {
    var row: Array[Double] = Array.fill(getTotalNewColumns(metadata))(0.0);
    var index = 0;
    
    //println(columnStartCounts+ "- " +values+" ROWLENGTH" +row.length+ " : "+getTotalNewColumns(metadata));
    for (column <- metadata.getQuasiColumns()) {
      if (column.getColType() == 's') {
        row((columnStartCounts(index) + column.getRootCategory().getIndexOfColumnValue(values.get(column.getIndex()).get))) = 1.0;
      } else { 
          row(columnStartCounts(index)) = ((values.get(column.getIndex()).get.toDouble) - column.getMin()) / (column.getMax() - column.getMin());
      }
        index = index + 1;
      
    }
    //println(values)
    //println(row)
    //println(extractReturnObject(metadata, columnStartCounts, row))
    return row;
  }

  def extractReturnObject(metadata: Metadata, columnStartCounts: Array[Int], data: Array[Double]): scala.collection.mutable.Map[Int, String] =
    {
      var index: Int = 0;
      var map = scala.collection.mutable.Map[Int, String]();
      for (column <- metadata.getQuasiColumns()) {
        //println("column "+column.getName()+" :"+column.getIndex());
        if (column.getColType() == 's') {
          var position = columnStartCounts(index);
          var max = data(position);
          var maxPosition = position;
          //println(position+" : "+(position + column.getNumUnique()))
          for (pos <- position to (position + column.getNumUnique()-1)) {
          //println("Looping: "+pos)
            if (max < data(pos)) {
              max = data(pos);
              maxPosition = pos;
            }
          }
          map.put(column.getIndex(), column.getRootCategory().getValueAtIndex((maxPosition - columnStartCounts(index))));
        } else {
          map.put(column.getIndex(), (data(columnStartCounts(index)) * (column.getMax() - column.getMin()) + column.getMin()).toString);
        }
        index = index + 1;
      }
      //println(map);
      return map;
    }
}

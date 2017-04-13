package edu.utd.security.risk

import scala.collection.mutable.ListBuffer

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.immutable.HashSet

object LSHUtil {
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
  var columnStartCounts = Array[Int]();
  var totalCounts = 0;

  /**
   * This method calculates summary statitic for the Array of lines received.
   * Assumption is that input dataset contains only attributes of our interest. i.e. quasiIdentifier fields.
   * This assumption was made in order to get accurate statistics of the algorithm.
   */
  def assignSummaryStatistic(metadata: Broadcast[Metadata], lines: Array[(Long, Map[Int, String])]): Map[Long, String] =
    {
      var indexValueGroupedIntermediate = lines.flatMap(_._2).map({ case (x, y) => (x, Set[String](y)) }).groupBy(_._1).map(x => (x._1, x._2.map(_._2).flatten));
      var map = indexValueGroupedIntermediate.map({
        case (x, y) =>
          val column = metadata.value.getMetadata(x).get;
          if (column.isCharColumn()) {
            (x, column.findCategory(y.toArray).value());
          } else {
            val listOfNumbers = y.map(_.toDouble);
            val min = listOfNumbers.min;
            val max = listOfNumbers.max;
            if (min == max) {
              (x, min.toString);
            } else {
              (x, min + "_" + max);
            }
          }
      });
      /**
       * Once we have found the generalization hierarchy,map it to all lines and return the same.
       */
      val generalization = map.toArray.sortBy(_._1).map(_._2).mkString(",");
      return lines.map({
        case (x, y) =>
          (x, generalization)
      }).toMap;
    }
  def assignSummaryStatisticToRDD(metadata: Broadcast[Metadata], lines: RDD[(Long, (String, Int, Int, String))]): RDD[(Long, String)] =
    {
      val op = lines.map(x => (Set[String](x._2._1), Array.fill(2)(x._2._2),  Array.fill(2)(x._2._3), Set[String](x._2._4)));
      val opt = op.reduce({
        case ((a, b, c, d), (p, q, r, s)) => {
          var bq = Array[Int](b(0), b(1));
          if (b(0) > q(0)) {
            bq(0) = q(0);
          }
          if (b(1) < q(1)) {
            bq(1) = q(1);
          }

          var cr = Array[Int](c(0), c(1));
          if (c(0) > r(0)) {
            cr(0) = r(0);
          }
          if (c(1) < r(1)) {
            cr(1) = r(1);
          }

          (a.union(p), bq, cr, d.union(s))
        }
      });

      var column = metadata.value.getMetadata(0).get;

      var generalization = column.findCategory(opt._1.toArray).value()
      var min = opt._2.min;
      var max = opt._2.max;
      if (min == max) {
        generalization += "," + min;
      } else {
        generalization += "," + min + "_" + max;
      }
      min = opt._3.min;
      max = opt._3.max;
      if (min == max) {
        generalization += "," + min;
      } else {
        generalization += "," + min + "_" + max;
      }
      column = metadata.value.getMetadata(3).get;
      generalization += "," + column.findCategory(opt._4.toArray).value() /*mapPartitions(
         
         partition=>partition.flatMap(_._2))*/ ;
      /*     indexValueGroupedIntermediate.cache();
       val map0Gender=indexValueGroupedIntermediate.filter(_._1=="0").values.reduce((a,b)=>a.union(b));
        val map3Race =indexValueGroupedIntermediate.filter(_._1=="3").values.reduce((a,b)=>a.union(b));
        val age = indexValueGroupedIntermediate.filter(_._1=="2").values.distinct();
        val zip= indexValueGroupedIntermediate.filter(_._1=="1").values.distinct();
        indexValueGroupedIntermediate.unpersist(false);
    */
      /* .reduceByKey((a,b)=>(a.union(b)));
        
      var map = indexValueGroupedIntermediate.map({
        case (x, y) =>
          val column = metadata.value.getMetadata(x).get;
          if (column.isCharColumn() ) {
            (x, column.findCategory(y.toArray).value());
          } else {
            var listOfNumbers = y.map(_.toDouble);
            val min = listOfNumbers.min;
            val max = listOfNumbers.max;
            listOfNumbers = null;
            if (min == max) {
              (x, min.toString);
            } else {
              (x, min + "_" + max);
            }
          }
      });*/
      /**
       * Once we have found the generalization hierarchy,map it to all lines and return the same.
       */
      //    val generalization = map.collect().sortBy(_._1).map(_._2).mkString(",");
      return lines.keys.map({
        case (x) =>
          (x, generalization)
      });
    }

  def assignSummaryStatisticArray(metadata: Broadcast[Metadata], lines: Array[(Long, (String, Int, Int, String))]): Array[(Long, String)] =
    {

      val val0 = lines.map(_._2._1).distinct;
      val val1 = lines.map(_._2._2).distinct;
      val val2 = lines.map(_._2._3).distinct;
      val val3 = lines.map(_._2._4).distinct;

      var column = metadata.value.getMetadata(0).get;

      var generalization = column.findCategory(val0.toArray).value()
      var min = val1.min;
      var max = val1.max;
      if (min == max) {
        generalization += "," + min;
      } else {
        generalization += "," + min + "_" + max;
      }
      min = val2.min;
      max = val2.max;
      if (min == max) {
        generalization += "," + min;
      } else {
        generalization += "," + min + "_" + max;
      }

      column = metadata.value.getMetadata(3).get;
      generalization += "," + column.findCategory(val3.toArray).value() /*mapPartitions(
         
         partition=>partition.flatMap(_._2))*/ ;
      /*     indexValueGroupedIntermediate.cache();
       val map0Gender=indexValueGroupedIntermediate.filter(_._1=="0").values.reduce((a,b)=>a.union(b));
        val map3Race =indexValueGroupedIntermediate.filter(_._1=="3").values.reduce((a,b)=>a.union(b));
        val age = indexValueGroupedIntermediate.filter(_._1=="2").values.distinct();
        val zip= indexValueGroupedIntermediate.filter(_._1=="1").values.distinct();
        indexValueGroupedIntermediate.unpersist(false);
    */
      /* .reduceByKey((a,b)=>(a.union(b)));
        
      var map = indexValueGroupedIntermediate.map({
        case (x, y) =>
          val column = metadata.value.getMetadata(x).get;
          if (column.isCharColumn() ) {
            (x, column.findCategory(y.toArray).value());
          } else {
            var listOfNumbers = y.map(_.toDouble);
            val min = listOfNumbers.min;
            val max = listOfNumbers.max;
            listOfNumbers = null;
            if (min == max) {
              (x, min.toString);
            } else {
              (x, min + "_" + max);
            }
          }
      });*/
      /**
       * Once we have found the generalization hierarchy,map it to all lines and return the same.
       */
      //    val generalization = map.collect().sortBy(_._1).map(_._2).mkString(",");
      return lines.map({
        case (x, (p, q, r, s)) =>
          (x, generalization)
      });
    }

  def getColumnStartCounts(metadata: Metadata): Array[Int] = {

    if (columnStartCounts.size == 0) {
      var nextStartCount = 0;
      var index = 0;
      val counts = ListBuffer[Int]();
      for (column <- metadata.getQuasiColumns()) {
        counts += nextStartCount;
        if (column.isCharColumn()) {
          nextStartCount = nextStartCount + column.getNumUnique();
        } else {
          nextStartCount = nextStartCount + 1;
        }
        index = index + 1;
      }
      columnStartCounts = counts.toArray;
      getTotalNewColumns(metadata);
    }
    return columnStartCounts;
  }
  var emptyRow: Array[Double] = Array[Double]();
  def getTotalNewColumns(metadata: Metadata): Int = {
    if (totalCounts == 0) {

      for (column <- metadata.getQuasiColumns()) {
        if (column.isCharColumn()) {
          totalCounts = totalCounts + column.getNumUnique();
        } else {
          totalCounts = totalCounts + 1;
        }
      }
    }

    return totalCounts;
  }
  /*def getMinimalDataSet(metadata: Metadata, linesRDD: RDD[(Long, Array[String])], quasiIdentifier: Boolean): RDD[(Long, Map[Int, String])] = {
    val list = ListBuffer[Row]();
    val columns = ListBuffer[Int]();

    for (i <- 0 to metadata.numColumns() - 1) {
      val column = metadata.getMetadata(i).get;
      if (quasiIdentifier) {
        if (column.getIsQuasiIdentifier()) {
          columns += column.getIndex();
          //println("blocking column : "+column.getIndex())
        }
      } else {
        if (!column.getIsQuasiIdentifier()) {
          columns += column.getIndex();
          //println("blocking column : "+column.getIndex())
        }
      }

    }
    val map = linesRDD.map({
      case (x, y) => ({
        var newY: scala.collection.mutable.Map[Int, String] = new scala.collection.mutable.HashMap[Int, String]();
        newY ++= y;
        for (i <- columns) {
          newY.remove(i)
        }
        (x, newY.toMap)
      })
    });
    return map;
  }*/

  /*
  def extractReturnObject(metadata: Metadata, data: Array[Double]): scala.collection.mutable.Map[Int, String] =
    {
      var index: Int = 0;
      columnStartCounts = getColumnStartCounts(metadata);

      var map = scala.collection.mutable.Map[Int, String]();
      for (column <- metadata.getQuasiColumns()) {
        if (column.getColType() == 's') {
          var position = columnStartCounts(index);
          var max = data(position);
          var maxPosition = position;
          for (pos <- position to (position + column.getNumUnique() - 1)) {
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
    }*/
  /*def extractReturnObject(metadata: Metadata, data: Array[Double]): scala.collection.mutable.Map[Int, String] =
    {
      var index: Int = 0;
      columnStartCounts = getColumnStartCounts(metadata);

      var map = scala.collection.mutable.Map[Int, String]();
      for (column <- metadata.getQuasiColumns()) {
        if (column.isCharColumn()) {
          var position = columnStartCounts(index);
          var max = data(position);
          var maxPosition = position;
          for (pos <- position to (position + column.getNumUnique() - 1)) {
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
*/ }

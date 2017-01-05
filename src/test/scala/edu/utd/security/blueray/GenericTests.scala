package edu.utd.security.blueray

import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object GenericTests {

  def rdd_BlockLii[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean, stringToBeBlocked: String, newString: String, valueNotBlocked: String) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size + " =======> " + inputFile.collect().mkString, "1")
    assert(!inputFile.collect().mkString.contains(stringToBeBlocked), "2")
    assert(inputFile.collect().mkString.contains(valueNotBlocked), "3")
    println("#$%#$%" + inputFile.collect().mkString)
    assert(inputFile.collect().mkString.contains(newString), "4")
    assert(3 == inputFile.count(), "Count method testing")
    assert(inputFile.take(3).mkString.contains(newString), "take(3)  testing")
    assert(inputFile.takeSample(false, 3, 0).mkString.contains(newString), "takeSample testing")
    if (mapReduceOpsTesting) {
      val inputFileMapped = inputFile.asInstanceOf[RDD[String]]
      assert(!inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains(stringToBeBlocked), "reduceByKey ")
      assert(!inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains(valueNotBlocked), "reduceByKey ")
      assert(3 == inputFileMapped.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
      assert(3 == inputFileMapped.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
    }
    assert(inputFile.first().toString().contains(newString), "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(coalescedFile.collect().mkString.contains(newString), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(savedFile.collect().mkString.contains(newString), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "delete happened")

  }

  def rdd_BlockNone[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean, stringToBeBlockedMain: String, newString: String) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().mkString)
    var stringToBeBlocked: String = stringToBeBlockedMain;
    if (stringToBeBlockedMain.startsWith("\\.\\*")) {
      stringToBeBlocked = (stringToBeBlockedMain.subSequence(2, stringToBeBlockedMain.length() - 2)).toString()
    }
    assert(!inputFile.collect().mkString.contains(newString), "11")
    assert(3 == inputFile.count(), "Count method testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(coalescedFile.count() == 3, "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "22")

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(savedFile.count() == 3, "savedFile method testing33")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "44")

  }

  def rdd_BlockAll[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean, stringToBeBlocked: String, newString: String) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    inputFile.collect().foreach(println)
    assert(!inputFile.collect().mkString.contains(stringToBeBlocked), "1323")
    println("**********************************" + inputFile.collect().mkString)
    assert(inputFile.collect().mkString.contains("-"), "133")
    assert(3 == inputFile.count(), "Count method testing")
    assert(!inputFile.take(3).mkString.contains(stringToBeBlocked), "take(3)  testing")
    assert(!inputFile.takeSample(false, 3, 0).mkString.contains(stringToBeBlocked), "takeSample testing")
    if (mapReduceOpsTesting) {
      assert(3 == inputFile.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
      assert(3 == inputFile.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
    }

    assert(!inputFile.first().toString.contains(stringToBeBlocked), "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(!coalescedFile.collect().mkString.contains(stringToBeBlocked), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(!savedFile.collect().mkString.contains(stringToBeBlocked), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "1324")

  }

  def df_BlockID(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame, stringToBeBlocked: String, newString: String, valueNotBlocked: String) = {
    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------" + dfs.select("id").collect().mkString)
    assert(dfs.select("id").collect().length == 3, "DF-id testing")

    println("----------------------------------------------23424234234------------>>>>>>>>>>>>>>")
    assert(!dfs.select("id").collect().mkString.matches(stringToBeBlocked), "DF- id found no blocking happeneing")

    println("------------323W---------------------------------------------->>>>>>>>>>>>>>")
    assert(dfs.select("id").collect().mkString.contains(valueNotBlocked), "DF- unblocked value not found")
    dfs.filter(!_.mkString.contains(stringToBeBlocked));
    println("--------------------------------324-------------------------->>>>>>>>>>>>>>")
    assert(dfs.select("age").collect().length == 3, "DF- count mismatch")
    assert(dfs.count() == 3, "DF- count2 mismatch")
    println("----------------------234234234------------------------------------>>>>>>>>>>>>>>")
    assert(dfs.groupBy("age").count().count() == 3, "DF-age count mismatch")
    println("----------------------234fvs234234------------------------------------>>>>>>>>>>>>>>")

    val currentMillis = System.currentTimeMillis;
    val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
    dfs.write.format("json").save(fileName)
    var fileSaved = sc.textFile(fileName)
    println("--------------------32e--234234234------------------------------------>>>>>>>>>>>>>>")

    assert(3 == fileSaved.count(), "saved testing: df 3 found")
    println("---------------------------------------------------------->>>2>>>>>>>>>>>" + fileSaved.collect().mkString + "|" + stringToBeBlocked)
    assert(!fileSaved.collect().mkString.matches(stringToBeBlocked), "DF - Blocked string not present in file")
    assert(fileSaved.collect().mkString.contains(newString), "NEWString not foundL:(" + newString);
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "DF file not deleted")

  }
  def df_BlockAll(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame, stringToBeBlocked: String, newString: String) = {
    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&wewew&&&&&&&&-------------" + dfs.select("id").collect().mkString)
    assert(dfs.select("id").collect().length == 3)
    assert(!dfs.select("id").collect().mkString.contains(stringToBeBlocked))
    assert(!dfs.select("age").collect().mkString.contains("21"), "DF - age found");
    assert(!dfs.select("age").collect().mkString.contains("22"), "DF - age2 found");
    assert(!dfs.select("age").collect().mkString.contains("23"), "DF - age3 found");
    dfs.filter(!_.mkString.contains(stringToBeBlocked));
    assert(dfs.select("age").collect().length == 3)
    val currentMillis = System.currentTimeMillis;
    val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
    dfs.write.format("json").save(fileName)
    var fileSaved = sc.textFile(fileName)
    assert(3 == fileSaved.count(), "saved testing")
    assert(!fileSaved.collect().mkString.contains(stringToBeBlocked))
    assert(!fileSaved.collect().mkString.contains("jane"), "Jane shud be blockedi n all")
    assert(!fileSaved.collect().mkString.contains("saki"), "saki shud be blockedi n all")
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "Blockall file not deleted")

  }

  def df_BlockNone(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame, stringToBeBlocked: String, newString: String) = {
    val fileName = "hdfs://localhost/user/user" + System.currentTimeMillis() + ".json";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)

    println("-------------&&&%$%%$%$%$%$%$%&&&&&&&&-------------" + dfs.select("id").collect().mkString + "|")
    // assert(dfs.select("id").collect().length == 3,"Total length not 3 in blcknone"+dfs.select("id").collect().length)
    println("checlomg   " + dfs.select("id").collect().mkString.contains(stringToBeBlocked) + ": " + stringToBeBlocked + " : " + dfs.select("id").collect().mkString.split(stringToBeBlocked).size + "}}}" + dfs.select("id").collect().mkString.matches(stringToBeBlocked));

    assert((dfs.select("id").collect().mkString.split(stringToBeBlocked).size > 0) || (dfs.select("id").collect().mkString.matches(stringToBeBlocked)), "REGEX not applicable")
    assert(dfs.select("age").collect().mkString.contains("23"), "DF - age not found");
    assert(dfs.select("age").collect().mkString.contains("22"), "DF - age not found");
    assert(dfs.select("age").collect().mkString.contains("21"), "DF - age not found");
    dfs.filter(!_.mkString.contains(stringToBeBlocked));
    assert(dfs.select("age").collect().length == 3, "Age not 3 in block none")
    assert(dfs.count() == 3, "count not 3 in block nonoe")
    assert(dfs.groupBy("age").count().count() == 3, "Age count.count not 3")
    dfs.write.format("json").save(fileName)
    val fileSaved = sc.textFile(fileName)
    assert(3 == fileSaved.count(), "saved testing block noen not 3")
    assert((fileSaved.collect().mkString.split(stringToBeBlocked).size > 0 || (fileSaved.collect().mkString.matches(stringToBeBlocked))), "none : string not present")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true), "Block none file not deleted")
  }

}
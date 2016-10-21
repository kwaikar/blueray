package edu.utd.security.blueray

import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object GenericTests {

  def rdd_BlockLii[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    assert(!inputFile.collect().mkString.contains("Lii"))
    assert(inputFile.collect().mkString.contains("jane"))
    assert(inputFile.collect().mkString.contains("saki"))
    println("#$%#$%" + inputFile.collect().mkString)
    assert(inputFile.collect().mkString.contains("BLOCK"))
    assert(inputFile.collect().mkString.contains("23"))
    assert(inputFile.collect().mkString.contains("22"))
    assert(inputFile.collect().mkString.contains("21"))

    assert(3 == inputFile.count(), "Count method testing")
    assert(inputFile.take(3).mkString.contains("BLOCK"), "take(3)  testing")
    assert(inputFile.takeSample(false, 3, 0).mkString.contains("BLOCK"), "takeSample testing")
    inputFile.foreach(println)
    if (mapReduceOpsTesting) {
      val inputFileMapped = inputFile.asInstanceOf[RDD[String]]
       assert(  !inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains("Lii"), "reduceByKey ")
       assert(  !inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains("jane"), "reduceByKey ")
       assert(  !inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains("saki"), "reduceByKey ")
      assert(3 == inputFileMapped.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
       assert(3== inputFileMapped.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
    }
    assert(inputFile.first().toString().contains("BLOCK"), "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(coalescedFile.collect().mkString.contains("BLOCK"), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(savedFile.collect().mkString.contains("BLOCK"), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def rdd_BlockNone[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    assert(inputFile.collect().mkString.contains("Lii"))
    assert(inputFile.collect().mkString.contains("jane"))
    assert(inputFile.collect().mkString.contains("saki"))
    assert(!inputFile.collect().mkString.contains("BLOCK"))
    assert(inputFile.collect().mkString.contains("23"))
    assert(inputFile.collect().mkString.contains("22"))
    assert(inputFile.collect().mkString.contains("21"))

    assert(3 == inputFile.count(), "Count method testing")
    assert(inputFile.take(3).mkString.contains("Lii"), "take(3)  testing")
    assert(inputFile.takeSample(false, 3, 0).mkString.contains("Lii"), "takeSample testing")
    inputFile.foreach(println)
    assert(inputFile.first().toString().contains("Lii"), "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(coalescedFile.collect().mkString.contains("Lii"), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(savedFile.collect().mkString.contains("Lii"), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def rdd_BlockAll[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    assert(!inputFile.collect().mkString.contains("Lii"))
    assert(!inputFile.collect().mkString.contains("jane"))
    assert(!inputFile.collect().mkString.contains("saki"))
    // assert( inputFile.collect().mkString.contains("BLOCK"))
    assert(!inputFile.collect().mkString.contains("23"))
    assert(!inputFile.collect().mkString.contains("22"))
    assert(!inputFile.collect().mkString.contains("21"))

    assert(3 == inputFile.count(), "Count method testing")
    assert(!inputFile.take(3).mkString.contains("Lii"), "take(3)  testing")
    assert(!inputFile.takeSample(false, 3, 0).mkString.contains("Lii"), "takeSample testing")
    if (mapReduceOpsTesting) {
      // assert(count == inputFile.map(x => (x(1), 1)).reduceByKey(_ + _).collect().size, "reduceByKey ")
      //assert(count == inputFile.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
      // assert(count == inputFile.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
    }

    assert(!inputFile.first().toString.contains("Lii"), "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(!coalescedFile.collect().mkString.contains("Lii"), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(!savedFile.collect().mkString.contains("Lii"), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def df_BlockLii(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame) = {
    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------")
    assert(dfs.select("id").collect().length == 3)
    assert(!dfs.select("id").collect().mkString.contains("Lii"))
    assert(dfs.select("id").collect().mkString.contains("jane"))
    assert(dfs.select("id").collect().mkString.contains("saki"))
    assert(dfs.select("id").collect().mkString.contains("_BLOCK"));
    dfs.filter(!_.mkString.contains("Lii"));
    dfs.collect().foreach(println)
    assert(dfs.select("age").collect().length == 3)
    assert(dfs.count() == 3)
    println("====" + dfs.groupBy("age").count().count())
    assert(dfs.groupBy("age").count().count() == 3)
    val currentMillis = System.currentTimeMillis;

    val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
    dfs.write.format("json").save(fileName)
    println("==========================>" + fileName)

    var fileSaved = sc.textFile(fileName)
    println("==============wd============>" + fileName)
    fileSaved.collect().foreach(println);
    println("============dcd==============>")
    assert(3 == fileSaved.count(), "saved testing")

    assert(!fileSaved.collect().mkString.contains("Lii"))
    assert(fileSaved.collect().mkString.contains("jane"))
    assert(fileSaved.collect().mkString.contains("saki"))
    assert(dfs.select("age").collect().mkString.contains("23"));
    assert(dfs.select("age").collect().mkString.contains("22"));
    assert(dfs.select("age").collect().mkString.contains("21"));
    assert(fileSaved.collect().mkString.contains("_BLOCK"));
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }
   def df_BlockAll(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame) = {
    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------" + dfs.select("id").collect().mkString)
    assert(dfs.select("id").collect().length == 3)
    assert(!dfs.select("id").collect().mkString.contains("Lii"))
    assert(!dfs.select("id").collect().mkString.contains("jane"))
    assert(!dfs.select("id").collect().mkString.contains("saki"))
    // assert(dfs.select("id").collect().mkString.contains("BLOCK"));
    //assert(dfs.select("age").collect().mkString.contains("_BLOCK"));
    assert(!dfs.select("age").collect().mkString.contains("21"));
    assert(!dfs.select("age").collect().mkString.contains("22"));
    assert(!dfs.select("age").collect().mkString.contains("23"));
    dfs.filter(!_.mkString.contains("Lii"));
    dfs.collect().foreach(println)
    assert(dfs.select("age").collect().length == 3)
    println("________________________" + dfs.groupBy("age").count().count())
   // assert(dfs.groupBy("age").count().count() == 3)
    val currentMillis = System.currentTimeMillis;

    val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
    dfs.write.format("json").save(fileName)
    println("==========================>" + fileName)

    var fileSaved = sc.textFile(fileName)
    println("==============wd============>" + fileName)
    fileSaved.collect().foreach(println);
    println("============dcd==============>")
    assert(3 == fileSaved.count(), "saved testing")

    assert(!fileSaved.collect().mkString.contains("Lii"))
    assert(!fileSaved.collect().mkString.contains("jane"))
    assert(!fileSaved.collect().mkString.contains("saki"))
   // assert(fileSaved.collect().mkString.contains("_BLOCK"));
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def df_BlockNone(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame) = {
    val fileName = "hdfs://localhost/user/user" + System.currentTimeMillis() + ".json";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)

    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------")
    assert(dfs.select("id").collect().length == 3)
    assert(dfs.select("id").collect().mkString.contains("Lii"))
    assert(dfs.select("id").collect().mkString.contains("jane"))
    assert(dfs.select("id").collect().mkString.contains("saki"))
    assert(!dfs.select("id").collect().mkString.contains("_BLOCK"));
    assert(dfs.select("age").collect().mkString.contains("23"));
    assert(dfs.select("age").collect().mkString.contains("22"));
    assert(dfs.select("age").collect().mkString.contains("21"));
    dfs.filter(!_.mkString.contains("Lii"));
    dfs.collect().foreach(println)
    assert(dfs.select("age").collect().length == 3)
    assert(dfs.count() == 3)
    println("====" + dfs.groupBy("age").count().count())
    assert(dfs.groupBy("age").count().count() == 3)
    dfs.write.format("json").save(fileName)
    println("==========================>" + fileName)

    val fileSaved = sc.textFile(fileName)
    println("==============wd============>" + fileName)
    fileSaved.collect().foreach(println);
    println("============dcd==============>")
    assert(3 == fileSaved.count(), "saved testing")

    assert(fileSaved.collect().mkString.contains("Lii"))
    assert(fileSaved.collect().mkString.contains("jane"))
    assert(fileSaved.collect().mkString.contains("saki"))
    assert(!fileSaved.collect().mkString.contains("_BLOCK"));

    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))
  }

}
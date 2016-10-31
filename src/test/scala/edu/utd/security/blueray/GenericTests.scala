package edu.utd.security.blueray

import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object GenericTests {

  def rdd_BlockLii[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean, stringToBeBlocked:String, newString:String,valueNotBlocked:String) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size+" =======> "+inputFile.collect().mkString)
    assert(!inputFile.collect().mkString.contains(stringToBeBlocked))
    assert(inputFile.collect().mkString.contains(valueNotBlocked))
    println("#$%#$%" + inputFile.collect().mkString)
    assert(inputFile.collect().mkString.contains(newString))
    assert(inputFile.collect().mkString.contains("23"))
    assert(inputFile.collect().mkString.contains("22"))
    assert(inputFile.collect().mkString.contains("21"))

    assert(3 == inputFile.count(), "Count method testing")
    assert(inputFile.take(3).mkString.contains(newString), "take(3)  testing")
    assert(inputFile.takeSample(false, 3, 0).mkString.contains(newString), "takeSample testing")
    inputFile.foreach(println)
    if (mapReduceOpsTesting) {
      val inputFileMapped = inputFile.asInstanceOf[RDD[String]]
       assert(  !inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains(stringToBeBlocked), "reduceByKey ")
       assert(  !inputFileMapped.map(x => (x(1), 1)).reduceByKey(_ + _).collect().toString().contains(valueNotBlocked), "reduceByKey ")
      assert(3 == inputFileMapped.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
       assert(3== inputFileMapped.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
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
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def rdd_BlockNone[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean, stringToBeBlocked:String, newString:String) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    assert(inputFile.collect().mkString.contains(stringToBeBlocked))
    assert(!inputFile.collect().mkString.contains(newString))
    assert(inputFile.collect().mkString.contains("23"))
    assert(inputFile.collect().mkString.contains("22"))
    assert(inputFile.collect().mkString.contains("21"))

    assert(3 == inputFile.count(), "Count method testing")
    assert(inputFile.take(3).mkString.contains(stringToBeBlocked), "take(3)  testing")
    assert(inputFile.takeSample(false, 3, 0).mkString.contains(stringToBeBlocked), "takeSample testing")
    assert(inputFile.first().toString().contains(stringToBeBlocked), "Size function testing")

    val fileName = "hdfs://localhost/user/user_authorized_single" + currentMillis + ".csv";
    inputFile.coalesce(1).saveAsTextFile(fileName);
    var coalescedFile = sc.textFile(fileName)
    assert(coalescedFile.collect().mkString.contains(stringToBeBlocked), "coalescedFile method testing")

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

    inputFile.saveAsTextFile(fileName);
    var savedFile = sc.textFile(fileName)
    assert(savedFile.collect().mkString.contains(stringToBeBlocked), "savedFile method testing")
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

   def rdd_BlockAll[T](sc: SparkContext, inputFile: RDD[T], mapReduceOpsTesting: Boolean, stringToBeBlocked:String, newString:String) = {
    val currentMillis = System.currentTimeMillis;
    println(inputFile.collect().size)
    inputFile.collect().foreach(println)
    assert(!inputFile.collect().mkString.contains(stringToBeBlocked))
    println("**********************************"+inputFile.collect().mkString)
     assert( inputFile.collect().mkString.contains("-"))
    assert(!inputFile.collect().mkString.contains("23"))
    assert(!inputFile.collect().mkString.contains("22"))
    assert(!inputFile.collect().mkString.contains("21"))

    assert(3 == inputFile.count(), "Count method testing")
    assert(!inputFile.take(3).mkString.contains(stringToBeBlocked), "take(3)  testing")
    assert(!inputFile.takeSample(false, 3, 0).mkString.contains(stringToBeBlocked), "takeSample testing")
    if (mapReduceOpsTesting) {
      assert(3== inputFile.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }), "reduceLeft ")
       assert(3== inputFile.map(x => (1)).collect().reduce({ (x, y) => x + y }), "reduce ")
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
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def df_BlockID(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame, stringToBeBlocked:String, newString:String,valueNotBlocked:String) = {
    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------"+dfs.select("id").collect().mkString)
    assert(dfs.select("id").collect().length == 3)
    assert(!dfs.select("id").collect().mkString.contains(stringToBeBlocked))
    assert(dfs.select("id").collect().mkString.contains(valueNotBlocked))
    assert(dfs.select("id").collect().mkString.contains(newString));
    dfs.filter(!_.mkString.contains(stringToBeBlocked));
    assert(dfs.select("age").collect().length == 3)
    assert(dfs.count() == 3)
    println("====" + dfs.groupBy("age").count().count())
    assert(dfs.groupBy("age").count().count() == 3)
    val currentMillis = System.currentTimeMillis;

    val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
    dfs.write.format("json").save(fileName)
    var fileSaved = sc.textFile(fileName)
    assert(3 == fileSaved.count(), "saved testing")

    assert(!fileSaved.collect().mkString.contains(stringToBeBlocked))
    assert(dfs.select("age").collect().mkString.contains("23"));
    assert(dfs.select("age").collect().mkString.contains("22"));
    assert(dfs.select("age").collect().mkString.contains("21"));
    assert(fileSaved.collect().mkString.contains(newString));
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }
   def df_BlockAll(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame, stringToBeBlocked:String, newString:String) = {
    dfs.select("id").collect().foreach(println)
    println("-------------&&&&&&&&&&&&&&&&&-------------" + dfs.select("id").collect().mkString)
    assert(dfs.select("id").collect().length == 3)
    assert(!dfs.select("id").collect().mkString.contains(stringToBeBlocked))
    // assert(dfs.select("id").collect().mkString.contains(newString));
    //assert(dfs.select("age").collect().mkString.contains(newString));
    assert(!dfs.select("age").collect().mkString.contains("21"));
    assert(!dfs.select("age").collect().mkString.contains("22"));
    assert(!dfs.select("age").collect().mkString.contains("23"));
    dfs.filter(!_.mkString.contains(stringToBeBlocked));
    dfs.collect().foreach(println)
    assert(dfs.select("age").collect().length == 3)
    println("------------------------" + dfs.groupBy("age").count().count())
   // assert(dfs.groupBy("age").count().count() == 3)
    val currentMillis = System.currentTimeMillis;

    val fileName = "hdfs://localhost/user/user" + currentMillis + ".json";
    dfs.write.format("json").save(fileName)
    var fileSaved = sc.textFile(fileName)
    assert(3 == fileSaved.count(), "saved testing")

    assert(!fileSaved.collect().mkString.contains(stringToBeBlocked))
    assert(!fileSaved.collect().mkString.contains("jane"))
    assert(!fileSaved.collect().mkString.contains("saki"))
   // assert(fileSaved.collect().mkString.contains(newString));
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)
    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))

  }

  def df_BlockNone(sc: SparkContext, dfs: org.apache.spark.sql.DataFrame, stringToBeBlocked:String, newString:String) = {
    val fileName = "hdfs://localhost/user/user" + System.currentTimeMillis() + ".json";
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fileName), sc.hadoopConfiguration)

    println("-------------&&&&&&&&&&&&&&&&&-------------"+dfs.select("id").collect().mkString+"|")
    assert(dfs.select("id").collect().length == 3)
    assert(dfs.select("id").collect().mkString.contains(stringToBeBlocked))
    assert(!dfs.select("id").collect().mkString.contains(newString));
    assert(dfs.select("age").collect().mkString.contains("23"));
    assert(dfs.select("age").collect().mkString.contains("22"));
    assert(dfs.select("age").collect().mkString.contains("21"));
    dfs.filter(!_.mkString.contains(stringToBeBlocked));
    dfs.collect().foreach(println)
    assert(dfs.select("age").collect().length == 3)
    assert(dfs.count() == 3)
    println("====" + dfs.groupBy("age").count().count())
    assert(dfs.groupBy("age").count().count() == 3)
    dfs.write.format("json").save(fileName)
    val fileSaved = sc.textFile(fileName)
    assert(3 == fileSaved.count(), "saved testing")

    assert(fileSaved.collect().mkString.contains(stringToBeBlocked))
    assert(!fileSaved.collect().mkString.contains(newString));

    assert(fs.delete(new org.apache.hadoop.fs.Path(fileName), true))
  }

}
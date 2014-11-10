package org.apache.spark

import java.io.{File, FileWriter}

import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListener}

import scala.io.Source

import com.google.common.io.Files
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{NewHadoopRDD, HadoopRDD}
import org.apache.spark.util.Utils
import scala.collection.mutable.ArrayBuffer

/**
 * Created by kostas on 9/3/14.
 */
class ExprSuite extends FunSuite with LocalSparkContext with Logging {
  var tempDir: File = _
  var clusterUrl = "local-cluster[2,1,512]"

  override def beforeEach() {
    super.beforeEach()
    tempDir = Files.createTempDir()
    tempDir.deleteOnExit()
  }

  override def afterEach() {
    super.afterEach()
    Utils.deleteRecursively(tempDir)
  }

  test("parallize") {
    sc = new SparkContext("local", "test")


    val filename = "/Users/kostas/words.txt"

    val map = sc.textFile(filename, 10)
                .map(word => (word.length, 1))
                .reduceByKey(_ + _)
                .collect()

    //val inmem = sc.parallelize(1 to 100, 10)
    //              .cache()


    //val data = map.collect()
    //val inmemdata = inmem.collect();

    //val foo = map.coalesce(5)

   // val partitions = map.collectPartitions()

    //val food = inmem.coalesce(5)
    //              .collectPartitions()

    /*
    val map = sc.parallelize(1 to 100, 10)
                //.coalesce(5)
    val partitions = map.collectPartitions()
    logInfo(map.collectPartitions().toString())
    */


    //val total = taskBytesRead.reduce(_ + _)
    // 1 154 335


    logInfo("done")

  }

  test("text files") {

    val conf = new SparkConf()
    //conf.set("spark.explainOnly", "true")
    //conf.set("spark.printPlan", "true")

    sc = new SparkContext(clusterUrl, "test", conf)
    sc.explainOn()
    sc.disableExecution()

    //sc.pauseExecution(1)

    val filename = "/Users/kostas/words.txt"

    val map = sc.textFile(filename, 4)
                .map(word => (word.length, 1))
                //.filter(length => length != 8)
                .reduceByKey(_ + _)
                .filter(_._1 > 10)
                .coalesce(2, true)
                //.map(word => (word, "foo"))
                //.reduceByKey(_ + _)
                //.repartition(5)
                .collect()

    println("SUP")


/*
    val words = sc.textFile(filename, 5).filter(_.length > 2)

    val wordsFirstMapped = words.map(word => (word.charAt(0), word))
    val wordsSecondMapped = words.map(word => (word.charAt(1), word))

    val joined = wordsFirstMapped.join(wordsSecondMapped)
    joined.collect()

    println("\n\n==========\n\n")
 */
    /*
    val words2 = sc.textFile(filename, 5)
    val a = words2.map(word => (word.length, 1)).reduceByKey(_ + _)
    val b = a.filter(_._1 > 10).reduceByKey(_+_)
    val c = a.filter(_._1 > 5).reduceByKey(_ + _)
    val d = b.union(c).reduceByKey(_ + _)
    d.collect()

    println("\n\n==========\n\n")
      */

    // Words to their lengths
    //val wordsToLength = words.map(word => (word, word.length))

    //val wordsByLetter = words.map(word => (word.charAt(0), word))

    //val wordsStartWithA = words.filter(_.startsWith("a"))

    //val wordsStartWithB = words.filter(_.startsWith("a"))



   // val content = Source.fromFile(filename).mkString
    // Also try reading it in as a text file RDD
//    assert(sc.textFile(outputDir).collect().toList === List("1", "2", "3", "4"))
  }

/*
  test("simple groupByKey") {
    sc = new SparkContext(clusterUrl, "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 5)
    val groups = pairs.groupByKey(5).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }
  */
}

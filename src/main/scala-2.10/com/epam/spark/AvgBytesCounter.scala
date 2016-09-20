package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AvgBytesCounter {

  val appName:String = "Server Log Analyzer"
  val master:String = "local"
  var sc:SparkContext = _

  def main(args: Array[String]) {
    if (args.length != 2){
      print("Please use 2 parameters: [source file, dest file]")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    sc = new SparkContext(conf)

    val dataSet = sc.textFile(args(0))
    val topLogs = getTop(dataSet)
    topLogs.saveAsTextFile("hdfs://"+ args(1))
    topLogs.collect.foreach(println)
  }

  def getTop(text: RDD[String]): RDD[String]={
    val logs = text.map(s=>s.split("""(?:[\"\]\[]|[\]] [\"\]\[]|[\]])|(?:( - - ))"""))
      .filter(f=>(f(0).length>2 && f(5).split(" ").size==3))
      .map(line => (parseIp(line(0)), parseBytes(line(5))))
      .aggregateByKey((0,0))(seqOp, combOp)
      .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2._1))
      .map(result => result._1.toString +";"+result._2._1.toString +";"+ (result._2._1/result._2._2).toString)

    return sc.parallelize(logs, 1)
  }

  def parseBytes(str:String):Int={
    val elem:String = str.split(" ").apply(2)
    if (elem.equals("-"))
      return 0
    return elem.toInt
  }
  def parseIp(str:String):Int={
    val elem:String = str.substring(2, str.length)
    return elem.toInt
  }

  def seqOp(totalCountPair: (Int, Int), bytesTransferred: Int):(Int, Int)={
    return (totalCountPair._1 + bytesTransferred, totalCountPair._2 + 1)
  }

  def combOp(pairOne: (Int, Int), pairTwo: (Int, Int)):(Int, Int)={
    return (pairOne._1 + pairTwo._1, pairOne._2 + pairTwo._2)
  }

}


package com.sample.csv.spark

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.sql.{Connection, Timestamp}
import java.text.SimpleDateFormat

object SparkCSVSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCSVSample")
    val context = new SparkContext(conf)
    val (logDir, csvFilePath, targetUrls) = (args(0), args(1), args(2))

    val records = logFiles(logDir).map { logFile =>
      val logData = context.textFile(logFile)
      logData.map(line => LogParser.parse(line).getOrElse(Log())).filter(!_.time.isEmpty)
    }.reduce(_ ++ _)

    val counts = targetUrls.split(",").map { url =>
      val trimmedUrl = url.trim
      countsByUrl(records, trimmedUrl)
    }.reduce(_ ++ _)

    writeDB(counts)
    writeCSV(csvFilePath, counts)
  }

  private def logFiles(logDir: String): Seq[String] = new File(logDir).listFiles.map(_.getPath).filter(_.lastIndexOf("log") > 0)

  private def countsByUrl(records: RDD[Log], url: String): RDD[Option[String]] = {
    val partitions = records.mapPartitions { partitionRecords =>
      val list = partitionRecords.toList
      val filteredRecords = list.filter(_.url == url)

      if (filteredRecords.isEmpty) {
        val time = list.map(_.time).head
        Iterator((time, 0))
      } else
        filteredRecords.map(record => (record.time, 1)).toIterator
    }

    partitions.reduceByKey(_ + _).sortBy(_._1).glom.map{ record =>
      val rec = record.map(_._2)
      record.map(_._1).headOption match{
        case Some(time) => Some(s"${time},$url,${rec.mkString(",")}")
        case _ => None
      }
    }
  }

  private def writeDB(countData: RDD[Option[String]]): Unit = {

    // 環境変数使いたいけど一旦はこれで。。
    val url = "jdbc:mysql://localhost:3306/nano_planner_dev"
    val username = "root"
    val password = ""

    countData.foreachPartition(iter => {
      using(getDbConnection(url,username,password)) { implicit conn =>
        iter.foreach {
          case Some(s) =>
            s.split(",") match {
              case  Array(lodDate, url, cnt) =>
                //洗い替えできるようにREPLACEを使いました
                val del = conn.prepareStatement ("REPLACE INTO logs (log_date, url, count) VALUES (?,?,?) ")
                del.setTimestamp(1,new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse(lodDate).getTime()))
                del.setString(2, url)
                del.setInt(3, cnt.toInt)
                del.executeUpdate
              case _ =>
            }
          case None =>
        }
      }
    })
  }

  def getDbConnection(url: String, username: String, password: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    java.sql.DriverManager.getConnection(url,username,password)
  }

  def using[X <: {def close()}, A](resource : X)(f : X => A): A =
    try { f(resource)
    } finally { resource.close()}

  private def writeCSV(csvFilePath: String, countData: RDD[Option[String]]): Unit = {
    val tempFileDir = "tmp/spark_temp"
    FileUtil.fullyDelete(new File(tempFileDir))
    FileUtil.fullyDelete(new File(csvFilePath))

    countData.filter(_.isDefined).map(_.head).saveAsTextFile(tempFileDir)
    merge(tempFileDir, csvFilePath)
  }

  private def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }
}

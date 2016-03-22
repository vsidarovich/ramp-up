import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.util.matching.Regex

object AverageCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Count Bytes Application").setMaster("local[4]")
    val ctx = new SparkContext(conf)

    val mozillaAc = ctx.accumulator(0, "Mozilla")
    val operaAc = ctx.accumulator(0, "Opera")
    val otherAc = ctx.accumulator(0, "Other")

    val logData = ctx.textFile("logs\\000000")

    val parsedLogs = parseLogs(logData, mozillaAc, operaAc, otherAc)
    val sum = countAverageSum(parsedLogs);

    sum.saveAsTextFile("output")

    println("Mozilla browser usages = " + mozillaAc)
    println("Opera browser usages = " + operaAc)
    println("Other browser usages = " + otherAc)
  }

  def parseLogs(logData: RDD[String], mozillaAc: Accumulator[Int], operaAc: Accumulator[Int],
                otherAc: Accumulator[Int]): RDD[(String, Int)] = {

    val ipPattern = new Regex("^ip[\\d]+")
    val bytesPattern = new Regex("\\d{3}+ \\d+")

    val parsedData = logData.map(line => {
      calculateBrowsers(mozillaAc, operaAc, otherAc, line)
      val ip = ipPattern.findFirstIn(line).getOrElse("none")
      var bytes = 0;
      bytesPattern.findAllMatchIn(line).foreach { digits => {
        val parsedBytes = digits.group(0).split(" ") {
          1
        }
        if (!parsedBytes.isEmpty) {
          bytes = parsedBytes.toInt
        }
      }
      }
      (ip, bytes)
    })
    return parsedData
  }

  def calculateBrowsers(mozillaAc: Accumulator[Int], operaAc: Accumulator[Int],
                        otherAc: Accumulator[Int], line: String): Unit = {
    if (line.contains("Mozilla")) {
      mozillaAc += 1;
    } else if (line.contains("Opera")) {
      operaAc += 1;
    } else {
      otherAc += 1;
    }
  }

  def countAverageSum(parsedLogs: RDD[(String, Int)]): RDD[(String, Int)] = {
    val countMap = parsedLogs.mapValues(word => (word, 1))
    val reducedData = countMap.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val sum = reducedData.mapValues { case (sum, count) => (sum) / count}
    return sum;
  }
}

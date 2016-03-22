import java.util.Calendar

import hbase.HbaseHelper
import kafka.CustomKafkaProducer
import model.{Statistics, Alert, PackageInfo, Setting}
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import receiver.CustomReceiver

import scala.collection.mutable.MutableList


object SnifferRunner {
  val kafkaHelper = new CustomKafkaProducer
  var hbaseHelper = new HbaseHelper
  var sparkContext: SparkContext = null

  val batchTime = Seconds(10)
  val checkpointDirectory: String = ("hdfs://sandbox.hortonworks.com:8020/user/root/sniffer/")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Sniffer analyzer")
      .setMaster("local[4]")
      .set("spark.driver.allowMultipleContexts", "true")

    sparkContext = new SparkContext(conf)

    val settings = hbaseHelper.getSettings(sparkContext)
    val defaultSetting = getDefaultSetting(settings)

    val ssc = new StreamingContext(conf, batchTime)
    val lines = ssc.receiverStream(new CustomReceiver())

    lines.checkpoint(batchTime * 10)
    ssc.checkpoint(checkpointDirectory)

    startStatisticsGathering(lines)

    val definedIps = analyzeKnownIps(lines, settings)
    analyzeUnknowsIps(lines, definedIps, defaultSetting)

    ssc.start()
    ssc.awaitTermination()
  }

  private def analyzeKnownIps(lines: DStream[PackageInfo], settings: Array[Setting]): MutableList[String] = {
    var definedIps = MutableList[String]()

    settings.foreach(s => {
      val ip = s.hostIp
      if (!s.hostIp.contains("NULL")) {
        var analyzePackages: DStream[PackageInfo] = null

        //Threshold
        if (s.limitType == 1) {
          analyzePackages = lines
          //Limit
        } else {
          analyzePackages = lines.window(Duration(1000 * s.limitPeriod))
        }

        val groupedPackages = analyzePackages.map(x => (x.ip, x.totalBytesSize))
          .reduceByKey((x, y) => {
          x + y
        })

        //  groupedPackages.print(100)

        groupedPackages.foreachRDD(x => {
          val filteredCollection = x.filter(f => {
            s.hostIp.equals(f._1)
          })

          if (!filteredCollection.isEmpty())
            analyzeKnownTraffic(filteredCollection.first(), s)
        })
        definedIps += s.hostIp
      }
    })
    definedIps
  }

  private def analyzeUnknowsIps(lines: DStream[PackageInfo], definedIps: MutableList[String],
                                defaultSetting: Setting): Unit = {
    val analyzePackages = lines.window(Duration(defaultSetting.limitPeriod * 1000))

    val groupedPackages = analyzePackages.map(x => (x.ip, x.totalBytesSize))
      .reduceByKey((x, y) => {
      x + y
    })

    groupedPackages.foreachRDD(x => {
      val filteredCollection = x.filter(f => {
        !definedIps.contains(f._1)
      })

      if (!filteredCollection.isEmpty())
        analyzeUnknownTraffic(filteredCollection.first(), defaultSetting)
    })
  }

  private def analyzeKnownTraffic(ipItem: (String, Int), s: Setting): Unit = {
    val currentIp = s.hostIp
    val limitValue = s.limitValue
    val currentDate = Calendar.getInstance().getTime()

    val flag = hbaseHelper.getAlertedFlag(currentIp, sparkContext)

    if (ipItem._2 > limitValue && !flag) {
      val guid = generateGUID(currentIp, currentDate.getTime)
      val alert = new Alert(guid, currentDate, ipItem._1, s.limitType.toString, ipItem._2, limitValue)
      kafkaHelper.sendAlert(alert)
      hbaseHelper.changeAlertedFlag(currentIp, true, sparkContext)
    }

    if (ipItem._2 < limitValue && flag) {
      hbaseHelper.changeAlertedFlag(currentIp, false, sparkContext)
    }
  }

  private def analyzeUnknownTraffic(ipItem: (String, Int), s: Setting): Unit = {
    val currentDate = Calendar.getInstance().getTime()
    val currentIp = ipItem._1

    if (ipItem._2 > s.limitValue) {
      val guid = generateGUID(currentIp, currentDate.getTime)
      val alert = new Alert(guid, currentDate, currentIp, s.limitType.toString, ipItem._2, s.limitValue)
      kafkaHelper.sendAlert(alert)
    }
  }

  private def getDefaultSetting(settings: Array[Setting]): Setting = {
    var defaultSettings: Setting = null
    settings.foreach(s => {
      if (s.hostIp == "NULL_2") {
        defaultSettings = s
      }
    })
    defaultSettings
  }

  private def generateGUID(ip: String, timeStamp: Long): String = {
    ip.replace(".", "") + timeStamp
  }

  //bad approach, because it will consume lots of memory while opening a window for 1 hour. See @StatisticsGathering
  private def startStatisticsGathering(lines: DStream[PackageInfo]): Unit = {
    val oneHour = 3600 * 1000
    val oneHourDur = Duration(oneHour)

    lines.window(oneHourDur, oneHourDur).foreachRDD(x => {
      val timestamp = Calendar.getInstance().getTime()
      val statisticsMap = x.map(x => (x.ip, x.totalBytesSize))
        .reduceByKey((x, y) => {
        x + y
      })

      statisticsMap.foreach(x => {
        val trafficConsumed = x._2
        hbaseHelper.putWindowStatistics(new Statistics(x._1, timestamp, trafficConsumed, trafficConsumed / oneHour))
      })
    })
  }
}
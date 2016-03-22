import java.util.Calendar

import encoder.PackageInfoDecoder
import hbase.HbaseHelper
import kafka.serializer.StringDecoder
import model.{PackageInfo, Statistics}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*The main idea of the class is to create an application, which will consume ip packages from Kafka topics, which will be divided 1 topic per hour.
The SnifferRunner will be responsible for generation new topics per hour and pushing ip packages info into these topics, while this application will
obtain information about the precise topic which it should consume and then calculate traffic statistics and send that to hbase. Frankly speaking,
I didn't have enough time to implement this functionality, but I decided to implement at least every 10 second consumption from topic "ips" and sending all
statistics to hbase. It works.*/

object StatisticsGathering {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Statistics reader")
      .setMaster("local[4]")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val kafkaConsumerSSC = new StreamingContext(conf, Seconds(10))

    val hbaseHelper = new HbaseHelper()

    val kafkaConf = Map(
      "metadata.broker.list" -> "sandbox.hortonworks.com:9092",
      "zookeeper.connect" -> "sandbox.hortonworks.com:2181",
      "group.id" -> "123",
      "zookeeper.connection.timeout.ms" -> "1000")

    val kafkaTopics = Map("ips" -> 1)
    val kafkaStream = KafkaUtils.createStream[String, PackageInfo, StringDecoder, PackageInfoDecoder](kafkaConsumerSSC, kafkaConf, kafkaTopics, StorageLevel.MEMORY_ONLY_SER)

//    kafkaStream.print(100)

    val today = Calendar.getInstance().getTime()

    val statistics = kafkaStream.map(x => {
      (x._2.ip, x._2)
    }).mapValues(x => {
      (x, 1)
    })
      .reduceByKey((x, y) => {
      val bytes = x._1.totalBytesSize + y._1.totalBytesSize
      (new PackageInfo(x._1.ip, bytes), x._2 + y._2)
    })
      .map(x => {
      val totalBytes = x._2._1.totalBytesSize
      val averageSpeed = totalBytes / 10
      new Statistics(x._1, today, totalBytes, averageSpeed)
    })

    hbaseHelper.putStatistics(statistics)
    kafkaConsumerSSC.start()
    kafkaConsumerSSC.awaitTermination()
  }
}

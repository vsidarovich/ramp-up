package kafka

import java.util

import model.{Alert, PackageInfo}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class CustomKafkaProducer {
  private val alertProducer: KafkaProducer[String, Object] = createAlertProducer()
  private val packageProducer: KafkaProducer[String, PackageInfo] = createProductProducer()

  def sendAlert(alertMsg: Alert): Unit = {
    val message = new ProducerRecord[String, Object]("alerts", null, alertMsg.toString)
    alertProducer.send(message)
  }

  def sendPackageInfo(packageInfo: PackageInfo): Unit = {
    val message = new ProducerRecord[String, PackageInfo]("ips", null, packageInfo)
    packageProducer.send(message)
  }

  private def createProductProducer(): KafkaProducer[String, PackageInfo] = {
    val props = new util.HashMap[String, Object]()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox.hortonworks.com:6667")
    props.put("zk.connect", "sandbox.hortonworks.com:2181")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "encoder.PackageInfoEncoder")

    new KafkaProducer[String, PackageInfo](props)
  }

  private def createAlertProducer(): KafkaProducer[String, Object] = {
    val props = new util.HashMap[String, Object]()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox.hortonworks.com:6667")
    props.put("zk.connect", "sandbox.hortonworks.com:2181")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, Object](props)
  }
}

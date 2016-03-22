package receiver

import kafka.CustomKafkaProducer
import model.PackageInfo
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.core.BpfProgram.BpfCompileMode
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode
import org.pcap4j.core.{PcapHandle, Pcaps}
import org.pcap4j.packet.{EthernetPacket, IpV4Packet}

case class CustomReceiver()
  extends Receiver[PackageInfo](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  val SNAPLEN_KEY = this.getClass.getName + ".snaplen"
  val SNAPLEN = Integer.getInteger(SNAPLEN_KEY, 65536)

  val READ_TIMEOUT_KEY = this.getClass.getName + ".readTimeout"
  val READ_TIMEOUT = Integer.getInteger(READ_TIMEOUT_KEY, 10)


  def onStart() {
    new Thread("Socket Receiver") {
      override def run(): Unit = {
        val nif = Pcaps.getDevByName("eth0")

        val handle: PcapHandle = nif.openLive(SNAPLEN, PromiscuousMode.PROMISCUOUS, READ_TIMEOUT)
        handle.setFilter("", BpfCompileMode.OPTIMIZE)

        val kafkaProducer = new CustomKafkaProducer()

        while (true) {
          var ethernetPacket = handle.getNextPacket
          if (ethernetPacket != null) {
            ethernetPacket = ethernetPacket.asInstanceOf[EthernetPacket]
            val nextLevelPayload = ethernetPacket.getPayload
            if (nextLevelPayload.isInstanceOf[IpV4Packet]) {
              val ip4vPacket = ethernetPacket.getPayload.asInstanceOf[IpV4Packet]
              val ip4vHeader = ip4vPacket.getHeader
              ip4vHeader.getTotalLengthAsInt
              val packageInfo = new PackageInfo(ip4vHeader.getDstAddr.getHostAddress, ip4vHeader.getTotalLengthAsInt)
              kafkaProducer.sendPackageInfo(packageInfo)
              store(packageInfo)
            }
          }
        }
      }
    }.start()
  }

  def onStop() {
    println("Stop")
  }
}


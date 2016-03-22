package encoder

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import model.PackageInfo
import org.apache.kafka.common.serialization.Serializer

/**
 * Created by root on 11/24/15.
 */
case class PackageInfoEncoder(props: VerifiableProperties = null) extends Encoder[PackageInfo] with Serializer[PackageInfo] {

  def this() {
    this(null)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: PackageInfo): Array[Byte] = {
    return this.toBytes(data)
  }

  override def close(): Unit = {

  }

  override def toBytes(value: PackageInfo): Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(value)
    return b.toByteArray()
  }
}

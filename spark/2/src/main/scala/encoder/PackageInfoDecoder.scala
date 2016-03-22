package encoder

import java.io.{ByteArrayInputStream, ObjectInputStream}

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import model.PackageInfo

/**
 * Created by root on 11/24/15.
 */
case class PackageInfoDecoder(props: VerifiableProperties = null) extends Decoder[PackageInfo] {
  def fromBytes(bytes: Array[Byte]): PackageInfo = {
    val b = new ByteArrayInputStream(bytes)
    val o = new ObjectInputStream(b)
    return o.readObject().asInstanceOf[PackageInfo]
  }
}

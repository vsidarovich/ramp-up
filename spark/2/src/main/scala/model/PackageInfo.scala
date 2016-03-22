package model

@SerialVersionUID(101L)
case class PackageInfo(ip: String, totalBytesSize: Int) extends Serializable {

  override def toString(): String = ip + " " + totalBytesSize
}

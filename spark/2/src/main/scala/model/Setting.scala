package model

@SerialVersionUID(102L)
case class Setting(hostIp: String, limitType: Int, limitValue: Long, limitPeriod: Long) extends Serializable {

}

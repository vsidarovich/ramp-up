package model

import java.util.Date

@SerialVersionUID(103L)
case class Statistics(hostIp: String, timestamp: Date, trafficConsumed: Long, averageSpeed: Double) extends Serializable {

}

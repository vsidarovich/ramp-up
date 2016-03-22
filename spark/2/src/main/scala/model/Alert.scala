package model

import java.util.Date

@SerialVersionUID(100L)
case class Alert (periodId: String, timestamp: Date, hostIp: String, limitType: String,
                  factValue: Long, thresholdValue: Long) extends Serializable {

  override def toString(): String = "GUID = " + periodId + " " + timestamp + " PERIOD_ID = " + periodId + " HOST_IP =  " + hostIp + " LIMIT_TYPE = " + limitType + " FACT_VALUE = " + factValue + " THRESHOLD_VALUE = " + thresholdValue

}

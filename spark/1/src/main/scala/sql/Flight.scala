package sql

class Flight (year: String, month: String, dayOfMonth: String, dayOfWeek: String, depTime: String, cRSDepTime: String,
               arrTime: String, cRSArrTime: String, uniqueCarrier: String, flightNum: String, tailNum: String,
               actualElapsedTime: String, cRSElapsedTime: String, airTime: String, arrDelay: String, depDelay: String,
               origin: String, dest: String, distance: String, taxiIn: String, taxiOut: String, cancelled: String,
               cancellationCode: String, diverted: String, carrierDelay: String, weatherDelay: String, nASDelay: String,
               securityDelay: String, lateAircraftDelay: String) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => year.replace("\"", "")
    case 1 => month.replace("\"", "")
    case 2 => dayOfMonth.replace("\"", "")
    case 3 => dayOfWeek.replace("\"", "")
    case 4 => depTime.replace("\"", "")
    case 5 => cRSDepTime.replace("\"", "")
    case 6 => arrTime.replace("\"", "")
    case 7 => cRSArrTime.replace("\"", "")
    case 8 => uniqueCarrier.replace("\"", "")
    case 9 => flightNum.replace("\"", "")
    case 10 => tailNum.replace("\"", "")
    case 11 => actualElapsedTime.replace("\"", "")
    case 12 => cRSElapsedTime.replace("\"", "")
    case 13 => airTime.replace("\"", "")
    case 14 => arrDelay.replace("\"", "")
    case 15 => depDelay.replace("\"", "")
    case 16 => origin.replace("\"", "")
    case 17 => dest.replace("\"", "")
    case 18 => distance.replace("\"", "")
    case 19 => taxiIn.replace("\"", "")
    case 20 => taxiOut.replace("\"", "")
    case 21 => cancelled.replace("\"", "")
    case 22 => cancellationCode.replace("\"", "")
    case 23 => diverted.replace("\"", "")
    case 24 => carrierDelay.replace("\"", "")
    case 25 => weatherDelay.replace("\"", "")
    case 26 => nASDelay.replace("\"", "")
    case 27 => securityDelay.replace("\"", "")
    case 28 => lateAircraftDelay.replace("\"", "")
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 28

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Flight]
}


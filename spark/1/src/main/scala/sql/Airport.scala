package sql

class Airport(iata: String, airport: String, city: String, state: String, country: String, lat: String, long: String) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => iata.replace("\"", "")
    case 1 => airport.replace("\"", "")
    case 2 => city.replace("\"", "")
    case 3 => state.replace("\"", "")
    case 4 => country.replace("\"", "")
    case 5 => lat.replace("\"", "")
    case 6 => long.replace("\"", "")
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 24

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Airport]
}

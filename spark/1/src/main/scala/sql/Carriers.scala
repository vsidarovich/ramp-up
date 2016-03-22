package sql

class Carriers(code: String, description: String) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => code.replace("\"", "")
    case 1 => description.replace("\"", "")
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 24

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Carriers]
}

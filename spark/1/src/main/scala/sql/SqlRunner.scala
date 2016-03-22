
package sql

import org.apache.spark.{SparkContext, SparkConf}

object SqlRunner {
  val flightsCountSql: String = "select count(f.flightNum), f.uniqueCarrier from flights f GROUP BY f.uniqueCarrier"
  val nyCountSql: String = "select sum(flightNum) from (select count (f.flightNum) as flightNum  from flights f inner join airports a on f.origin  = a.iata where f.month = 6 and a.city = 'New York' UNION ALL select count (f.flightNum)  as flightNum  from flights f inner join airports a on f.dest  = a.iata where f.month = 6 and a.city = 'New York') as flights_count"
  val fiveAirportsSql: String = "select count(f.flightNum) as fcount, f.uniqueCarrier from flights f where f.month between 6 and 8 group by f.uniqueCarrier order by fcount desc limit 5"
  val biggestNumberSql: String = "select count(f.flightNum) as fcount, f.uniqueCarrier from flights f group by f.uniqueCarrier order by fcount desc limit 1"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Count Bytes Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);

    import sqlContext.implicits._

    val flights = sc.textFile("logs\\flights.csv")
      .map(_.split(","))
      .map(a => {
      new Flight(a(0), a(1), a(2), a(3),
        a(4), a(5), a(6), a(7), a(8),
        a(9), a(10), a(11), a(12),
        a(13), a(14), a(15), a(16),
        a(17), a(18), a(19), a(20),
        a(21), a(22), a(23), a(24),
        a(25), a(26), a(27), a(28))
    }).toDF()

    val airports = sc.textFile("logs\\airports.csv")
      .map(_.split(","))
      .map(a => {
      new Airport(a(0), a(1), a(2),
        a(3), a(4), a(5), a(6))
    }).toDF()

    val carriers = sc.textFile("logs\\carriers.csv").map(_.split(",")).map(a => {
      new Carriers(a(0), a(1))
    }).toDF()

    carriers.registerTempTable("carriers")
    flights.registerTempTable("flights")
    airports.registerTempTable("airports")

    val flightsCount = sqlContext.sql(flightsCountSql)
    val nyCount = sqlContext.sql(nyCountSql)
    val fiveAirports = sqlContext.sql(fiveAirportsSql)
    val biggestNumber = sqlContext.sql(biggestNumberSql)

    println(flightsCount.show())
    println(nyCount.show())
    println(fiveAirports.show())
    println(biggestNumber.show())
  }
}

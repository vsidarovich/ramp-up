import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, _}

class AverageCountTest extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {
  private val master = "local[2]"
  private val appName = "average-count-test"
  private var ctx: SparkContext = _

  val logsArray = Array("ip1 - - [24/Apr/2011:04:06:01 -0400] GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1 200 40028 - Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots))",
    "ip1 - - [24/Apr/2011:04:10:19 -0400] GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1 200 56928 - Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
    "ip1 - - [24/Apr/2011:04:14:36 -0400] GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1 200 42011 - Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
    "ip1 - - [24/Apr/2011:04:18:54 -0400] GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1 200 6244 - Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
    "ip2 - - [24/Apr/2011:04:20:11 -0400] GET /sun_ss5/ HTTP/1.1 200 14917 http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16",
    "ip2 - - [24/Apr/2011:04:20:11 -0400] GET /sun_ss5/pdf.gif HTTP/1.1 200 390 http://host2/sun_ss5/ Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16",
    "ip7 - - [24/Apr/2011:04:31:21 -0400]  GET /sgi_indy/indy_monitor.jpg HTTP/1.1 200 12805 http://machinecity-hello.blogspot.com/2008_01_01_archive.html Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10"
  )

  val ipBytesArray = Array(("ip1", 40028), ("ip1", 56928),
    ("ip1", 42011), ("ip1", 6244), ("ip1", 14917), ("ip1", 390), ("ip7", 12805))

  val averageSumArray = Array(("ip1", 26753), ("ip7", 12805))

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "true")
    ctx = new SparkContext(conf)
  }

  after {
    if (ctx != null) {
      ctx.stop()
    }
  }

  "Parsed logs" should "be checked" in {
    Given("Logs checking")

    Given("Spark accumulator for Mozilla browsers usages")
    var mozillaAc = ctx.accumulator(0, "Mozilla")

    Given("Spark accumulator for Opera browsers usages")
    var operaAc = ctx.accumulator(0, "Opera")

    Given("Spark accumulator for other browsers usages")
    var otherAc = ctx.accumulator(0, "Other")

    When("Parser processing...")
    val parsedLogs = AverageCount.parseLogs(ctx.parallelize(logsArray), mozillaAc, operaAc, otherAc)

    Then("Parsed logs array length is checked")
    ipBytesArray.length should equal(parsedLogs.count())

    Then("Parsed logs content is checked")
    ipBytesArray.contains(parsedLogs.first()) shouldBe true
  }

  "Average sum" should "be checked" in {
    Given("Sum checking")
    When("Sum calculation processing...")
    val sum = AverageCount.countAverageSum(ctx.parallelize(ipBytesArray))

    Then("Average sum array length is checked ")
    sum.count() should equal(averageSumArray.length)

    Then("PAverage sum array content is checked")
    averageSumArray.contains(sum.first()) shouldBe true
  }

  "Calculate browsers" should "be checked" in {
    Given("Calculate browsers checking")

    Given("Spark accumulator for Mozilla browsers usages")
    var mozillaAc = ctx.accumulator(0, "Mozilla")

    Given("Spark accumulator for Opera browsers usages")
    var operaAc = ctx.accumulator(0, "Opera")

    Given("Spark accumulator for other browsers usages")
    var otherAc = ctx.accumulator(0, "Other")

    When("Calculate browsers processing...")
    val sum = AverageCount.calculateBrowsers(mozillaAc, operaAc, otherAc, logsArray(0))

    Then("Mozilla accumulator value is checked")
    mozillaAc.value should equal(1)

    Then("Opera accumulator value is checked")
    operaAc.value should equal(0)

    Then("Other accumulator value is checked")
    otherAc.value should equal(0)
  }
}

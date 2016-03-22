package hbase

import model.{Setting, Statistics}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by root on 11/25/15.
 */
case class HbaseHelper() {

  def getAlertedFlag(ip: String, sc: SparkContext): Boolean = {
    val hbaseConf = createHbaseConf()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "settings")
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "f1:alerted")
    hbaseConf.set(TableInputFormat.SCAN_ROW_START, ip)
    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, ip)

    val jobConfig: JobConf = new JobConf(hbaseConf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])

    return sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).map(x => {
      Bytes.toString(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("alerted"))).toBoolean
    }).first()
  }

  def changeAlertedFlag(ip: String, alerted: Boolean, sc: SparkContext): Unit = {
    val hbaseConf = createHbaseConf()

    val jobConfig: JobConf = new JobConf(hbaseConf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "settings")

    val put = new Put(Bytes.toBytes(ip))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("alerted"), Bytes.toBytes(alerted.toString))

    val myTable = new HTable(hbaseConf, "settings")
    myTable.put(put)
  }

  def getSettings(sc: SparkContext): Array[Setting] = {
    val hbaseConfiguration = createHbaseConf()
    hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, "settings")

    val jobConfig: JobConf = new JobConf(hbaseConfiguration, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])

    val settings = sc.newAPIHadoopRDD(
      hbaseConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).map(x => {
      val ip = Bytes.toString(x._2.getRow)
      val limitType = Bytes.toString(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("type"))).toInt
      val limitValue = Bytes.toString(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("value"))).toLong
      val period = Bytes.toString(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("period"))).toLong
      new Setting(ip, limitType, limitValue, period)
    }).collect()

    validateSettingsTable(settings)
    return settings
  }

  //bad approach for window statistics
  def putWindowStatistics(x: Statistics): Unit = {
    val hbaseConf = createHbaseConf()

    val jobConfig: JobConf = new JobConf(hbaseConf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "statistics")

    val put = new Put(Bytes.toBytes(x.hostIp))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("timestamp"), Bytes.toBytes(x.timestamp.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("trafficConsumed"), Bytes.toBytes(x.trafficConsumed))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("averageSpeed"), Bytes.toBytes(x.averageSpeed))

    val myTable = new HTable(hbaseConf, "statistics")
    myTable.put(put)
  }

  def putStatistics(stats: DStream[Statistics]): Unit = {
    val hbaseConf = createHbaseConf()

    val jobConfig: JobConf = new JobConf(hbaseConf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "statistics")

    stats.foreachRDD(x => {
      x.map(x => {
        convertToPut(x)
      }).saveAsHadoopDataset(jobConfig)
    })
  }

  private def convertToPut(x: Statistics): (ImmutableBytesWritable, Put) = {
    val put = new Put(Bytes.toBytes(x.hostIp))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("timestamp"), Bytes.toBytes(x.timestamp.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("trafficConsumed"), Bytes.toBytes(x.trafficConsumed.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("averageSpeed"), Bytes.toBytes(x.averageSpeed.toString))
    return (new ImmutableBytesWritable(Bytes.toBytes(x.hostIp)), put)
  }

  private def createHbaseConf(): Configuration = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.master", "localhost:60000")
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    return hbaseConf
  }

  private def validateSettingsTable(settings: Array[Setting]): Unit = {
    var hasDefaultLimit = false
    var hasDefaultThreshold = false

    settings.map(x => {
      if (x.hostIp.equals("NULL_1")) {
        hasDefaultThreshold = true
      }

      if (x.equals("NULL_2")) {
        hasDefaultLimit = true
      }
    })

    if (!hasDefaultThreshold && !hasDefaultLimit) {
      throw new Exception("Error while validating settings table! HasDefaultThreshold = "
        + hasDefaultThreshold + " ,hasDefaultLimit = "
        + hasDefaultLimit)
    }
  }
}

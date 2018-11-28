package com.hduser.sink
import com.hduser.configuration.`type`.SinkType
import com.hduser.context.DQContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class HiveSink(config:Map[String,Any],metricName:String,timeStamp:Long) extends Sink {
  override def available(): Boolean = hiveTableName.nonEmpty

  override def start(msg: String): Unit = {
    println(s"${timeStamp} ${metricName} start ${msg}")
  }

  override def finish(): Unit = {
    println(s"${timeStamp} ${metricName} finished")
  }

  override def log(rt: Long, msg: String): Unit = {}

  /**
    * 数据写入到hive
    * @param context
    * @param tableName
    */
  override def sinkRecords(context: DQContext, tableName: String): Unit = {
    if (available) {
      val sql = s"INSERT INTO TABLE ${owner}.${hiveTableName} SELECT * FROM ${tableName}"
      context.sqlContext.sql(sql)
    }
  }

  private val _tableName="table_name"
  private val _owner="owner"
  private val owner:String=if (config.get(_owner).size>0) {
    config.get(_owner).head.toString }else{
    throw new Exception(s"env json文件没有配置hive owner信息")
  }
  private val hiveTableName:String=if (config.get(_tableName).size>0) {
    config.get(_tableName).head.toString
  }else{
    throw new Exception(s"env json文件没有配置hive table信息")
  }
}
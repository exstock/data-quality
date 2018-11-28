package com.hduser.sink
import java.util.Properties

import com.hduser.configuration.`type`.{MySQLSinkType, SinkType}
import com.hduser.context.DQContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class MySQLSink(config:Map[String,Any],metricName:String,timeStamp:Long) extends Sink {


  override def available(): Boolean = {
    init(config)
    url.nonEmpty&&tableName.nonEmpty&&properties.size()>=2
  }

  override def start(msg: String): Unit = {
    println(s"${timeStamp} ${metricName} start ${msg}")
  }

  override def finish(): Unit = {
    println(s"${timeStamp} ${metricName} finished")
  }

  override def log(rt: Long, msg: String): Unit = {}

  /**
    * 数据写入到数据库
    *
    * @param dataFrame
    */
  override def sinkRecords(context:DQContext,tabName:String): Unit = {
    val dataFrame=getDataFrame(context, tabName)
    init(config)
    if (available) {
      val df=dataFrame.get
      try {
        df.write.mode(SaveMode.Append).jdbc(url, tableName, properties)
      }catch {
        case e:Throwable=>{
          println(s"dataFrame 不存在")
          throw e
        }
      }
    }
  }

  private var _initialed=false
  private var properties:Properties=_
  private var tableName:String=_
  private var url:String=_

  /**
    * 初始化数据库连接参数
    * @param config
    */
  def init(config: Map[String, Any]) {
    if (!_initialed) {
      val _url = "url"
      val _username = "username"
      val _password = "password"
      val _tableName = "table_name"
      properties = new Properties()

      url = if (config.get(_url).size>0) {
        config.get(_url).head.toString
      }else {
        throw new Exception(s"env json没有配置mysql的url信息")
      }
      tableName = if (config.get(_tableName).size>0) {
        config.get(_tableName).head.toString
      }else{
        throw new Exception(s"env json没有配置mysql的table信息")
      }

      val username = if (config.get(_username).size>0) {
        config.get(_username).head.toString
      }else{
        throw new Exception(s"env json没有配置mysql的username信息")
      }

      val password = if (config.get(_password).size>0) {
        config.get(_password).head.toString
      }else{
        throw new Exception(s"env json没有配置mysql的password信息")
      }

      properties.put("user", username)
      properties.put("password", password)
    }

    _initialed = true
  }
}

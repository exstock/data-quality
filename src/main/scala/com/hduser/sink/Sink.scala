package com.hduser.sink


import com.hduser.configuration.`type`.SinkType
import com.hduser.context.DQContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

trait Sink {
  val timeStamp:Long
  val metricName:String
  val config:Map[String,Any]

  def available():Boolean

  def start(msg:String):Unit

  def finish():Unit

  def log(rt:Long,msg:String):Unit

  //def sinkRecords(records:RDD[String],name:String):Unit

  //def sinkRecords(dataFrame: Option[DataFrame],sinkType:SinkType):Unit

  def sinkRecords(context:DQContext,tableName:String=""):Unit

  //def sinkRecords[T<:Any](args0:T,args1:T):Unit
  def getDataFrame(context: DQContext, tableName: String): Option[DataFrame] = {
    try {
      val df = context.sqlContext.table(tableName)
      Some(df)
    } catch {
      case e: Throwable => {
        println(s"get data frame fails:${e.getMessage}")
        None
      }
    }
  }
}

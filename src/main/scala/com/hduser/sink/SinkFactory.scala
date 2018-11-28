package com.hduser.sink

import com.hduser.configuration.`type`.{ConsoleSinkType, HiveSinkType, MySQLSinkType}
import com.hduser.configuration.param.SinkParam
import scala.util.{Try,Success}


case class SinkFactory(sinkParams: Seq[SinkParam],metricName:String){
  def getSinks(timeStamp:Long): Seq[Sink] ={
    sinkParams.flatMap{sinkParam=>getSink(timeStamp,sinkParam)}
  }

  def getSink(timeStamp:Long,sinkParam: SinkParam): Option[Sink] ={
    val config=sinkParam.getConfig
    val sinkType=sinkParam.getType
    val sinkTry=sinkType match {
      case ConsoleSinkType=>Try(ConsoleSink(config,metricName,timeStamp))
      case HiveSinkType=>Try(HiveSink(config,metricName,timeStamp))
      case MySQLSinkType=>Try(MySQLSink(config,metricName,timeStamp))
      case _=>throw new Exception(s"sink Type ${sinkType} is not support")
    }

    sinkTry match {
      case Success(sink) if (sink.available())=>Some(sink)
      case _=>None
    }
  }
}

package com.hduser.context

import com.hduser.sink._
import com.hduser.configuration.param.SinkParam
import com.hduser.configuration.`type`.{BatchProcessType, ProcessType, SinkType}
import com.hduser.sink.SinkFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

case class DQContext(contextId: ContextId,
                     metricName:String,
                     sinkParams:Seq[SinkParam],
                     procType:ProcessType)(implicit val sparkSession:SparkSession){

  val getSinkParam=if(sinkParams!=null) sinkParams else Seq[SinkParam]()
  val sqlContext: SQLContext = sparkSession.sqlContext
  val compileTableRegister: CompileTableRegister = CompileTableRegister()
  val runTimeTableRegister: RunTimeTableRegister = RunTimeTableRegister(sqlContext)

  val dataFrameCache: DataFrameCache = DataFrameCache()


  /**
    * 获取sink
    * @return
    */
  def getSink():Seq[Sink]=defaultSink
  def getSink(timesTamp:Long):Seq[Sink]={
    if(timesTamp==contextId.timestamp) getSink()
    else createSink(timesTamp)
  }
  private val sinkFactory=SinkFactory(sinkParams,metricName)
  private val defaultSink:Seq[Sink]=createSink(contextId.timestamp)
  private def createSink(timeStamp:Long): Seq[Sink] ={
    procType match {
      case BatchProcessType=>sinkFactory.getSinks(timeStamp)
      case _=>null
    }
  }


  def cloneDQContext(newContextId: ContextId): DQContext = {
     DQContext(newContextId, metricName, sinkParams, procType)(sparkSession)
  }

  def clean(): Unit = {
    compileTableRegister.unregisterALLTables()
    runTimeTableRegister.unregisterALLTables()

    dataFrameCache.uncacheAllDataFrames()
    dataFrameCache.clearAllTrashDataFrames()

  }
}
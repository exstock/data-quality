package com.hduser.step
import com.hduser.context.DQContext



case class SparkWriteStep(name:String,timeStamp:Long) extends WriteStep {

  override def execute(context: DQContext): Boolean = {
    val sinkParams = context.getSinkParam
    val multiSinks = context.getSink(timeStamp)
    multiSinks.foreach{
      sink=>sink.sinkRecords(context,name)
    }
    true
  }
}

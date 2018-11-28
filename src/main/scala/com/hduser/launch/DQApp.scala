package com.hduser.launch

import com.hduser.configuration.param._
import scala.util.Try


trait DQApp extends Serializable {
  val envParam: EnvConfig
  val dqParam: DQConfig

  def init(): Try[_]

  def run(): Try[_]

  def close(): Try[_]

  /**
    * application will exit if it fails in run phase.
    * if retryable is true, the exception will be threw to spark env,
    * and enable retry strategy of spark application
    */
  def retryable: Boolean

  protected def getMeasureTime: Long = {
    dqParam.getTimestampOpt match {
      case Some(t) if t > 0 => t
      case _ => System.currentTimeMillis()
    }
  }

  protected def getSinkParam: Seq[SinkParam] = {
    val validSinkParam = dqParam.getValidSinkTypes
    envParam.getSinkParams.flatMap {
      sinkParam => {
        if (validSinkParam.contains(sinkParam.getType)) Some(sinkParam) else None
      }
    }
  }
}
package com.hduser.configuration.param

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.commons.lang.StringUtils
import com.hduser.configuration.`type`._

/**
  * environment param
  * @param sparkParam         config of spark environment (must)
  * @param sinkParams         config of sink ways (optional)
  * @param checkpointParams   config of checkpoint locations (required in streaming mode)
  */
@JsonInclude(Include.NON_NULL)
case class EnvConfig(@JsonProperty("spark") private val sparkParam: SparkParam,
                     @JsonProperty("sinks") private val sinkParams: List[SinkParam]
                   ) extends Param {
  def getSparkParam: SparkParam = sparkParam
  def getSinkParams: Seq[SinkParam] = if (sinkParams != null) sinkParams else Nil

  def validate(): Unit = {
    assert((sparkParam != null), "spark param should not be null")
    sparkParam.validate
    getSinkParams.foreach(_.validate)
  }
}

/**
  * spark param
  * @param logLevel         log level of spark application (optional)
  * @param cpDir            checkpoint directory for spark streaming (required in streaming mode)
  * @param batchInterval    batch interval for spark streaming (required in streaming mode)
  * @param processInterval  process interval for streaming dq calculation (required in streaming mode)
  * @param config           extra config for spark environment (optional)
  * @param initClear        clear checkpoint directory or not when initial (optional)
  */
@JsonInclude(Include.NON_NULL)
case class SparkParam( @JsonProperty("log.level") private val logLevel: String,
                       @JsonProperty("checkpoint.dir") private val cpDir: String,
                       @JsonProperty("batch.interval") private val batchInterval: String,
                       @JsonProperty("process.interval") private val processInterval: String,
                       @JsonProperty("config") private val config: Map[String, String],
                       @JsonProperty("init.clear") private val initClear: Boolean
                     ) extends Param {
  def getLogLevel: String = if (logLevel != null) logLevel else "WARN"
  def getCpDir: String = if (cpDir != null) cpDir else ""
  def getBatchInterval: String = if (batchInterval != null) batchInterval else ""
  def getProcessInterval: String = if (processInterval != null) processInterval else ""
  def getConfig: Map[String, String] = if (config != null) config else Map[String, String]()
  def needInitClear: Boolean = if (initClear) initClear else false

  def validate(): Unit = {
//    assert(StringUtils.isNotBlank(cpDir), "checkpoint.dir should not be empty")
//    assert(TimeUtil.milliseconds(getBatchInterval).nonEmpty, "batch.interval should be valid time string")
//    assert(TimeUtil.milliseconds(getProcessInterval).nonEmpty, "process.interval should be valid time string")
  }
}

/**
  * sink param
  * @param sinkType       sink type, e.g.: log, hdfs, http, mongo (must)
  * @param config         config of sink way (must)
  */
@JsonInclude(Include.NON_NULL)
case class SinkParam(@JsonProperty("type") private val sinkType: String,
                     @JsonProperty("config") private val config: Map[String, Any]
                    ) extends Param {
  def getType: SinkType = SinkType(sinkType)
  def getConfig: Map[String, Any] = if (config != null) config else Map[String, Any]()

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(sinkType), "sink type should not be empty")
  }
}


package com.hduser.configuration.param

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.commons.lang.StringUtils
import com.hduser.configuration.`type`._

/**
  * dq param
  * @param name           name of dq measurement (must)
  * @param timestamp      default timestamp of measure in batch mode (optional)
  * @param procType       batch mode or streaming mode (must)
  * @param evaluateRule   dq measurement (must)
  * @param sinks          sink types (optional, by default will be elasticsearch)
  */
@JsonInclude(Include.NON_NULL)
case class DQConfig(@JsonProperty("name") private val name: String,
                    @JsonProperty("timestamp") private val timestamp: Long,
                    @JsonProperty("process.type") private val procType: String,
                    @JsonProperty("evaluate.rule") private val evaluateRule: EvaluateRuleParam,
                    @JsonProperty("sinks") private val sinks: List[String]
                  ) extends Param {
  def getName: String = name
  def getTimestampOpt: Option[Long] = if (timestamp != 0) Some(timestamp) else None
  def getProcType: String = procType
  def getEvaluateRule: EvaluateRuleParam = evaluateRule
  def getValidSinkTypes: Seq[SinkType] = SinkType.validSinkTypes(if (sinks != null) sinks else Nil)

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(name), "dq config name should not be blank")
    assert(StringUtils.isNotBlank(procType), "process.type should not be blank")
    assert((evaluateRule != null), "evaluate.rule should not be null")
    evaluateRule.validate
  }
}


/**
  * evaluate rule param
  * @param rules      rules to define dq measurement (optional)
  */
@JsonInclude(Include.NON_NULL)
case class EvaluateRuleParam( @JsonProperty("rules") private val rules: List[RuleParam]
                            ) extends Param {
  def getRules: Seq[RuleParam] = if (rules != null) rules else Nil

  def validate(): Unit = {
    getRules.foreach(_.validate)
  }
}

/**
  * rule param
  * @param dslType    dsl type of this rule (must)
  * @param dqType     dq type of this rule (must if dsl type is "griffin-dsl")
  * @param sparkTmpTable must
  * @param threshold optional
  * @param rule       rule to define dq step calculation (must)
  * @param details    detail config of rule (optional)
  * @param cache      cache the result for multiple usage (optional, valid for "spark-sql" and "df-ops" mode)
  */
@JsonInclude(Include.NON_NULL)
case class RuleParam(@JsonProperty("dsl.type") private val dslType: String,
                     @JsonProperty("dq.type") private val dqType: String,
                     @JsonProperty("spark.temp.table") private val sparkTmpTable: String,
                     @JsonProperty("threshold") private val threshold:String,
                     @JsonProperty("rule") private val rule: Map[String,Any],
                     @JsonProperty("details") private val details: Map[String, Any],
                     @JsonProperty("cache") private val cache: Boolean
                    ) extends Param {
  def getDslType: DslType = if (dslType != null) DslType(dslType) else DslType("")
  def getDqType: DqType = if (dqType != null) DqType(dqType) else DqType("")
  def getCache: Boolean = if (cache) cache else false
  def getThreshold(tempThreshold:String="0.05"):String=if (threshold!=null) threshold else tempThreshold

  def getSparkTmpTable(tempName: String = ""): String = if (sparkTmpTable != null) sparkTmpTable else tempName
  def getRule:Map[String,Any] = if (rule != null) rule else Map[String,Any]()
  def getDetails: Map[String, Any] = if (details != null) details else Map[String, Any]()


  def validate(): Unit = {
    assert(!(getDslType.equals(PaDslType) && getDqType.equals(UnknownType)),
      "unknown dq type for pa dsl")
  }
}
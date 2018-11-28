package com.hduser.step.builder
import com.hduser.configuration.param._
import com.hduser.configuration.`type`._
import com.hduser.context.DQContext
import com.hduser.step.DQStep

trait DQStepBuilder extends Serializable{
  type ParamType<:Param

  def buildDQStep(context:DQContext,param:ParamType):Option[DQStep]

}

object DQStepBuilder{
  //原表
  val _source="source"
  //目标表
  val _targe="target"

  def buildStepOptByRuleParam(context: DQContext,
                              ruleParam: RuleParam):Option[DQStep]={

    val dslType=ruleParam.getDslType

    //将json中配置的表注册到内存中(此处假设源表跟目标表均只是配置了一个)
    val sourceTableName=ruleParam.getDetails.get(_source)
    val targetTableName=ruleParam.getDetails.get(_targe)

    if (sourceTableName.size>0)
      context.runTimeTableRegister.registerTable(sourceTableName.head.toString)
    else
      throw new Exception(s"dq.json文件没有配置source")

    if(targetTableName.size>0)
      context.runTimeTableRegister.registerTable(targetTableName.head.toString)

    val dqStepOpt = getRuleParamStepBuilder(dslType).flatMap(_.buildDQStep(context, ruleParam))

    dqStepOpt
  }

  private def getRuleParamStepBuilder(dslType: DslType): Option[RuleParamStepBuilder] = {
    dslType match {
      case SparkSqlType => None
      case DataFrameOpsType =>None
      case PaDslType => Some(PaDslDQStepBuilder())
      case _ => None
    }
  }

}
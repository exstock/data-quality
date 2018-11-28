package com.hduser.step.builder
import com.hduser.context.DQContext
import com.hduser.step.{DQStep, SeqDQStep}
import com.hduser.configuration.param._

trait RuleParamStepBuilder extends DQStepBuilder{
  type ParamType = RuleParam

  def buildDQStep(context: DQContext, param: ParamType): Option[DQStep]={
    val steps=buildSteps(context,param)
    if(steps.size>1) Some(SeqDQStep(steps))
    else if(steps.size==1)steps.headOption
    else None
  }

  def buildSteps(context: DQContext, ruleParam: RuleParam): Seq[DQStep]
}

package com.hduser.job

import com.hduser.configuration.param.EvaluateRuleParam
import com.hduser.context.DQContext
import com.hduser.configuration.param.RuleParam
import com.hduser.step.builder._

/**
  * build dq job based on configuration
  */
object DQJobBuilder {

 def buildDQJob(context:DQContext,evaluateRuleParam: EvaluateRuleParam):DQJob={
  val ruleParams=evaluateRuleParam.getRules
  buildDQJob(context, ruleParams)
 }

 def buildDQJob(context:DQContext, ruleParams:Seq[RuleParam]):DQJob={
  val ruleSteps=ruleParams.flatMap{
   ruleParam=>DQStepBuilder.buildStepOptByRuleParam(context,ruleParam)
  }

  DQJob(ruleSteps)
 }
}

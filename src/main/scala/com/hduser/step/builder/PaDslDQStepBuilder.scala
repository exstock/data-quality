package com.hduser.step.builder
import com.hduser.configuration.param.RuleParam
import com.hduser.context.DQContext
import com.hduser.step.DQStep
import com.hduser.step.Expr2DQSteps._

case class PaDslDQStepBuilder() extends RuleParamStepBuilder {
  override def buildSteps(context: DQContext, ruleParam: RuleParam): Seq[DQStep] = {
    try{
      val expr2DQSteps = Expr2DQSteps(context, ruleParam)
      expr2DQSteps.getDQStep()
    }catch{
      case e:Throwable=>println(s"generate rule plan fails: ${e.getMessage}")
      Nil
    }
  }
}

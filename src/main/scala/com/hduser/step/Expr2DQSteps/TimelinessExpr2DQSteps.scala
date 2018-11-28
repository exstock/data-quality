package com.hduser.step.Expr2DQSteps
import com.hduser.configuration.param.RuleParam
import com.hduser.step.DQStep
import com.hduser.context.DQContext

case class TimelinessExpr2DQSteps(context:DQContext,
                                  ruleParam:RuleParam
                                 ) extends Expr2DQSteps{

  private object TimelinessKeys{
    val _source="source"
  }

  import TimelinessKeys._

  override def getDQStep(): Seq[DQStep] = {
    Nil
  }
}

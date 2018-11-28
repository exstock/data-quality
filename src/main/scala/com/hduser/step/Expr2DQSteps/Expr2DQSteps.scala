package com.hduser.step.Expr2DQSteps

import com.hduser.step.DQStep
import com.hduser.configuration.param._
import com.hduser.configuration.`type`._
import com.hduser.context.DQContext
import com.hduser.step.Expr2DQSteps._

trait Expr2DQSteps extends Serializable{
  protected val emptyDQstep=Seq[DQStep]()

  protected val emptyMap=Map[String,Any]()

  def getDQStep():Seq[DQStep]
}

object Expr2DQSteps{
  private val emptyDQstep=new Expr2DQSteps(){
    override def getDQStep(): Seq[DQStep] = emptyDQstep
  }


  def apply(context:DQContext,ruleParam:RuleParam):Expr2DQSteps={
    ruleParam.getDqType match {
      case AccuracyType=>AccuracyExpr2DQSteps(context,ruleParam)
      case ProfilingType=>ProfilingExpr2DQSteps(context,ruleParam)
      case UniquenessType=>UniquenessExpr2DQSteps(context,ruleParam)
      case TimelinessType=>TimelinessExpr2DQSteps(context,ruleParam)
      case CompletenessType=>CompletenessExpr2DQSteps(context,ruleParam)
      case NullnessType=>NullnessExpr2DQSteps(context,ruleParam)
      case RegexnessType=>RegexnessExpr2DQSteps(context,ruleParam)
      case _=>emptyDQstep
    }
  }
}

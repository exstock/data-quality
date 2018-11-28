package com.hduser.step
import com.hduser.context.DQContext

case class SeqDQStep(dqSteps: Seq[DQStep]) extends DQStep {
  val name: String = ""
  val rule: String = ""
  val details: Map[String, Any] = Map()

  override def execute(context: DQContext): Boolean = {
    dqSteps.foldLeft(true){
      (ret,dqStep)=>ret&&dqStep.execute(context)
    }
  }

  override def getNames(): Seq[String] = {
    dqSteps.foldLeft(Nil:Seq[String]){
      (ret,dqStep)=>ret++dqStep.getNames()
    }
  }
}

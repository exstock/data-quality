package com.hduser.job

import com.hduser.context.DQContext
import com.hduser.step.DQStep

case class DQJob (dqSteps:Seq[DQStep]){
 def execute(context:DQContext): Unit ={
  dqSteps.foldLeft(true){
   (ret,dqStep)=>ret&&dqStep.execute(context)
  }
 }
}

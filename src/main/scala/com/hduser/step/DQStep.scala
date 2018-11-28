package com.hduser.step
import com.hduser.context.DQContext

trait DQStep {

  val name:String

  def getNames():Seq[String]=name::Nil

  def execute(context:DQContext):Boolean

}

package com.hduser.step

trait ReadStep extends DQStep{
  val rule:String

  val details:Map[String,Any]

  val cache:Boolean

}

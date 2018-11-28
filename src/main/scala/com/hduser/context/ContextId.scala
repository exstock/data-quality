package com.hduser.context

case class ContextId(timestamp:Long,tag:String=""){
  def id:String={
    if (tag.nonEmpty) s"${tag}_${timestamp}" else s"${timestamp}"
  }
}

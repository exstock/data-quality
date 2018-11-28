package com.hduser.configuration.reader

import com.hduser.configuration.param.Param

import scala.reflect.{ClassTag, classTag}
import scala.util.Try

trait ParamReader extends Serializable{
  def readConfig[T<:Param](implicit m:ClassTag[T]):Try[T]

  protected def validate[T<:Param](param:T):T={
    param.validate()
    param
  }
}

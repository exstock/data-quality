package com.hduser.configuration.reader
import com.hduser.configuration.param.Param
import com.hduser.utils.JsonUtils

import scala.reflect.ClassTag
import scala.util.Try

class ParamJsonReader(jsonString:String) extends ParamReader {
  override def readConfig[T <: Param](implicit m: ClassTag[T]): Try[T] = {
    Try{
      val param=JsonUtils.fromJson[T](jsonString)
      validate(param)
    }
  }
}

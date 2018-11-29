package com.hduser.configuration.reader
import java.io.FileInputStream

import com.hduser.configuration.param.Param
import com.hduser.utils.{HdfsUtils, JsonUtils}

import scala.reflect.ClassTag
import scala.util.Try

class ParamFileReader(filePath:String) extends ParamReader {
  override def readConfig[T <: Param](implicit m: ClassTag[T]): Try[T] = {
    Try{
      if (filePath.startsWith("hdfs:")){
        val source =HdfsUtils.openFile(filePath)
        val param=JsonUtils.fromJson(source)
        validate(param)

      }else{
        val param=JsonUtils.fromJson(new FileInputStream(filePath))
        validate(param)

      }
    }
  }
}

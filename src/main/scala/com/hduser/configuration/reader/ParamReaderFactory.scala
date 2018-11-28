package com.hduser.configuration.reader
import com.hduser.utils.JsonUtils

object ParamReaderFactory {
  val json:String="json"
  val file:String="file"

  private def paramStrType(str:String):String={
    try{
      JsonUtils.toAnyMap(str)
      json
    }catch{
      case e:Throwable=>file
    }
  }
  def getParamReader(pathOrJson:String):ParamReader={
    val strType=paramStrType(pathOrJson)
    if (file.equals(strType)){
      new ParamFileReader(pathOrJson)
    }else{
      new ParamJsonReader(pathOrJson)
    }
  }
}

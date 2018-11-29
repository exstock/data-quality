package com.hduser

import java.util.concurrent.{ExecutorService, Executors}

import Thread.TaskExecutors
import com.hduser.utils.HdfsUtils

object Application{

  //获取hdfs上存放的param文件
  def getParamFiles(path:String):Iterable[String]={
    val urlOpt=HdfsUtils.listSubFiles(path)
    urlOpt
  }

  def main(args: Array[String]): Unit = {
    val threadPool:ExecutorService=Executors.newFixedThreadPool(5)

    val dqParamList=getParamFiles("/dq-conf")
    val envParamList=getParamFiles("/env-conf")

    if (dqParamList.size<1)
      throw new Exception(s"hdfs目录dq-conf下面没有配置dq.json文件")

    if(envParamList.size<1)
      throw new Exception(s"hdfs目录env-conf下面没有配置env.json文件")

    val envParamFile=envParamList.headOption.get

    try {
      dqParamList.foreach {
        dqParamFile => {
          var runnable=new TaskExecutors(envParamFile, dqParamFile)
          threadPool.execute(runnable)
        }
      }
    }catch{
      case e:Throwable=>{
        println(s"程序异常，信息：${e.getMessage}")
        throw e
      }
    }finally{
      threadPool.shutdown()
    }

  }



}
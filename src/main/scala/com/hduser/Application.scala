package com.hduser

import scala.util.Success
import scala.util.Failure
import scala.util.Try
import scala.reflect._
import com.hduser.configuration.param.{DQConfig, EnvConfig, PaConfig, Param}
import com.hduser.configuration.`type`._
import com.hduser.configuration.reader.ParamReaderFactory
import com.hduser.launch.DQApp
import com.hduser.launch.BatchDQApp

object Application{

  private def readParamFile[T<:Param](file:String)(implicit m:ClassTag[T]):Try[T]={
    val paramReader=ParamReaderFactory.getParamReader(file)
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {
  }

  private def shutdown(): Unit = {
  }

  def main(args: Array[String]): Unit = {


    if (args.length<2){
      println("参数数量少于2")
      sys.exit(-1)
    }

    val envParamFile=args(0)
    val dqParamFile=args(1)

/*
    val envParamFile="D:\\json\\env.json"
    val dqParamFile="D:\\json\\nullness_dq.json"
*/

    val envParam=readParamFile[EnvConfig](envParamFile) match {
      case Success(p)=>p
      case Failure(exception)=>{
        throw new Exception(s"${envParamFile}配置文件读取失败")
      }
    }

    val dqParam=readParamFile[DQConfig](dqParamFile) match {
      case Success(p)=>p
      case Failure(exception)=>{
        throw new Exception(s"${dqParamFile}配置文件读取失败")
      }
    }


    val allParam:PaConfig=PaConfig(envParam,dqParam)

    val procType=ProcessType(allParam.getDqConfig.getProcType)
    val dqApp:DQApp=procType match {
      case BatchProcessType=>BatchDQApp(allParam)
      case _=>{
        throw new Exception(s"未知处理类型:${procType}")
      }
    }

    startup()
    //init
    dqApp.init() match {
      case Success(_)=>println(s"process init success")
      case Failure(_)=>{
        shutdown()
        throw new Exception(s"process init failed")
      }
    }

    //run
    dqApp.run() match {
      case Success(_)=>println(s"process run success")
      case Failure(ex)=>{
        println(s"process run error:${ex.getMessage}")
        if (dqApp.retryable) {
          throw ex
        }
        else {
          shutdown()
          sys.exit(-1)
        }
      }
    }
    //close
    dqApp.close() match {
      case Success(_)=>println(s"process close success")
      case Failure(ex)=>{
        shutdown()
        throw new Exception(s"process close error:${ex.getMessage}")
      }
    }

    shutdown()
  }

}

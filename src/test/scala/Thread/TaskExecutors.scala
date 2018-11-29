package Thread

import com.hduser.configuration.`type`.{BatchProcessType, ProcessType}
import com.hduser.configuration.param.{DQConfig, EnvConfig, PaConfig, Param}
import com.hduser.configuration.reader.ParamReaderFactory
import com.hduser.launch.{BatchDQApp, DQApp}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class TaskExecutors(envParamFile:String,dqParamFile:String) extends Runnable{

  private def readParamFile[T<:Param](file:String)(implicit m:ClassTag[T]):Try[T]={
    val paramReader=ParamReaderFactory.getParamReader(file)
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {
  }

  private def shutdown(): Unit = {
  }


  override def run(): Unit = {

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

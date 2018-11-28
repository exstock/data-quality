package com.hduser.launch
import com.hduser.configuration.param.{DQConfig, EnvConfig, PaConfig}
import com.hduser.context.ContextId
import com.hduser.context.DQContext
import com.hduser.job.DQJobBuilder
import com.hduser.udf.UDFAgent
import com.hduser.configuration.`type`.ProcessType

import scala.util.Try
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}



case class BatchDQApp(allParam:PaConfig) extends DQApp {
  override val envParam: EnvConfig = allParam.getEnvConfig
  override val dqParam: DQConfig = allParam.getDqConfig

  val sparkParam=envParam.getSparkParam
  val metricName=dqParam.getName
  val sinkParams=getSinkParam

  val procType=ProcessType(dqParam.getProcType)

  var sqlContext:SQLContext=_
  var sparkSession:SparkSession=_
  override def retryable: Boolean = false


  override def init(): Try[_] = Try{
    //val conf=new SparkConf().setAppName(metricName).setMaster("local")
    val conf=new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.getConfig)
    conf.set("spark.sql.crossJoin.enabled", "true")

    //sparkSession=SparkSession.builder().config(conf).getOrCreate()
    sparkSession=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    sparkSession.sparkContext.setLogLevel(sparkParam.getLogLevel)
    sqlContext=sparkSession.sqlContext

    //自定义函数注册到spark
    UDFAgent.register(sqlContext)
  }

  override def run(): Try[_] = Try{

    val measureTime=getMeasureTime
    val contextId=ContextId(measureTime)

    // create dq context
    val dqContext:DQContext= DQContext(contextId, metricName, sinkParams, procType)(sparkSession)
    // build job
    val dqJob = DQJobBuilder.buildDQJob(dqContext, dqParam.getEvaluateRule)

    // dq job execute
    dqJob.execute(dqContext)

    // clean context
    dqContext.clean()
  }

  override def close(): Try[_] = Try{
    sparkSession.close()
    sparkSession.stop()
  }

}

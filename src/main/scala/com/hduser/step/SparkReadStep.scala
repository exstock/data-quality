package com.hduser.step

import org.apache.spark.sql.SQLContext
import com.hduser.context.DQContext

case class SparkReadStep(name:String,
                         rule:String,
                         details:Map[String, Any],
                         cache: Boolean=false) extends ReadStep {

  override def execute(context: DQContext): Boolean = {
    val sqlContext:SQLContext=context.sqlContext

    try{
      val df=sqlContext.sql(rule)
      if(cache) context.dataFrameCache.cacheDataFrame(name, df)
        context.runTimeTableRegister.registerTable(name, df)
      true
    }catch{
      case e:Throwable=>{
        println(s"run spark sql ${rule} error:${e.getMessage}")
        false
      }
    }

  }
}

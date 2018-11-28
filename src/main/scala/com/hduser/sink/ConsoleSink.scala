package com.hduser.sink
import com.hduser.configuration.`type`.{ConsoleSinkType, SinkType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.hduser.context.DQContext

import scala.reflect.ClassTag

case class ConsoleSink(config:Map[String,Any],metricName:String,timeStamp:Long) extends Sink {

  override def available(): Boolean = showRowNum.nonEmpty

  override def start(msg: String): Unit = {
    println(s"${timeStamp} ${metricName} start ${msg}")
  }

  override def finish(): Unit = {
    println(s"${timeStamp} ${metricName} finished")
  }

  override def log(rt: Long, msg: String): Unit = {}

  /**
    * 将结果打印输出到控制台
    *
    * @param dataFrame
    */
  override def sinkRecords(context: DQContext,tableName: String): Unit = {
    val dataFrame=getDataFrame(context, tableName)
    if(available) {
      //如果json配置的是all或者没有配置
      if (showRowNum.equalsIgnoreCase("all"))
        dataFrame.foreach{
          df=>df.show()
        }
      //如果json配置的不是all是数字
      else dataFrame.foreach {
        df => try{
          df.show(showRowNum.toInt)
        }catch {
          //如果json配置的不是all页不是数字
          case e:Throwable=>df.show(10)
        }
      }
    }
    else dataFrame.foreach{
      df=>df.show()
    }
  }

  private val _showRowNum="show_row_num"
  private val showRowNum=config.get(_showRowNum).getOrElse("all").toString
}
package com.hduser.context

import scala.collection.mutable.{MutableList, Map => MutableMap}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class DataFrameCache() {
  val dataFrames:MutableMap[String,DataFrame]=MutableMap()

  val trashDataFrames:MutableList[DataFrame]=MutableList()

  private def trashDataFrame(df:DataFrame):Unit={
    trashDataFrames+=df
  }

  private def trashDataFrames(df:Seq[DataFrame]):Unit={
    trashDataFrames++=df
  }

  def cacheDataFrame(name:String,df:DataFrame):Unit={
    dataFrames.get(name) match {
      case Some(odf)=> {
        trashDataFrame(odf)
        dataFrames+=(name->odf)
        df.cache()
        println("cache after replace old df")
      }
      case _=>{
        dataFrames+=(name->df)
        df.cache()
        println(s"cache after replace no old df")
      }
      }
    }

    def uncacheDataFrame(name:String):Unit={
      dataFrames.get(name).foreach{
        df=>trashDataFrame(df)
      }
      dataFrames.remove(name)
    }

  def uncacheAllDataFrames():Unit={
    trashDataFrames(dataFrames.values.toSeq)
    dataFrames.clear
  }

  def clearAllTrashDataFrames(): Unit = {
    trashDataFrames.foreach(_.unpersist)
    trashDataFrames.clear
  }

}

package com.hduser.context

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.{Set => MutableSet}


trait TableRegister extends Serializable{
  protected val tables:MutableSet[String]=MutableSet()

  def registerTable(name:String):Unit={
    tables+=name
  }

  def existsTable(name:String):Boolean={
    tables.exists(_.equals(name))
  }

  def unregisterTable(name:String):Unit={
    if (existsTable(name)) tables-=name
  }

  def unregisterALLTables():Unit={
    tables.clear()
  }

  def getTables():Set[String]={
    tables.toSet
  }


}

/**
  * register table name when building dq job
  */
 case class CompileTableRegister() extends TableRegister{}


/**
  * register table name and create temp view during calculation
  */

case class RunTimeTableRegister(sqlContext: SQLContext) extends TableRegister{
  def registerTable(name:String,df:DataFrame):Unit={
    registerTable(name)
    df.createOrReplaceTempView(name)
  }

  override def unregisterTable(name:String):Unit={
    if(existsTable(name)) {
      sqlContext.dropTempTable(name)
      tables -= name
    }
  }

  override def unregisterALLTables():Unit={
    val allTables=getTables
    allTables.foreach{
      tables=>sqlContext.dropTempTable(tables)
    }
    tables.clear()
  }
}
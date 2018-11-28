package com.hduser.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object UDFAgent {
 def register(sqlContext: SQLContext):Unit={
  UDFs.register(sqlContext)
  UDFAggs.register(sqlContext)
 }
}

object UDFs{

 def register(sqlContext: SQLContext): Unit ={
  /*两种写法都行，但是用udf的register方法注册的函数只能在sql()中调用，对DataFrame API是不可见的
    sqlContext.sql("SELECT amount, indexOf(date, time, tz) from df").take(2)//true
    df.select($"customer_id", indexOf($"date", $"time", $"tz"), $"amount").take(2) // fails
  */
   sqlContext.udf.register("indexOf",indexOf(_:Seq[String],_:String))
   sqlContext.udf.register("matches",matches _)
   sqlContext.udf.register("regexReplace",regexReplace _)
 }

 /**
   * 返回数组中某个元素的索引下标
   * @param arr
   * @param v
   * @return
   */
 private def indexOf(arr:Seq[String],item:String): Int ={
  arr.indexOf(item)
 }

 /**
   * 给定的字符串是否跟正则表达式匹配
   * @param str
   * @param regex
   * @return
   */
 private def matches(str:String,regex:String):Boolean={
  str.matches(regex)
 }

 /**
   * 正则替换字符
   * @param str
   * @param regex
   * @param replacement
   * @return
   */
 private def regexReplace(str:String,regex:String,replacement:String):String={
  str.replaceAll(regex,replacement)
 }
}

object UDFAggs{
 def register(sqlContext: SQLContext):Unit={

 }
}
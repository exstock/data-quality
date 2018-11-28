package com.hduser.configuration.`type`

import scala.util.matching.Regex
import scala.collection.immutable.List

sealed trait DslType{
  val idPattern:Regex
  val desc:String
}

object DslType{
  private val dslTypes:List[DslType]=List(
    SparkSqlType,DataFrameOpsType,PaDslType
  )

  def apply(pattern:String): DslType =dslTypes.find(
    dslType=>if (dslType.idPattern.pattern.matcher(pattern).matches()) true else false
  ).getOrElse(PaDslType)

  def unapply(pattern: DslType): Option[String] = Some(pattern.desc)
}
/**
  * spark-sql: rule defined in "SPARK-SQL" directly
  */
case object SparkSqlType extends DslType{
  val idPattern:Regex="^(?i)spark-?sql$".r
  val desc:String="spark-sql"
}

/**
  * df-ops: data frame operations rule, support some pre-defined data frame ops
  */
case object DataFrameOpsType extends DslType{
  val idPattern:Regex="^(?i)df-?(?:ops|opr|operations)$".r
  val desc:String="df-ops"
}

/**
  * pa-dsl: griffin dsl rule, to define dq measurements easier
  */
case object PaDslType extends DslType{
  val idPattern:Regex="^(?i)pa-?dsl$".r
  val desc:String="pa-dsl"
}





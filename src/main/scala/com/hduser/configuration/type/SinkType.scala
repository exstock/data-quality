package com.hduser.configuration.`type`

import scala.util.matching.Regex

/**
  * sink type
  */
sealed trait SinkType {
  val idPattern: Regex
  val desc: String
}

object SinkType {
  private val sinkTypes: List[SinkType] = List(
    ConsoleSinkType, HiveSinkType,MySQLSinkType, UnknownSinkType
  )

  def apply(pattern: String): SinkType = {
    sinkTypes.find(sinkType =>
    if (sinkType.idPattern.pattern.matcher(pattern).matches()) true else false
    ).getOrElse(UnknownSinkType)
  }

  def unapply(pattern: SinkType): Option[String] = Some(pattern.desc)

  def validSinkTypes(strs: Seq[String]): Seq[SinkType] = {
    val seq = strs.map(s => SinkType(s)).filter(_ != UnknownSinkType).distinct
    if (seq.size > 0) seq else Seq(HiveSinkType)
  }
}

/**
  * console sink, will sink metric in console
  */
 case object ConsoleSinkType extends SinkType {
  val idPattern = "^(?i)console|log$".r
  val desc = "console"
}

/**
  * hive sink ,will sink records in hive
  */
case object HiveSinkType extends SinkType{
  override val idPattern: Regex = "^(?i)hive$".r
  override val desc: String = "hive"
}

/**
  * mysql sink ,will sink records in mysql
  */
case object MySQLSinkType extends SinkType{
  override val idPattern: Regex = "^(?i)mysql$".r
  override val desc: String = "mysql"
}

 case object UnknownSinkType extends SinkType {
  val idPattern = "".r
  val desc = "unknown"
}

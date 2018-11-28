package com.hduser.configuration.`type`

import com.hduser.configuration.`type`.ProcessType.procTypes

import scala.util.matching.Regex

/**
  * process type enum
  */
sealed trait ProcessType {
  val idPattern: Regex
  val desc: String
}

object ProcessType {
  private val procTypes: List[ProcessType] = List(BatchProcessType, StreamingProcessType)
  def apply(pattern: String): ProcessType = {
    procTypes.find(procType =>
      if(procType.idPattern.pattern.matcher(pattern).matches()) true else false
    ).getOrElse(BatchProcessType)
  }
  def unapply(pt: ProcessType): Option[String] = Some(pt.desc)
}

/**
  * process in batch mode
  */
 case object BatchProcessType extends ProcessType {
  val idPattern = """^(?i)batch$""".r
  val desc = "batch"
}

/**
  * process in streaming mode
  */
 case object StreamingProcessType extends ProcessType {
  val idPattern = """^(?i)streaming$""".r
  val desc = "streaming"
}

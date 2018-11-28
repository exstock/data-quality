package com.hduser.configuration.`type`

import scala.util.matching.Regex
import scala.collection.immutable.List
sealed trait DqType{
  val idPattern:Regex
  val desc:String
}

/**
  * accuracy: the match percentage of items between source and target
  * count(source items matched with the ones from target) / count(source)
  * e.g.: source [1, 2, 3, 4, 5], target: [1, 2, 3, 4]
  *       metric will be: { "total": 5, "miss": 1, "matched": 4 }
  *       accuracy is 80%.
  */
case object AccuracyType extends DqType{
  val idPattern:Regex="^(?i)accuracy$".r
  val desc:String="accuracy"
}

/**
  * profiling: the statistic data of data source
  * e.g.: max, min, average, group by count, ...
  */
case object ProfilingType extends DqType{
  val idPattern:Regex="^(?i)profiling$".r
  val desc:String="profiling"
}

/**
  * uniqueness: the uniqueness of data source comparing with itself
  * count(unique items in source) / count(source)
  * e.g.: [1, 2, 3, 3] -> { "unique": 2, "total": 4, "dup-arr": [ "dup": 1, "num": 1 ] }
  * uniqueness indicates the items without any replica of data
  */
case object UniquenessType extends DqType{
  val idPattern:Regex="^(?i)uniqueness$".r
  val desc:String="uniqueness"
}


/**
  * timeliness: the latency of data source with timestamp information
  * e.g.: (receive_time - send_time)
  * timeliness can get the statistic metric of latency, like average, max, min, percentile-value,
  * even more, it can record the items with latency above threshold you configured
  */
case object TimelinessType extends DqType{
  val idPattern:Regex="^(?i)timeliness$".r
  val desc:String="timeliness"
}

/**
  * completeness: the completeness of data source
  * the columns you measure is incomplete if it is null
  */
case object CompletenessType extends DqType{
  val idPattern:Regex="^(?i)completeness$".r
  val desc:String="completeness"
}


case object NullnessType extends DqType{
  val idPattern: Regex = "^(?i)nullness$".r
  val desc: String ="nullness"
}

case object RegexnessType extends DqType{
  val idPattern: Regex = "^(?i)regexness$".r
  val desc: String ="regexness"
}

case object UnknownType extends DqType{
  val idPattern:Regex="^(?i)unknown$".r
  val desc:String="unknown"
}



object DqType{
  private val dqTypes:List[DqType]=List(
    AccuracyType,ProfilingType,UniquenessType,TimelinessType,CompletenessType,NullnessType,RegexnessType,UnknownType
  )

  def apply(pattern:String): DqType ={
    dqTypes.find(dqtype=>
    if(dqtype.idPattern.pattern.matcher(pattern).matches()) true else false
    ).getOrElse(UnknownType)
  }

  def unapply(pattern: DqType): Option[String] =Some(pattern.desc)
}
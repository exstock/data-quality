package com.hduser.step.Expr2DQSteps

import com.hduser.configuration.param.RuleParam
import com.hduser.configuration.`type`.BatchProcessType
import com.hduser.step.{DQStep, SparkReadStep, SparkWriteStep}
import com.hduser.context.DQContext
import com.hduser.configuration.`type`.StreamingProcessType

/**
  * 码值规则校验
  * @param context
  * @param ruleParam
  */
case class AccuracyExpr2DQSteps(context:DQContext,
                                ruleParam:RuleParam) extends Expr2DQSteps {

  private object AccuracyKeys{
    val _source_owner="source_owner"
    val _source = "source"
    val _target_owner="target_owner"
    val _target = "target"
    val _miss = "miss"
    val _total = "total"
  }
  import AccuracyKeys._

  var sourceTableName:String=_
  var sourceOwner:String=_
  var targetTableName:String=_
  var targetOwner:String=_

  override def getDQStep(): Seq[DQStep] = {
    val details=ruleParam.getDetails

    if (details.get(_source).size>0){
      sourceTableName=details.get(_source).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置源表")

    if(details.get(_target).size>0){
      targetTableName=details.get(_target).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置目标表")

    if (details.get(_source_owner).size>0){
      sourceOwner=details.get(_source_owner).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置源表的owner")

    if(details.get(_target_owner).size>0){
      targetOwner=details.get(_target_owner).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json有配置目标表的owner没")

    val procType = context.procType
    val timestamp = context.contextId.timestamp


    val rules=ruleParam.getRule
    var onClauseList=List[String]()
    var column_list=List[String]()

    for (item<- rules){
      val source_column=item._1
      val target_column=item._2
      val condition=s""" ${sourceTableName}.${source_column} = ${targetTableName}.${target_column} """.stripMargin
      onClauseList=onClauseList:+condition
      column_list=column_list:+source_column
    }
    val onClause=onClauseList.mkString(" AND ")


    var whereClauseList=List[String]()
    for (item<-rules) {
      val source_column=item._1
      val target_column=item._2
      val condition=s"""(${sourceTableName}.${source_column} IS NOT NULL AND ${targetTableName}.${target_column} IS NULL)"""
      whereClauseList=whereClauseList:+condition
    }
    val whereClause=whereClauseList.mkString(" AND ")

    if (!context.runTimeTableRegister.existsTable(sourceTableName)) {
      println(s"[${timestamp}] data source ${sourceTableName} not exists")
      Nil
    }
    else{
      // 1. miss record
      val missRecordsTableName = s"${sourceTableName}_missRecords"
      val selClause = s"${sourceTableName}.*"
      val missRecordsSql = if (!context.runTimeTableRegister.existsTable(targetTableName)) {
        println(s"[${timestamp}] data source ${targetTableName} not exists")
        s"SELECT ${selClause} FROM ${sourceOwner}.${sourceTableName}"
      }else{
        s"SELECT ${selClause} FROM ${sourceOwner}.${sourceTableName} AS ${sourceTableName} LEFT JOIN ${targetOwner}.${targetTableName} AS ${targetTableName} ON ${onClause} where ${whereClause}"
      }

      val missRecordsReadStep=SparkReadStep(missRecordsTableName,missRecordsSql,emptyMap)

      //2、miss_count
      val missCountTableName = s"${sourceTableName}_missCount"
      val missColName=details.get(_miss).getOrElse("MISS_COUNT")
      val missCountSql=procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS ${missColName} FROM ${missRecordsTableName}"
        case _ => throw new Exception(s"流式处理暂时没有实现")
      }
      val missCountReadStep=SparkReadStep(missCountTableName,missCountSql,emptyMap)

      //3、total_count
      val totalCountTableName = s"${sourceTableName}_totalCount"
      val totalColName=details.get(_total).getOrElse("TOTAL_COUNT")
      val totalCountSql=procType match {
        case BatchProcessType=>s"SELECT COUNT(*) AS ${totalColName} FROM ${sourceOwner}.${sourceTableName}"
        case _=>throw new Exception(s"流式处理暂时没有实现")
      }
      val totalCountReadStep=SparkReadStep(totalCountTableName,totalCountSql,emptyMap)

      //4、accuracy metric
      val metricTableName=ruleParam.getSparkTmpTable()          //将sql结果输出到spark临时表
      if (metricTableName.isEmpty){
        throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置spark临时表")
      }
      val columns=column_list.mkString(",")

      val computeTable=s"${sourceTableName}_accuracy"
      val computeSql=procType match {
        case BatchProcessType=>
          s"""
             |SELECT
             |${missCountTableName}.${missColName} AS ${missColName},
             |${totalCountTableName}.${totalColName} AS ${totalColName}
             |FROM ${totalCountTableName} FULL JOIN ${missCountTableName}
           """.stripMargin
        case _=>throw new Exception(s"流式处理逻辑暂时没有实现")
      }
      val computeReadStep=SparkReadStep(computeTable,computeSql,emptyMap)

      //表关联规则校验
      val ruleName=s"table join rule"
      val MetricSql=
        s"""SELECT '${sourceOwner}' AS TABLE_OWNER,
           |'${sourceTableName}' AS TABLE_NAME,
           |'${columns}' AS COLUMN_NAME,
           |COALESCE(${missColName},0) AS COLUMN_VALUE,
           |CURRENT_TIMESTAMP AS INSERT_DATE,
           |'${ruleName}' as DATA_SOURCE
           |FROM ${computeTable}""".stripMargin

      println(MetricSql)
      val accuracyReadStep = SparkReadStep(metricTableName, MetricSql, emptyMap)
      val accuracyWriteStep=SparkWriteStep(metricTableName,timestamp)

      //full step
      val executeRule=missRecordsReadStep :: missCountReadStep :: totalCountReadStep ::computeReadStep::accuracyReadStep :: accuracyWriteStep::Nil

      executeRule
    }
  }
}
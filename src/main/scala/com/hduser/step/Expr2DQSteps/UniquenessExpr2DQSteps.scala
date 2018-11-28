package com.hduser.step.Expr2DQSteps

import com.hduser.configuration.param.RuleParam
import com.hduser.step.{DQStep, SparkReadStep, SparkWriteStep}
import com.hduser.context.DQContext

/**
  * 主键重复性规则校验
  * @param context
  * @param ruleParam
  */
case class UniquenessExpr2DQSteps (context: DQContext,
                                     ruleParam: RuleParam
                                    ) extends Expr2DQSteps {

  private object UniquenessKeys{
    val _source_owner="source_owner"
    val _source = "source"
    val _uniquecnt="unique"
    val _group = "group"
    val _dupcnt="duplicate"
    val _distinct="distinct"
    val _total="total"

  }

  import UniquenessKeys._

  var sourceOwner:String=_
  var sourceTableName:String=_

  override def getDQStep(): Seq[DQStep] = {
    val details=ruleParam.getDetails
    val rules=ruleParam.getRule

    if (details.get(_source).size>0){
      sourceTableName=details.get(_source).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置源表")

    if(details.get(_source_owner).size>0){
      sourceOwner=details.get(_source_owner).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置源表owner")

    val procType = context.procType
    val timestamp = context.contextId.timestamp

    if (!context.runTimeTableRegister.existsTable(sourceTableName)) {
      println(s"[${timestamp}] data source ${sourceTableName} not exists")
      Nil
    } else{

      // 1. 去除重复后的记录
      val distinctTableName = s"${sourceTableName}_distinct"
      //从rule中取出所有的字段
      val sourceColName = rules.map(_._2).map{
        column=>s"${column}"
      }.mkString(",")

      val distinctSql = s"SELECT ${sourceColName} FROM ${sourceOwner}.${sourceTableName} GROUP BY ${sourceColName}"
      val distinctReadStep = SparkReadStep(distinctTableName, distinctSql, emptyMap)

      //2、聚合操作
      val groupTableName = s"${sourceTableName}_group"
      val groupColName=details.get(_group).getOrElse("GROUP_CNT")
      val groupSql = s"SELECT ${sourceColName},(COUNT(*)-1) AS ${groupColName} FROM ${sourceOwner}.${sourceTableName} GROUP BY ${sourceColName}"
      val groupReadStep = SparkReadStep(groupTableName, groupSql, emptyMap)

      //3、唯一记录
      val uniqueRecordTableName=s"${sourceTableName}_uniqueRecord"
      val uniqueRecoreSql={
        s"SELECT * FROM ${groupTableName} WHERE ${groupColName}=0"
      }
      val uniqueRecordReadStep = SparkReadStep(uniqueRecordTableName, uniqueRecoreSql, emptyMap)

      //4、重复记录
      val dupRowCountTableName=s"${sourceTableName}_dupRowCount"
      val dupRowCountSql={
        s"SELECT * FROM ${groupTableName} WHERE ${groupColName}>0"
      }
      val dupRowCountReadStep = SparkReadStep(dupRowCountTableName, dupRowCountSql, emptyMap)

      // 5、 metric 输出原表的重复记录条数、非重复记录条数
      val metricTableName = ruleParam.getSparkTmpTable()
      if (metricTableName.isEmpty){
        throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置spark临时表")
      }
      val uniqueRecordCountColName=details.get(_uniquecnt).getOrElse("UNIQUE_CNT")
      val dupRecordCountColName=details.get(_dupcnt).getOrElse("DUPLICATE_CNT")
      val totalRecordCountColName=details.get(_total).getOrElse("TOTAL_CNT")
      val distRecordCountColName=details.get(_distinct).getOrElse("DISTINCT_CNT")

      val computeTableName=s"${sourceTableName}_compute"
      val computeSql = {
        s"""SELECT '${sourceOwner}.${sourceTableName}' AS TABLE_NAME,
           |C.${totalRecordCountColName},
            |D.${distRecordCountColName},
            |A.${uniqueRecordCountColName},
            |B.${dupRecordCountColName}
            |FROM (SELECT COUNT(1) AS ${uniqueRecordCountColName} FROM ${uniqueRecordTableName}) A
            |FULL JOIN (SELECT COUNT(1) AS ${dupRecordCountColName} FROM ${dupRowCountTableName}) B
            |FULL JOIN (SELECT COUNT(1) AS ${totalRecordCountColName} FROM ${sourceOwner}.${sourceTableName}) C
            |FULL JOIN (SELECT COUNT(1) AS ${distRecordCountColName} FROM ${distinctTableName}) D
         """.stripMargin
      }
      val computeReadStep = SparkReadStep(computeTableName, computeSql, emptyMap)

      //主键重复记录
      val ruleName=s"unique rule"
      val metricSql=
        s"""SELECT '${sourceOwner}' AS TABLE_OWNER,
           |'${sourceTableName}' AS TABLE_NAME,
           |'${sourceColName}' as COLUMN_NAME,
           |${dupRecordCountColName} as COLUMN_VALUE,
           |CURRENT_TIMESTAMP AS INSERT_DATE,
           |'${ruleName}' as DATA_SOURCE
           |FROM ${computeTableName}""".stripMargin

      println(metricSql)
      val metricReadStep = SparkReadStep(metricTableName, metricSql, emptyMap)
      val metricWriteStep=SparkWriteStep(metricTableName,timestamp)

      //full step
      val executeRule=distinctReadStep::groupReadStep::uniqueRecordReadStep::dupRowCountReadStep::computeReadStep::metricReadStep::metricWriteStep::Nil

      executeRule
    }
  }
}

package com.hduser.step.Expr2DQSteps

import com.hduser.configuration.param.RuleParam
import com.hduser.context.DQContext
import com.hduser.step.{DQStep, SparkReadStep, SparkWriteStep}

/**
  * 空值率规则校验
  * @param context
  * @param ruleParam
  */
case class NullnessExpr2DQSteps(context: DQContext,
                                ruleParam: RuleParam) extends Expr2DQSteps{
  private object NullnessKeys{
    val _source_owner="source_owner"
    val _source="source"
    val _null="null"
    val _total="total"
  }
  import NullnessKeys._

  var sourceOwner:String=_
  var sourceTableName:String=_

  override def getDQStep(): Seq[DQStep] = {
    val details=ruleParam.getDetails

    if (details.get(_source).size>0) {
      sourceTableName=details.get(_source).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置源表")

    if(details.get(_source_owner).size>0){
      sourceOwner=details.get(_source_owner).head.toString
    }else
      throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置源表owner")

    val rules=ruleParam.getRule

    val procType = context.procType
    val timestamp = context.contextId.timestamp

    if (!context.runTimeTableRegister.existsTable(sourceTableName)) {
      println(s"[${timestamp}] data source ${sourceTableName} not exists")
      Nil
    } else{
      // 1. 直接生成最终sql进行规制校验
      val metricTableName =ruleParam.getSparkTmpTable()
      if (metricTableName.isEmpty){
        throw new Exception(s"dslType:${ruleParam.getDslType},dqType:${ruleParam.getDqType},json没有配置spark临时表")
      }
      val prefix=details.get(_null).getOrElse("")
      val total=details.get(_total).getOrElse("")

      //内层sql
      val rulesql1=rules.map(_._2).map{
        column=>{
          s"SUM(CASE WHEN ${column} IS NOT NULL THEN 0 ELSE 1 END) AS ${prefix}${column}"
        }
      }.mkString(",")
      //外层sql
      val rulesql2=rules.map(_._2).map{
        column=>{
          s"CASE WHEN ${total}>0 THEN ${prefix}${column}*100/${total} END AS ${prefix}${column}"
        }
      }.mkString(",")

      val computeTableName=s"${sourceTableName}_nullness"
      val computeSql =
        s"""
           |SELECT ${rulesql2} FROM (
           |SELECT COUNT(1) AS ${total},${rulesql1} FROM ${sourceOwner}.${sourceTableName}) A
           |""".stripMargin
      val computeReadStep = SparkReadStep(computeTableName, computeSql, emptyMap)

      //空值率计算
      val ruleNname=s"null rule"
      val metricSql=rules.map(_._2).map{
        column=>
          s"""SELECT '${sourceOwner}' AS TABLE_OWNER,
             |'${sourceTableName}' AS TABLE_NAME,
             |'${column}' AS COLUMN_NAME,
             |CONCAT(ROUND(${prefix}${column},2),'%') AS COLUMN_VALUE,
             |CURRENT_TIMESTAMP AS INSERT_DATE,
             |'${ruleNname}' as DATA_SOURCE
             |FROM ${computeTableName}""".stripMargin
      }.mkString(" UNION ALL ")

      println(metricSql)
      val metricReadStep = SparkReadStep(metricTableName, metricSql, emptyMap)
      val metricWriteStep=SparkWriteStep(metricTableName,timestamp)

      //full step
      val executeRule=computeReadStep::metricReadStep::metricWriteStep::Nil

      executeRule
    }
  }
}
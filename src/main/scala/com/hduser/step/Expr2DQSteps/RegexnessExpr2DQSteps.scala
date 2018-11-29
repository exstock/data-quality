package com.hduser.step.Expr2DQSteps

import com.hduser.configuration.param.RuleParam
import com.hduser.context.DQContext
import com.hduser.step.{DQStep, SparkReadStep, SparkWriteStep}

/**
  * 正则规则校验
  * @param context
  * @param ruleParam
  */
case class RegexnessExpr2DQSteps (context: DQContext,
                                  ruleParam: RuleParam) extends Expr2DQSteps{

  private object RegexnessKeys{
    val _source_owner="source_owner"
    val _source="source"
    val _not_match="not_match"
    val _total="total"
  }

  import RegexnessKeys._

  var sourceOwner:String=_
  var sourceTableName:String=_

  override def getDQStep(): Seq[DQStep] = {
    val details=ruleParam.getDetails

    if (details.get(_source).size>0){
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
      val prefix=details.get(_not_match).getOrElse("UNMATCH_")
      val total=details.get(_total).getOrElse("COUNT")

      var ruleSqlList=List[String]()
      var columnList=List[String]()

      for (item <- rules){
        val str=s"SUM(CASE WHEN ${item._1} RLIKE ${item._2} THEN 0 ELSE 1 END) AS ${prefix}${item._1}"
        ruleSqlList=ruleSqlList:+str
        columnList=columnList:+item._1
      }
      val ruleSql:String=ruleSqlList.mkString(",")
      val computeTableName = s"${sourceTableName}_regexness"
      val computeSql =
        s"""
           |SELECT COUNT(1) AS ${total},${ruleSql}
           |FROM ${sourceOwner}.${sourceTableName}""".stripMargin
      val computeReadStep = SparkReadStep(computeTableName, computeSql, emptyMap)

      //计算得出每个字段匹配不上正则规则的数据
      val ruleName=s"regex rule"
      val metricSql=columnList.map{
        column=>{
          s"""SELECT '${sourceOwner}' AS TABLE_OWNER,
             |'${sourceTableName}' AS TABLE_NAME,
             |'${column}' AS COLUMN_NAME,
             |${prefix}${column} AS COLUMN_VALUE,
             |CURRENT_TIMESTAMP AS INSERT_DATE,
             |'${ruleName}' as DATA_SOURCE
             |FROM ${computeTableName}
           """.stripMargin
        }
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

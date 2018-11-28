package com.hduser.step.Expr2DQSteps
import com.hduser.configuration.param._
import com.hduser.step.{DQStep, SparkReadStep, SparkWriteStep}
import com.hduser.context.DQContext

/**
  * 金额类环比规则校验
  * @param context
  * @param ruleParam
  */
case class ProfilingExpr2DQSteps(context: DQContext,
                                  ruleParam: RuleParam) extends Expr2DQSteps {
  private object ProfilingKeys {
    val _source_owner="source_owner"
    val _source="source"
    val _sum="sum"
  }

  import ProfilingKeys._

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

      val prefix=details.get(_sum).getOrElse("SUM_")

      //外层sql
      var outRuleSqlList=List[String]()
      val threshold=try {
        ruleParam.getThreshold().toFloat
      }catch {
        case e:Throwable=>{
          println(s"输入的阈值不是double型")
         throw e
        }
      }
      for (item <- rules){
        val dateColumn=item._1
        val amtColumn=item._2
        val str=
          s"""
             |CASE WHEN PRE_${prefix}${item._1}>0 AND ${prefix}${item._1}>PRE_${prefix}${item._1}*${threshold} THEN 'Y' ELSE 'N' END AS ${item._1}
           """.stripMargin

        outRuleSqlList=outRuleSqlList:+str
      }
      var outRuleSql:String=outRuleSqlList.mkString(",")


      //内层sql
      var inRuleSqlList=List[String]()
      for (item <- rules){
        val dateColumn=item._1
        val amtColumn=item._2
        val str=
          s"""
             |SUM(CASE WHEN DATE_FORMAT(${dateColumn},'yyyy-MM-dd')=DATE_ADD(CURRENT_DATE,-1) THEN NVL(${amtColumn},0) END) AS ${prefix}${item._1},
             |SUM(CASE WHEN DATE_FORMAT(${dateColumn},'yyyy-MM-dd')=DATE_ADD(CURRENT_DATE,-2) THEN NVL(${amtColumn},0) END) AS PRE_${prefix}${item._1}
           """.stripMargin

        inRuleSqlList=inRuleSqlList:+str
      }
      var inRuleSql:String=inRuleSqlList.mkString(",")

      val computeTableName=s"${sourceTableName}_compute"
      val computeSql =
        s"""
           |SELECT '${sourceOwner}.${sourceTableName}' AS TABLE_NAME,
           |${outRuleSql} FROM (
           |SELECT ${inRuleSql} FROM ${sourceOwner}.${sourceTableName}) A
           |""".stripMargin

      val computeReadStep = SparkReadStep(computeTableName, computeSql, emptyMap)

      //计算金额环比规则
      val ruleName=s"amt rule"
      val metricSql=rules.map{
        item=>{
         s"""SELECT '${sourceOwner}' AS TABLE_OWNER,
            |'${sourceTableName}' AS TABLE_NAME,
            |'${item._1}-${item._2}' AS COLUMN_NAME,
            |${item._1} AS COLUMN_VALUE,
            |CURRENT_TIMESTAMP AS INSERT_DATE,
            |'${ruleName}' as DATA_SOURCE
            |FROM ${computeTableName}""".stripMargin
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
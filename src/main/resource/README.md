数据质量校验主要是从下面的四个方向着手：
1、数据准确性
2、数据完整性
3、数据一致性
4、数据时效性

这个项目主要是从数据准确性及数据完整性做的。数据完整性主要包括：空值、主键唯一、表关联、正则表达式等方面出发，
每个规则一个类，本应该是继承数据完整性抽象类的，但是实际仓促就先开源了。

数据的准确性主要包括金额等环比校验
数据的时效性及数据一致性还没有实现

项目采用的技术方案是：scala+spark sql+mysql,面向对象编程。


程序的思路：
1、规则通过json来配置，程序读取json文件，获取json信息，通过jackjson将json反序列化为对象
json中有复合信息（如value是map类型或者list类型等），这样的信息都是转换为对象

2、获取json中配置的处理类型(目前只是配置批量处理类型),来选择不同的处理逻辑，
批量处理类型，选用批量处理类进行处理，如BatchDQApp

3、BatchDQApp 定义 init、run、close等方法
init中进行UDF函数注册、sparkSession的生成，sqlContext的生成，将这些基本信息封装到DQContext中，将这个对象可以
作为参数传递使用这些基本信息。DQContext做一些spark临时创建、缓存、清除等工作

run中需要构建job，如类DQJobBuilder的方法buildDQJob，job又需要分解为step哪些步骤，
如类DQStepBuilder.buildStepOptByRuleParam 返回Seq[RuleParamStepBuilder],类RuleParamStepBuilde通过buildDQStep
获取了dq的处理类型，如AccuracyExpr2DQSteps，调用getDQStep既可以获得DQStep类型的对象SparkSqlTransformStep，DQStep是抽象类
close方法是关闭sparkSession

4、根据上一步获取的DQJob，执行execute方法，这个方法是调用了DQStep的execute方法，实际执行的是
SparkReadStep中的方法和SparkWriteStep中的execute方法（SparkReadStep和SparkWriteStep是DQStep的子类，已经重写了execute方法）
这个是用来执行规则校验的拼接sql并注册成内存表的


5、调用DQContext中的clean 方法，之前注册的spark临时表，清除缓存等操作。
注意函数注册只是存在于sparksession中，如果这个sparksession关闭，这些注册的函数会消失

6、调用sparkSession的close方法，关闭sparksession


总结：
程序是面向对象设计，采用了工厂模式、结构很清晰
如果后面新增规则，只需要新增DslType、DqType及对应的Dq的处理类型，如AccuracyExpr2DQSteps
如果需要增加json的配置信息，则对应的json映射类需要修改。这个程序功能简单，复杂功能需要进一步开发


--程序的执行
./spark-submit --class com.hduser.Application --master yarn \
--deploy-mode client \
--queue default \
--driver-memory 1g --executor-memory 1g --num-executors 2 \
--driver-class-path /home/hduser/spark-2.2.2/db_driver/mysql-connector-java-5.1.38-bin.jar \
/home/hduser/check_data_quality-1.0-SNAPSHOT.jar /home/hduser/env.json /home/hduser/dq.json
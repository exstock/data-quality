��������У����Ҫ�Ǵ�������ĸ��������֣�
1������׼ȷ��
2������������
3������һ����
4������ʱЧ��

�����Ŀ��Ҫ�Ǵ�����׼ȷ�Լ��������������ġ�������������Ҫ��������ֵ������Ψһ���������������ʽ�ȷ��������
ÿ������һ���࣬��Ӧ���Ǽ̳����������Գ�����ģ�����ʵ�ʲִپ��ȿ�Դ�ˡ�

���ݵ�׼ȷ����Ҫ�������Ȼ���У��
���ݵ�ʱЧ�Լ�����һ���Ի�û��ʵ��

��Ŀ���õļ��������ǣ�scala+spark sql+mysql,��������̡�


�����˼·��
1������ͨ��json�����ã������ȡjson�ļ�����ȡjson��Ϣ��ͨ��jackjson��json�����л�Ϊ����
json���и�����Ϣ����value��map���ͻ���list���͵ȣ�����������Ϣ����ת��Ϊ����

2����ȡjson�����õĴ�������(Ŀǰֻ������������������),��ѡ��ͬ�Ĵ����߼���
�����������ͣ�ѡ��������������д�����BatchDQApp

3��BatchDQApp ���� init��run��close�ȷ���
init�н���UDF����ע�ᡢsparkSession�����ɣ�sqlContext�����ɣ�����Щ������Ϣ��װ��DQContext�У�������������
��Ϊ��������ʹ����Щ������Ϣ��DQContext��һЩspark��ʱ���������桢����ȹ���

run����Ҫ����job������DQJobBuilder�ķ���buildDQJob��job����Ҫ�ֽ�Ϊstep��Щ���裬
����DQStepBuilder.buildStepOptByRuleParam ����Seq[RuleParamStepBuilder],��RuleParamStepBuildeͨ��buildDQStep
��ȡ��dq�Ĵ������ͣ���AccuracyExpr2DQSteps������getDQStep�ȿ��Ի��DQStep���͵Ķ���SparkSqlTransformStep��DQStep�ǳ�����
close�����ǹر�sparkSession

4��������һ����ȡ��DQJob��ִ��execute��������������ǵ�����DQStep��execute������ʵ��ִ�е���
SparkReadStep�еķ�����SparkWriteStep�е�execute������SparkReadStep��SparkWriteStep��DQStep�����࣬�Ѿ���д��execute������
���������ִ�й���У���ƴ��sql��ע����ڴ���


5������DQContext�е�clean ������֮ǰע���spark��ʱ���������Ȳ�����
ע�⺯��ע��ֻ�Ǵ�����sparksession�У�������sparksession�رգ���Щע��ĺ�������ʧ

6������sparkSession��close�������ر�sparksession


�ܽ᣺
���������������ƣ������˹���ģʽ���ṹ������
���������������ֻ��Ҫ����DslType��DqType����Ӧ��Dq�Ĵ������ͣ���AccuracyExpr2DQSteps
�����Ҫ����json��������Ϣ�����Ӧ��jsonӳ������Ҫ�޸ġ���������ܼ򵥣����ӹ�����Ҫ��һ������


--�����ִ��
./spark-submit --class com.hduser.Application --master yarn \
--deploy-mode client \
--queue default \
--driver-memory 1g --executor-memory 1g --num-executors 2 \
--driver-class-path /home/hduser/spark-2.2.2/db_driver/mysql-connector-java-5.1.38-bin.jar \
/home/hduser/check_data_quality-1.0-SNAPSHOT.jar /home/hduser/env.json /home/hduser/dq.json
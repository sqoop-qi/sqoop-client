package com.qi.util;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MListInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.validation.Status;

public class SqoopUtilsTeacher {
	public static final String SQOOP_URL = "http://master:12000/sqoop/";
	public static SqoopClient client = new SqoopClient(SQOOP_URL);
	
	//创建一个link来连接linux上的mysql
	public static void createLinkToLinuxMysql(){
		MLink mysqlLink = client.createLink("generic-jdbc-connector");
		mysqlLink.setName("mysql_on_linux_teacher");
		mysqlLink.setCreationUser("root");
		MLinkConfig linkConfig = mysqlLink.getConnectorLinkConfig();
		describeConfigs(linkConfig.getConfigs());
		linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://master:3306/hive");
		linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		linkConfig.getStringInput("linkConfig.username").setValue("root");
		linkConfig.getStringInput("linkConfig.password").setValue("123456");
		linkConfig.getStringInput("dialect.identifierEnclose").setValue("`");
		
		Status status = client.saveLink(mysqlLink);
		if(status.canProceed()){
			System.out.println("创建LInk成功");
		}else{
			System.out.println("创建link失败"+status.toString());
		}
	}
	//创建一个job,把mysql_on_linux的TBLS的数据导入到hdfs的/sqooptest/tbls目录下面
	public static void createJobMysqlTblsToHdfs(){
		MJob job = client.createJob("mysql_on_linux_teacher", "my-hdfs");
		job.setName("mysql_tbls_to_hdfs_teacher");
		
		MFromConfig fconfigs = job.getFromJobConfig();
		
		MToConfig tconfigs = job.getToJobConfig();
		System.out.println("打印from job的配置参数");
		describeConfigs(fconfigs.getConfigs());
		fconfigs.getStringInput("fromJobConfig.schemaName").setValue("hive");
		fconfigs.getStringInput("fromJobConfig.tableName").setValue("TBLS");
		fconfigs.getStringInput("fromJobConfig.partitionColumn").setValue("TBL_ID");
		
		System.out.println("打印to job的配置参数");
		describeConfigs(tconfigs.getConfigs());
		tconfigs.getStringInput("toJobConfig.outputDirectory").setValue("/sqoopteacher/tbls");
		tconfigs.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE");
		tconfigs.getBooleanInput("toJobConfig.appendMode").setValue(true);
		
		Status status = client.saveJob(job);
		if(status.canProceed()){
			System.out.println("创建job成功");
		}else{
			System.out.println("创建job失败"+ status);
		}
	}
	public static void startJob(String jobName){
		MSubmission submission = client.startJob(jobName);
		while(submission.getStatus().isRunning()){
			System.out.println("程序运行中，请等候。。。");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if(submission.getStatus().isFailure()){
			System.out.println("job运行失败！");
		}else{
			System.out.println("job运行成功");
		}
	}
	//打印配置项
	public static void describeConfigs(List<MConfig> configs){
		for(MConfig config :configs){
			List<MInput<?>> inputs = config.getInputs();
			for(MInput<?> input :inputs){
				System.out.println(input);
			}
		}
	}
	public static void main(String[] args) {
//		createLinkToLinuxMysql();
//		createJobMysqlTblsToHdfs();
		SqoopUtilsTeacher.startJob("mysql_tbls_to_hdfs_teacher");
	}
}

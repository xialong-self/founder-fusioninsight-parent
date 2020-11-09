package com.founder.fusioninsight.spark.dataimport;

import static org.apache.spark.sql.functions.lit;

import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;

/**
 *  机场项目：每日任务：每日航班数、每日旅客数、每日到达目的地
 * @author xialong
 */


public class jwzh_jcgl_mrrw {
	/**
	 * setu_log
	 */
	static String SETU_LOG="jwzh_jcgl.tb_st_ajxx";
	
	public static void main(String[] args)  {
		FusioninsightConfig.initArgs(args);
		new jwzh_jcgl_mrrw().run();
	  }
	public void run() {

		String	endDate = DateTimeUtils.DateToString(new Date(), DateTimeUtils.YYYY_MM_DD);
		String tjzq = DateTimeUtils.DateToString(DateTimeUtils.addDays(DateTimeUtils.StringToDate(endDate, DateTimeUtils.YYYY_MM_DD), -1), DateTimeUtils.YYYY_MM_DD);
		String tjsj = DateTimeUtils.getSystemDateTimeString();
	    // 通过spark接口获取表中的数据
		FusioninsightConfig.getInstance().login();
		SparkSession sparkSession = SparkSession.builder()
	              .appName("jwzh_jcgl_mrrw")	        
	              .config("spark.driver.host",FusioninsightConfig.getDriverHost())
	              .config(ConfigurationOptions.ES_NET_USE_SSL,"true")
	              .config(ConfigurationOptions.ES_NODES,FusioninsightConfig.getInstance().getEsConfig().getEsServerHost())
	              .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, FusioninsightConfig.getInstance().getUserConfig().getUsername())
	              .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, FusioninsightConfig.getInstance().getUserConfig().getPassword())
				  .config("spark.sql.crossJoin.enabled",true)
				  .config("spark.yarn.am.waitTime", 1000)
				  .config("spark.sql.broadcastTimeout", 1000)
				  .config(ConfigurationOptions.ES_INDEX_AUTO_CREATE,true)
				  .config(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY,"yes")
	              .getOrCreate();
		//原数据
		Dataset<Row> dpxxdata_y=jwzh_jcgl_data.DPXX(sparkSession).cache();
		Dataset<Row> zdxxdata_y=jwzh_jcgl_data.ZDB(sparkSession).cache();
		Dataset<Row> gtxxdata_y=jwzh_jcgl_data.GTXX(sparkSession).cache();

		
		//取前一天的dpxx
		Dataset<Row> dpxxYesterday=dpxxdata_y;
		//有效证件号
		Dataset<Row> dpxxdata=DataZjhm(dpxxYesterday,"dpxx_zjhm").cache();
		//每日数据
		Dataset<Row> dataDay=dpxxdata
				.where("dpxx_flightDate like '%"+tjzq+"%' ");
		dataDay.cache();
		//航班信息
		Dataset<Row> datahbxx=dataDay
				.groupBy("dpxx_flightNo","dpxx_flightDate","dpxx_qfsj","dpxx_ddsj","dpxx_qfjcdm","dpxx_ddjcdm").count()
				.selectExpr("dpxx_flightNo","dpxx_flightDate","dpxx_qfsj","dpxx_ddsj","dpxx_qfjcdm as qfjcdm","dpxx_ddjcdm as ddjcdm");
		Dataset<Row> datahbxx_fy=DataChange_hb(datahbxx,zdxxdata_y,gtxxdata_y).withColumn("tjsj", lit(tjsj))
				.selectExpr("dpxx_flightNo","dpxx_flightDate","dpxx_qfsj","dpxx_ddsj","qfjcdm_mc","ddjcdm_mc","qfjcdm","ddjcdm","qfjcsfxzqh","ddjcsfxzqh","qfjcsfm","ddjcsfm","tjsj");
	    //每日旅客数
		Dataset<Row> data_mrlks=dataDay
				.dropDuplicates("addid_dp").withColumn("tjzq", lit(tjzq)).groupBy("tjzq").count()
				.withColumn("tjsj", lit(tjsj))
				.selectExpr("tjzq as id","count as number","tjzq","tjsj");
		//每日目的地
		Dataset<Row> data_mrmdd=DataQf_Dd(dataDay, gtxxdata_y).groupBy("mddsfm","ddjcsfxzqh").count()
				.withColumn("tjsj",lit(tjsj)).withColumn("tjzq", lit(tjzq))
				.selectExpr("concat(ddjcsfxzqh,tjzq) as id","mddsfm","count as number","tjsj","tjzq");
		//翻译
	    EsSparkSQL.saveToEs(datahbxx_fy,"jwzh_jcgl.mrhb/_doc");
	    saveTjjgToEs(data_mrlks,"jwzh_jcgl.mrlks");
	    EsSparkSQL.saveToEs(data_mrmdd,"jwzh_jcgl.mrddmdd/_doc");

	    sparkSession.stop();
	}

	//剔除无效证件号码
	public static Dataset<Row> DataZjhm(Dataset<Row> data,String zjhm) {
		Dataset<Row> dataOne=data.filter(" "+zjhm+" like 'N%' ");	
		Dataset<Row> dataTwo=data.filter(" length("+zjhm+")=18 ");	
		Dataset<Row> datayx=dataOne.unionByName(dataTwo).distinct();
		return datayx;
	}
	//存入ES
	public void saveTjjgToEs(Dataset<Row> tjjg,String indexname) {
		//指定ID字段，便于覆盖原有数据
		java.util.Map<String, String> map = new HashMap<String, String>(); 
		map.put("es.mapping.id","id");
		scala.collection.mutable.Map<String, String> salaMap = scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
		EsSparkSQL.saveToEs(tjjg,indexname+"/_doc",salaMap);
	}
	
	//字段翻译-航班信息使用
	public static Dataset<Row> DataChange_hb(Dataset<Row> data,Dataset<Row> zddata,Dataset<Row> gtdata) {
		Dataset<Row> zddata_yx=zddata.where("dictLabel='DB_YW_JCDM'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
		Dataset<Row> sfdata_zjtd=gtdata.select("jcszdm","sfxzqh","sfm").groupBy("jcszdm","sfxzqh","sfm").count().select("jcszdm","sfxzqh","sfm");

		Dataset<Row> datajcOne=data.join(zddata_yx,data.col("qfjcdm").equalTo(zddata_yx.col("itemValue")),"left")
				.withColumnRenamed("itemName","qfjcdm_mc").drop("itemValue");
		Dataset<Row> datajcTwo=datajcOne.join(zddata_yx,datajcOne.col("ddjcdm").equalTo(zddata_yx.col("itemValue")),"left")
				.withColumnRenamed("itemName","ddjcdm_mc").drop("itemValue");
		
		//人员起飞和到达省份
		Dataset<Row> datagtThree=datajcTwo.join(sfdata_zjtd,datajcTwo.col("qfjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
				.withColumnRenamed("sfxzqh", "qfjcsfxzqh").withColumnRenamed("sfm", "qfjcsfm").drop("jcszdm");
		Dataset<Row> datagtFour=datagtThree.join(sfdata_zjtd,datagtThree.col("ddjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
				.withColumnRenamed("sfxzqh", "ddjcsfxzqh").withColumnRenamed("sfm", "ddjcsfm").drop("jcszdm");
		
		
		
		return datagtFour;
	}
	//目的地机场翻译
	public static Dataset<Row> DataQf_Dd(Dataset<Row> data,Dataset<Row> gtdata){
		Dataset<Row> sfdata_zjtd=gtdata.select("jcszdm","sfxzqh","sfm").groupBy("jcszdm","sfxzqh","sfm").count().select("jcszdm","sfxzqh","sfm");
		//目的地省份
		Dataset<Row> datagtFour=data.join(sfdata_zjtd,data.col("dpxx_ddjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
				.withColumnRenamed("sfxzqh", "ddjcsfxzqh").withColumnRenamed("sfm", "mddsfm").drop("jcszdm");
		
		return datagtFour;
	}

	//读取登机信息
	public static Dataset<Row> DJXX(SparkSession sparkSession,String date) {
		Dataset<Row> djxx=sparkSession
				.sql("select * from "+SETU_LOG+" ");
		return djxx;
	}
	

}

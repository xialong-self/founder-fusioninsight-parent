package com.founder.fusioninsight.spark.dataimport;

import static org.apache.spark.sql.functions.lit;

import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.incremental.jwzh_jcgl_addData;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;



/**
 *  机场项目：宽表全量计算
 *  jwzh_jcgl_data 全量数据取出
 *  jwzh_cjgl_glzh 数据取出后关联转换
 * @author xialong
 */
public class jwzh_jcgl_all {
	public static void main(String[] args)  {
		FusioninsightConfig.initArgs(args);
		new jwzh_jcgl_all().run();
	  }
	
	public void run() {	
		String tjsj = DateTimeUtils.getSystemDateTimeString();
	    //更新时间戳
	    String StartTime = DateTimeUtils.getSystemDateTimeString();
	    // 通过spark接口获取表中的数据
	    FusioninsightConfig.getInstance().login();
		SparkSession sparkSession = SparkSession.builder()
	              .appName("jwzh_jcgl_all")	        
	              .config("spark.driver.host",FusioninsightConfig.getDriverHost())
	              .config(ConfigurationOptions.ES_NET_USE_SSL,"true")
	              .config(ConfigurationOptions.ES_NODES,FusioninsightConfig.getInstance().getEsConfig().getEsServerHost())
	              .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, FusioninsightConfig.getInstance().getUserConfig().getUsername())
	              .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, FusioninsightConfig.getInstance().getUserConfig().getPassword())
	              .config("spark.sql.crossJoin.enabled",true)
	              .getOrCreate();


		//原数据
		Dataset<Row> dpxxdata_y=jwzh_jcgl_data.DPXX(sparkSession).where("xtlrsj_dpxx < '"+StartTime+"'")
				.repartition(100).cache();
		Dataset<Row> ajxxdata_y=jwzh_jcgl_data.AJXX(sparkSession).where("xtlrsj_ajxx < '"+StartTime+"'")
				.repartition(100).cache();
		Dataset<Row> zjxxdata_y=jwzh_jcgl_data.ZJXX(sparkSession).where("xtlrsj_zjxx < '"+StartTime+"'")
				.repartition(100).cache();
		Dataset<Row> djxxdata_y=jwzh_jcgl_data.DJXX(sparkSession).where("xtlrsj_djxx < '"+StartTime+"'")
				.repartition(100).cache();
		Dataset<Row> zdxxdata_y=jwzh_jcgl_data.ZDB(sparkSession).where("xtZxbz='0' ").cache();
		Dataset<Row> gtxxdata_y=jwzh_jcgl_data.GTXX(sparkSession).cache();
		Dataset<Row> zdrydata_y=jwzh_jcgl_data.ZDRY(sparkSession).where("xtlrsj < '"+StartTime+"'").where("xtZxbz='0' ").cache();

		//有效证件号
		Dataset<Row> dpxxdata=DataZjhm(dpxxdata_y,"dpxx_zjhm").cache();
		Dataset<Row> ajxxdata=DataZjhm(ajxxdata_y,"ajxx_idcard").cache();
		Dataset<Row> zjxxdata=DataZjhm(zjxxdata_y,"zjxx_cNumber").cache();
		Dataset<Row> djxxdata=DataZjhm(djxxdata_y,"djxx_cNumber").cache();
		//能关联上的--dpxx为准
	    Dataset<Row> dataJoin1=jwzh_jcgl_glzh.joinOne(sparkSession,dpxxdata,zjxxdata,ajxxdata,djxxdata);
	    Dataset<Row> dataJoin2=jwzh_jcgl_glzh.joinTwo(sparkSession,zjxxdata,dpxxdata,ajxxdata,djxxdata);
	    Dataset<Row> dataJoin3=jwzh_jcgl_glzh.joinThree(sparkSession,ajxxdata,zjxxdata,dpxxdata,djxxdata);
	    Dataset<Row> dataJoin4=jwzh_jcgl_glzh.joinFour(sparkSession,djxxdata,zjxxdata,ajxxdata,dpxxdata);
	    

	    //合并 
	    Dataset<Row> dataJoin_union=dataJoin1.unionByName(dataJoin2).unionByName(dataJoin3).unionByName(dataJoin4);
//		dataJoin_union.createOrReplaceTempView("es_table");
//		Dataset<Row> dataxb=sparkSession
//				.sql("select id,addid,cnName,flightno,flightDate,dpxx_qfsj,dpxx_ddsj,zjhm,xtlrsj,"
//						+ "xzqhdm,dpsj,zjxx_gender,seat,brdno,zjsj,zjxx_loc,"
//						+ "ajsj,ajxx_channelNo,djxx_gate,djsj,djxx_brdst,"
//						+ "dpxx_qfjcdm,bs,dpxx_ddjcdm,zjlxdm,"
//						+ "concat_ws(',',collect_set(dpxx_id)) as dpxx_id,"
//						+ "concat_ws(',',collect_set(zjxx_id)) as zjxx_id,"
//						+ "concat_ws(',',collect_set(ajxx_id)) as ajxx_id,"
//						+ "concat_ws(',',collect_set(djxx_id)) as djxx_id "
//						+ " from es_table "
//						+ "group by id,addid,cnName,flightno,flightDate,dpxx_qfsj,dpxx_ddsj,zjhm,xtlrsj,"
//						+ "xzqhdm,dpsj,zjxx_gender,seat,brdno,zjsj,zjxx_loc,"
//						+ "ajsj,ajxx_channelNo,djxx_gate,djsj,djxx_brdst,"
//						+ "dpxx_qfjcdm,bs,dpxx_ddjcdm,zjlxdm,dpxx_id,zjxx_id,ajxx_id,djxx_id");
	    //重点人员标记
	    Dataset<Row> dataZdry=DataZdry(dataJoin_union,zdrydata_y);
	    //字段翻译 
	    Dataset<Row> datafy_all=DataChange(dataZdry,zdxxdata_y,gtxxdata_y,sparkSession).withColumn("tjsj",lit(tjsj));
	    //输出
	    saveTjjgToEs(datafy_all,"jwzh_jcgl.all");
//		EsSparkSQL.saveToEs(datafy_all,"jwzh_jcgl.all/_doc");
	    //释放内存
	    dpxxdata_y.unpersist();
	    ajxxdata_y.unpersist();
	    zjxxdata_y.unpersist();
	    djxxdata_y.unpersist();
	    dpxxdata.unpersist();
	    ajxxdata.unpersist();
	    zjxxdata.unpersist();
	    djxxdata.unpersist();

	    NewTimeStamp(sparkSession, StartTime, tjsj);
	    sparkSession.stop();
	    
	    
	}

	//剔除无效证件号码
	public static Dataset<Row> DataZjhm(Dataset<Row> data,String zjhm) {
		
		Dataset<Row> dataOne=data.filter(" "+zjhm+" like 'N%' ");	
		Dataset<Row> dataTwo=data.filter(" length("+zjhm+")=18 ");	
		Dataset<Row> datayx=dataOne.unionByName(dataTwo).distinct();
		return datayx;
	}
	
	//重点人员标记
	public static Dataset<Row> DataZdry(Dataset<Row> data,Dataset<Row> zdry) {
		//按类型取出重点人员数据
		Dataset<Row> zdry_all=zdry.groupBy("addid_bdmx","sjly").count().selectExpr("addid_bdmx","sjly").cache();
		Dataset<Row> dataOne=data.join(zdry_all.where("sjly='在逃' "),data.col("addid").equalTo(zdry_all.where("sjly='在逃' ").col("addid_bdmx")),"left").withColumnRenamed("sjly","rylx_zt").drop("addid_bdmx");
		Dataset<Row> dataTwo=dataOne.join(zdry_all.where("sjly='布控' "),dataOne.col("addid").equalTo(zdry_all.where("sjly='布控' ").col("addid_bdmx")),"left").withColumnRenamed("sjly","rylx_bk").drop("addid_bdmx");
		Dataset<Row> dataThree=dataTwo.join(zdry_all.where("sjly='重点' "),dataTwo.col("addid").equalTo(zdry_all.where("sjly='重点' ").col("addid_bdmx")),"left").withColumnRenamed("sjly","rylx_zd").drop("addid_bdmx");
		return dataThree;
	}

		
	//和并去重
	public static Dataset<Row> DataAdd(Dataset<Row> data) {
		Dataset<Row> dataOne=data
				.dropDuplicates("addid","zjlxdm","zjhm","cnName","flightno","flightDate","xtlrsj")
				.withColumn("zdbz",lit("1"));	
		return dataOne;
	}
		
	//字段翻译
	public static Dataset<Row> DataChange(Dataset<Row> data,Dataset<Row> zddata,Dataset<Row> gtdata,SparkSession sparkSession) {

		//获取字典表各个指标数据
		Dataset<Row> zddata_yx=zddata.where("dictLabel='DB_YW_JCDM'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
		Dataset<Row> zddata_zjlx=zddata.where("dictLabel='DB_GA_ZJLX'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
		Dataset<Row> zddata_xzqh=zddata.where("dictLabel='DB_GA_XZQH'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
		Dataset<Row> gtdata_ajtd=gtdata.select("ajtdh","ajtdmc").groupBy("ajtdh","ajtdmc").count().select("ajtdh","ajtdmc");
		Dataset<Row> gtdata_zjtd=gtdata.select("zjgt_pid","zjgt").groupBy("zjgt_pid","zjgt").count().select("zjgt_pid","zjgt");
		Dataset<Row> sfdata_zjtd=gtdata.select("jcszdm","sfxzqh","sfm").groupBy("jcszdm","sfxzqh","sfm").count().select("jcszdm","sfxzqh","sfm");
		//根据字典表翻译字段
		Dataset<Row> datajcOne=data.join(zddata_yx,data.col("dpxx_qfjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","qfjcdm_mc").drop("itemValue");
		Dataset<Row> datajcTwo=datajcOne.join(zddata_yx,datajcOne.col("dpxx_ddjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","ddjcdm_mc").drop("itemValue");
		Dataset<Row> datajcThree=datajcTwo.join(zddata_zjlx,datajcTwo.col("zjlxdm").equalTo(zddata_zjlx.col("itemValue")),"left").withColumnRenamed("itemName","zjlxdm_mc").drop("itemValue");
		Dataset<Row> datajcFour=datajcThree.join(zddata_xzqh,datajcThree.col("xzqhdm").equalTo(zddata_xzqh.col("itemValue")),"left").withColumnRenamed("itemName","xzqh").drop("itemValue");
		
		//性别和zjsj翻译--直接生成临时表翻译
		datajcFour.createOrReplaceTempView("xb_table");
		Dataset<Row> dataxb=sparkSession
				.sql("select id,addid,cnname,flightno,flightdate,dpxx_qfsj,dpxx_ddsj,zjhm,xtlrsj," + 
				"xzqhdm,dpsj,zjxx_gender,seat,brdno,zjxx_loc," + 
				"ajsj,ajxx_channelno,djxx_gate,djsj,djxx_brdst," + 
				"dpxx_qfjcdm,qfjcdm_mc,bs,dpxx_ddjcdm,ddjcdm_mc,zjlxdm_mc,zjlxdm,xzqh,rylx_zt,rylx_bk,rylx_zd,dpxx_id,zjxx_id,ajxx_id,djxx_id," +
				"case " + 
				"  when zjxx_gender='M' then '男性'  " + 
				"  when zjxx_gender='F' then '女性'  " + 
				"  when zjxx_gender='C' then '儿童'  " + 
				"  else '' " +
				"  end as xb, " + 
				"case " + 
				"  when zjsj=':' then ''  " + 
				"  else zjsj " +
				"  end as zjsj " + 
				" from xb_table");
		//安检通道翻译 --channelno
		Dataset<Row> datagtOne=dataxb.join(gtdata_ajtd,dataxb.col("ajxx_channelno").equalTo(gtdata_ajtd.col("ajtdh")),"left").withColumnRenamed("ajtdmc", "channelno_mc").drop("ajtdh");
		//值机操作地点翻译  --loc
		Dataset<Row> datagtTwo=datagtOne.join(gtdata_zjtd,datagtOne.col("zjxx_loc").equalTo(gtdata_zjtd.col("zjgt_pid")),"left").withColumnRenamed("zjgt", "loc_mc").drop("zjgt_pid");
		//人员起飞和到达省份
		Dataset<Row> datagtThree=datagtTwo.join(sfdata_zjtd,datagtTwo.col("dpxx_qfjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
				.withColumnRenamed("sfxzqh", "qfjcsfxzqh").withColumnRenamed("sfm", "qfjcsfm").drop("jcszdm");
		Dataset<Row> datagtFour=datagtThree.join(sfdata_zjtd,datagtThree.col("dpxx_ddjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
				.withColumnRenamed("sfxzqh", "ddjcsfxzqh").withColumnRenamed("sfm", "ddjcsfm").drop("jcszdm");
		
		return datagtFour;
	}
	
	//字段翻译-航班信息使用
	public static Dataset<Row> DataChange_hb(Dataset<Row> data,Dataset<Row> zddata) {
		Dataset<Row> zddata_yx=zddata.where("dictLabel='DB_YW_JCDM'").groupBy("itemValue","itemName").count().select("itemValue","itemName").cache();
		Dataset<Row> datajcOne=data.join(zddata_yx,data.col("dpxx_qfjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","qfjcdm").drop("dpxx_qfjcdm","itemValue");
		Dataset<Row> datajcTwo=datajcOne.join(zddata_yx,datajcOne.col("dpxx_ddjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","ddjcdm").drop("dpxx_ddjcdm","itemValue");
		return datajcTwo;
	}
	
	//测试存入mysql
	public static String saveTjjgToEsMYSQL(Dataset<Row> tjjg,String tablename) {
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "jwzh#123");
		connectionProperties.put("Driver", "com.mysql.jdbc.Driver");
		tjjg.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://80.2.21.211:32000/xialong?characterEncoding=utf8", tablename, connectionProperties);
		return "yes";
	}
	
	//ES指定id字段覆盖原数据
	private void saveTjjgToEs(Dataset<Row> tjjg,String indexname) {
		java.util.Map<String, String> map = new HashMap<String, String>(); 
		map.put("es.mapping.id","id");
		scala.collection.mutable.Map<String, String> salaMap = scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
		EsSparkSQL.saveToEs(tjjg,indexname+"/_doc",salaMap);
		
		
	}
	
	
	//更新时间戳
	private void NewTimeStamp(SparkSession sparkSession,String endTime,String tjsj) {
	    Dataset<Row> sjcdata_y2=jwzh_jcgl_addData.TIMESTAMP2(sparkSession);
	    Dataset<Row> endtime=sjcdata_y2.drop("id","timestampx")
	    		.withColumn("id", lit("endtimestamp"))
	    		.withColumn("endtime", lit(endTime))
	    		.withColumn("tjzq", lit(tjsj));
	    endtime.write().format("Hive").mode(SaveMode.Append).saveAsTable("jwzh_jcgl.sjc");
	}
	

}



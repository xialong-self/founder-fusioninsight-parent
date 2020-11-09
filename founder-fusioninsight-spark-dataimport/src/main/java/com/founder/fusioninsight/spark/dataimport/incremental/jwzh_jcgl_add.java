package com.founder.fusioninsight.spark.dataimport.incremental;



import static org.apache.spark.sql.functions.lit;

import java.util.HashMap;

import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_data;
import com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_glzh;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;
import com.huawei.hadoop.security.LoginUtil;


/**
 *  机场项目：宽表增量计算
 *  jwzh_jcgl_addData 增量数据取出
 * 	jwzh_cjgl_addGlzh 增量数据取出后关联转换
 *  esDataGlzh 新数据计算完后取出ES中数据进行对比
 * @author xialong
 */

public class jwzh_jcgl_add {
	public static void main(String[] args)  {
		FusioninsightConfig.initArgs(args);
		new jwzh_jcgl_add().run();

	  }
	
	public void run()  {
		String tjsj = DateTimeUtils.getSystemDateTimeString();
		//更新时间戳
		String endTime = DateTimeUtils.getSystemDateTimeString();
		//集群sparksession
		SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl.add");
		//获取时间戳
		Dataset<Row> sjcdata_y=EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.sjc/_doc")
				.filter("id='endtimestamp'")
				.selectExpr("max(endtime)");
		String timestamp=sjcdata_y.first().get(0).toString();
		//取上次结束时间60s之前
		String beforetime=DateTimeUtils.DateToString(DateTimeUtils.addHour24(DateTimeUtils.StringToDate(timestamp, DateTimeUtils.YYYY_MM_DD_HH_MM_SS), -1), DateTimeUtils.YYYY_MM_DD_HH_MM_SS);
		System.out.println("---------------TIME-------------------");
		//原数据



		Dataset<Row> dpxxdata_y=jwzh_jcgl_addData.DPXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> ajxxdata_y=jwzh_jcgl_addData.AJXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> zjxxdata_y=jwzh_jcgl_addData.ZJXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> djxxdata_y=jwzh_jcgl_addData.DJXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> zdxxdata_y=jwzh_jcgl_addData.ZDB(sparkSession).where("xtZxbz='0' ").cache();
		Dataset<Row> gtxxdata_y=jwzh_jcgl_data.GTXX(sparkSession).cache();
//		Dataset<Row> zdrydata_y=jwzh_jcgl_data.ZDRY(sparkSession).where("xtZxbz='0' ").cache();
		//每次任务数据量
//		Dataset<Row> dataAllCount=DataCount(dpxxdata_y, ajxxdata_y, zjxxdata_y, djxxdata_y).withColumn("tjsj",lit(tjsj));
//		EsSparkSQL.saveToEs(dataAllCount,"jwzh_jcgl.mcsjl/_doc");
		//有效证件号
		Dataset<Row> dpxxdata=DataZjhm(dpxxdata_y,"dpxx_zjhm");
		dpxxdata.cache();
		Dataset<Row> ajxxdata=DataZjhm(ajxxdata_y,"ajxx_idcard");
		ajxxdata.cache();
		Dataset<Row> zjxxdata=DataZjhm(zjxxdata_y,"zjxx_cNumber");
		zjxxdata.cache();
		Dataset<Row> djxxdata=DataZjhm(djxxdata_y,"djxx_cNumber");
		djxxdata.cache();
		System.out.println("-----------------检查点1------------------------");
		//多表关联生成宽表
	    Dataset<Row> dataJoin1=jwzh_jcgl_glzh.joinOne(sparkSession,dpxxdata,zjxxdata,ajxxdata,djxxdata);
	    Dataset<Row> dataJoin2=jwzh_jcgl_glzh.joinTwo(sparkSession,zjxxdata,dpxxdata,ajxxdata,djxxdata);
	    Dataset<Row> dataJoin3=jwzh_jcgl_glzh.joinThree(sparkSession,ajxxdata,zjxxdata,dpxxdata,djxxdata);
	    Dataset<Row> dataJoin4=jwzh_jcgl_glzh.joinFour(sparkSession,djxxdata,zjxxdata,ajxxdata,dpxxdata);
		System.out.println("-----------------检查点2------------------------");

	    //合并去重 
	    Dataset<Row> dataJoin_union=dataJoin1.unionByName(dataJoin2).unionByName(dataJoin3).unionByName(dataJoin4);
		System.out.println("-----------------检查点3------------------------");
	    //重点人员标记
//	    Dataset<Row> dataZdry=DataZdry(dataJoin_union,zdrydata_y).cache();

	    //字段翻译 
	    Dataset<Row> datafy_all=DataChange(dataJoin_union,zdxxdata_y,gtxxdata_y,sparkSession).withColumn("tjsj",lit(tjsj))
	    		.cache();
	    //于之前的数据进行对比处理
//	    Dataset<Row> dataAdd=esDataGlzh.DataZh(datafy_all, sparkSession);
	    //输出

	    new SaveToEsUtils().saveTjjgToEs_Append(datafy_all,"jwzh_jcgl.all");

	    NewTimeStamp(sparkSession, endTime, tjsj);
	    sparkSession.stop();
	  
	}    
	
	
	//求每次数据量
	public static Dataset<Row> DataCount(Dataset<Row> data1,Dataset<Row> data2,Dataset<Row> data3,Dataset<Row> data4) {
		Dataset<Row> dataOne=data1.withColumn("bs", lit("dpxx")).groupBy("bs").count().selectExpr("count as datanum","bs");
		Dataset<Row> dataTwo=data2.withColumn("bs", lit("ajxx")).groupBy("bs").count().selectExpr("count as datanum","bs");
		Dataset<Row> dataThree=data3.withColumn("bs", lit("zjxx")).groupBy("bs").count().selectExpr("count as datanum","bs");
		Dataset<Row> dataFour=data4.withColumn("bs", lit("djxx")).groupBy("bs").count().selectExpr("count as datanum","bs");
		
		Dataset<Row> data=dataOne.unionByName(dataTwo).unionByName(dataThree).unionByName(dataFour);
		return data;
	}

	//剔除无效证件号码
	public static Dataset<Row> DataZjhm(Dataset<Row> data,String zjhm) {
		Dataset<Row> dataOne=data.filter(" "+zjhm+" like 'N%' ");	
		Dataset<Row> dataTwo=data.filter(" length("+zjhm+")=18 ");	
		Dataset<Row> datayx=dataOne.unionByName(dataTwo).distinct();
		return datayx;
	}
	//重点人员标记
//	public static Dataset<Row> DataZdry(Dataset<Row> data,Dataset<Row> zdry) {
//		//按类型取出重点人员数据
//		Dataset<Row> zdry_all=zdry.groupBy("addid_bdmx","sjly").count().selectExpr("addid_bdmx","sjly").cache();
//		Dataset<Row> dataOne=data.join(zdry_all.where("sjly='在逃' "),data.col("addid").equalTo(zdry_all.where("sjly='在逃' ").col("addid_bdmx")),"left").withColumnRenamed("sjly","rylx_zt").drop("addid_bdmx");
//		Dataset<Row> dataTwo=dataOne.join(zdry_all.where("sjly='布控' "),dataOne.col("addid").equalTo(zdry_all.where("sjly='布控' ").col("addid_bdmx")),"left").withColumnRenamed("sjly","rylx_bk").drop("addid_bdmx");
//		Dataset<Row> dataThree=dataTwo.join(zdry_all.where("sjly='重点' "),dataTwo.col("addid").equalTo(zdry_all.where("sjly='重点' ").col("addid_bdmx")),"left").withColumnRenamed("sjly","rylx_zd").drop("addid_bdmx");
//
//		return dataThree;
//	}
		

		
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
		Dataset<Row> datajcThree=datajcTwo.join(zddata_zjlx,datajcTwo.col("zjlxdm").equalTo(zddata_zjlx.col("itemValue")),"left").withColumnRenamed("itemName","zjlxdm_mc");
		Dataset<Row> datajcFour=datajcThree.join(zddata_xzqh,datajcThree.col("xzqhdm").equalTo(zddata_xzqh.col("itemValue")),"left").withColumnRenamed("itemName","xzqh").drop("itemValue");
		
		//性别和zjsj翻译--直接生成临时表翻译
		datajcFour.createOrReplaceTempView("xb_table");
		Dataset<Row> dataxb=sparkSession
				.sql("select id,addid,cnname,flightno,flightdate,dpxx_qfsj,dpxx_ddsj,zjhm,xtlrsj," + 
				"xzqhdm,dpsj,zjxx_gender,seat,brdno,zjxx_loc," + 
				"ajsj,ajxx_channelno,djxx_gate,djsj,djxx_brdst," + 
				"dpxx_qfjcdm,qfjcdm_mc,bs,dpxx_ddjcdm,ddjcdm_mc,zjlxdm_mc,zjlxdm,xzqh,dpxx_id,zjxx_id,ajxx_id,djxx_id," +
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


	


	private void saveTjjgToEs(Dataset<Row> tjjg,String indexname) {
		//指定ID字段，便于覆盖原有数据
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
	    EsSparkSQL.saveToEs(endtime,"jwzh_jcgl.sjc/_doc");
//	    endtime.write().format("Hive").mode(SaveMode.Append).saveAsTable("jwzh_jcgl.sjc");
	}


}




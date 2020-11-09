package com.founder.fusioninsight.spark.dataimport;

import static org.apache.spark.sql.functions.lit;

import java.util.HashMap;

import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.incremental.jwzh_jcgl_addData;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;

import javax.xml.crypto.Data;

/**
 * @author xialong
 */
public class jwzh_jcgl_allData {
	/**
	 * 安检信息、值机信息、订票信息、登机信息、进港来源表 全量数据迁移
	 */
	 static String TB_ST_AJXX="jwzh_jcgl.tb_st_ajxx";
	 static String TB_ST_ZJXX="jwzh_jcgl.tb_st_zjxx";
	 static String TB_ST_DPXX="jwzh_jcgl.tb_st_dpxx";
	 static String TB_ST_DJXX="jwzh_jcgl.tb_st_djxx";
	 static String TB_ST_TIMESTAMP="jwzh_jcgl.sjc";
	 static String TB_ST_TIMESTAMP2="jwzh_jcgl.xialong_timestamp";
	 static String TB_ST_JGXX="jwzh_jcgl.tb_st_jgxx";
	
	public static void main(String[] args) {
		FusioninsightConfig.initArgs(args);
		new jwzh_jcgl_allData().run();

	}
	
	public void run() {
		SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl_allDate");
		String tjsj = DateTimeUtils.getSystemDateTimeString();

		String endTime = DateTimeUtils.getSystemDateTimeString();
		//获取时间戳
		Dataset<Row> sjcdata_y=EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.sjc/_doc")
				.filter("id='alladata_timestamp'")
				.selectExpr("max(endtime)");
//		Dataset<Row> sjcdata_y=TIMESTAMP(sparkSession);
		String timestamp=sjcdata_y.first().get(0).toString();
		//取上次结束时间60s之前
		String beforetime=DateTimeUtils.DateToString(DateTimeUtils.addHour24(DateTimeUtils.StringToDate(timestamp, DateTimeUtils.YYYY_MM_DD_HH_MM_SS), -1), DateTimeUtils.YYYY_MM_DD_HH_MM_SS);


		Dataset<Row> dpxxdata=DPXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> ajxxdata=AJXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> zjxxdata=ZJXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> djxxdata=DJXX(sparkSession,beforetime).repartition(100).cache();
		Dataset<Row> jgxxdata=JGXX(sparkSession,beforetime).repartition(100).cache();


		//每次任务数据量
//		Dataset<Row> dataAllCount=DataCount(dpxxdata, ajxxdata, zjxxdata, djxxdata).withColumn("tjsj",lit(tjsj));


		new SaveToEsUtils().saveTjjgToEs_Append(dpxxdata,"jwzh_jcgl.tb_st_dpxx");
		new SaveToEsUtils().saveTjjgToEs_Append(ajxxdata,"jwzh_jcgl.tb_st_ajxx");
		new SaveToEsUtils().saveTjjgToEs_Append(zjxxdata,"jwzh_jcgl.tb_st_zjxx");
		new SaveToEsUtils().saveTjjgToEs_Append(djxxdata,"jwzh_jcgl.tb_st_djxx");
		new SaveToEsUtils().saveTjjgToEs_Append(jgxxdata,"jwzh_jcgl.tb_st_jgxx");


//		EsSparkSQL.saveToEs(dataAllCount,"jwzh_jcgl.mcsjl/_doc");
		//更新时间戳
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


	//读取登机信息
	public static Dataset<Row> DJXX(SparkSession sparkSession,String beforetime) {
		Dataset<Row> djxx=sparkSession
				.sql("select rowkey as id,upper(concat(cNumber,flightno,flightdate))as addid_dj,flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ," +
						"gate ,brdno ,name ,cnName ,brdst ,seat ,from_unixtime(unix_timestamp(optm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as optm ," +
						"sndr ,seqn ,dttm ,type ,mesState ,wetm ,cType ,cNumber ,appId ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm ,xtLrrid ,xtLrrbm ," +
						"xtLrrbmid ,xtLrip ,substr(xtZhxgsj,1,19) as xtZhxgsj ,xtZhxgrxm ,xtZhxgrid ,xtZhxgrbm ,xtZhxgrbmid ,xtZxbz ,xtZxyy  from "+TB_ST_DJXX+"  where xtLrsj >='"+beforetime+"' ");
		return djxx;
	}
	//读取值机信息
	public static Dataset<Row> ZJXX(SparkSession sparkSession,String beforetime) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,flightNo ,upper(concat(cNumber,flightno,flightdate))as addid_zj," +
						"from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ,dept as qfjcdm," +
						"dest as ddjcdm,name,cnName,cType,cNumber,gender,seat,brdno,concat(substr(ckitm,1,2),':',substr(ckitm,3,2)) as ckitm," +
						"substr(cNumber,1,6) as xzqhdm,dept,dest,leval,depttm,inf,dttm,ckipid,ckiofc,ckiagt,sndr,seqn,type,styp ,loc,prsta ," +
						"gates,ctct,etFl,etNo,isGroup,bsgs,bagWgt,sip,wetm,mesState,appId,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrip," +
						"xtLrrbm  ,xtLrrbmid ,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid,xtZhxgip,xtZhxgrbm,xtZhxgrbmid,xtZxbz ,xtZxyy from "+TB_ST_ZJXX+"   where xtLrsj >='"+beforetime+"' ");
		return data;
	}
	//读取订票信息
	public static Dataset<Row> DPXX(SparkSession sparkSession,String beforetime) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,xxId,flightNo,upper(concat(zjhm,flightno,flightdate))as addid_dp," +
						"from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate,ddsj," +
						"qfsj   ,qfjcdm ,ddjcdm ,name   ,substr(zjhm,1,6) as xzqhdm,cnName,lkzt,zjlx,zjhm,rksj,mesState," +
						"substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrrbm,xtLrrbmid,xtLrip,substr(xtZhxgsj,1,19) as xtZhxgsj," +
						"xtZhxgrxm,xtZhxgrid ,xtZhxgrbm,xtZhxgrbmid,xtZhxgip,xtZxbz,xtZxyy  from "+TB_ST_DPXX+"   where xtLrsj >='"+beforetime+"' ");
		return data;
	}
	//读取安检信息
	public static Dataset<Row> AJXX(SparkSession sparkSession,String beforetime) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,upper(concat(idcard,flightno,flightdate))as addid_aj,sndr ,seqn ,dttm ,type ,styp ," +
						"flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ,brdno ,dept ,isInOut ," +
						"name ,idcard ,channelNo  ,from_unixtime(unix_timestamp(sctm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as sctm ," +
						"mesState ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm,xtLrrid,xtLrip,xtLrrbm ,xtLrrbmid,substr(xtZhxgsj,1,19) as xtZhxgsj," +
						"xtZhxgrxm , xtZhxgrid ,xtZhxgip,xtZhxgrbm ,xtZhxgrbmid ,xtZxbz  ,xtZxyy  from "+TB_ST_AJXX+"   where xtLrsj >='"+beforetime+"'  ");
		return data;
	}


	//JGXX表
	public static Dataset<Row> JGXX(SparkSession sparkSession,String beforetime) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,upper(concat(zjhm,flightno,flightdate))as addid_jgxx,snId,flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate," +
						"qfjcdm,ddjcdm,qfsj,ddsj,name,cnName,lkzt,csrq,xb,zjhm,substr(zjhm,1,6) as xzqhdm,fjgj,rksj," +
						"mesState,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrrbm,xtLrrbmid,xtLrip,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid," +
						"xtZhxgrbm,xtZhxgrbmid,xtZhxgip,xtZxbz,xtZxyy from "+TB_ST_JGXX+"  where xtLrsj >='"+beforetime+"' ");

		return data;
	}







		//读取时间戳表
		public static Dataset<Row> TIMESTAMP(SparkSession sparkSession) {
			Dataset<Row> data=sparkSession
					.sql("select max(endtime) as timestampx from  "+TB_ST_TIMESTAMP+" where id='alladata_timestamp' ");
			return data;	
		}
		
		//读取时间戳表
		public static Dataset<Row> TIMESTAMP2(SparkSession sparkSession) {
			Dataset<Row> data=sparkSession
					.sql("select max(endtime) as timestampx from  "+TB_ST_TIMESTAMP2+" ");
			return data;
		}

	
	//更新时间戳
	private void NewTimeStamp(SparkSession sparkSession,String endTime,String tjsj) {
		String id ="alladata_timestamp";
	    Dataset<Row> sjcdata_y2=jwzh_jcgl_addData.TIMESTAMP2(sparkSession);
	    Dataset<Row> endtime=sjcdata_y2.drop("id","timestampx")
	    		.withColumn("id", lit(id))
	    		.withColumn("endtime", lit(endTime))
	    		.withColumn("tjzq", lit(tjsj));
		EsSparkSQL.saveToEs(endtime,"jwzh_jcgl.sjc/_doc");
//	    endtime.write().format("Hive").mode(SaveMode.Append).saveAsTable("jwzh_jcgl.sjc");
	}

}

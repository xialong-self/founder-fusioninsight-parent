package com.founder.fusioninsight.spark.dataimport.incremental;

import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.founder.fusioninsight.spark.utils.DateTimeUtils;



public class jwzh_jcgl_addData  {

	String	endDate = DateTimeUtils.DateToString(new Date(), DateTimeUtils.YYYY_MM_DD);
	String tjzq = DateTimeUtils.DateToString(DateTimeUtils.addDays(DateTimeUtils.StringToDate(endDate, DateTimeUtils.YYYY_MM_DD), -1), DateTimeUtils.YYYY_MM_DD);
	

	
	 static String TB_ST_AJXX="jwzh_jcgl.tb_st_ajxx";//安检信息
	 static String TB_ST_ZJXX="jwzh_jcgl.tb_st_zjxx";//值机信息
	 static String TB_ST_DPXX="jwzh_jcgl.tb_st_dpxx";//订票信息
	 static String TB_ST_DJXX="jwzh_jcgl.tb_st_djxx";//登机信息
	 static String TB_ST_BKRY="jwzh_jcgl.tb_st_bkry";//暴恐人员
	 static String MSG_STORAGE="jwzh_jcgl.msg_storage";//人员表 
	 static String SYS_DICT_ITEM="jwzh_jcgl.sys_dict_item";//字典表
	 static String TB_ST_JC="jwzh_jcgl.jcgl_gtxx";//柜台表
	 static String TB_ST_TIMESTAMP="jwzh_jcgl.sjc";//时间戳表
	 static String TB_ST_TIMESTAMP2="jwzh_jcgl.xialong_timestamp";//时间戳表
	

	//读取登机信息
	public static Dataset<Row> DJXX(SparkSession sparkSession,String enddate) {
		Dataset<Row> djxx=sparkSession
				.sql("select upper(concat(cNumber,flightno,flightdate))as addid_dj,rowkey,"
						+ "rowkey as djxx_id,cType as djxx_cType,cNumber as djxx_cNumber,"
						+ "cnName as djxx_cnName,flightNo as djxx_flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as djxx_flightDate,"
						+ "gate as djxx_gate,from_unixtime(unix_timestamp(optm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as djsj ,"
						+ "brdno as djxx_brdno,brdst as djxx_brdst,seat as djxx_seat,substr(xtlrsj,1,19) as xtlrsj_djxx,substr(cNumber,1,6) as djxx_xzqhdm from "+TB_ST_DJXX+" where xtlrsj >= '"+enddate+"' ");	
//		logEntity.addLogMsg("读取登机信息");		
		return djxx;	
	}
	//读取值机信息
	public static Dataset<Row> ZJXX(SparkSession sparkSession,String enddate) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(cNumber,flightno,flightdate))as addid_zj,rowkey as zjxx_id,rowkey,"
						+ "cType as zjxx_cType,cNumber as zjxx_cNumber,cnName as zjxx_cnName,"
						+ "gender as zjxx_gender,flightNo as zjxx_flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as zjxx_flightDate,"
						+ "seat as zjxx_seat,brdno as zjxx_brdno,concat(substr(ckitm,1,2),':',substr(ckitm,3,2)) as zjsj,loc as zjxx_loc,substr(xtlrsj,1,19) as xtlrsj_zjxx,substr(cNumber,1,6) as zjxx_xzqhdm from "+TB_ST_ZJXX+" where xtlrsj >= '"+enddate+"' ");	
//		logEntity.addLogMsg("读取从业人员表");		
		return data;	
	}
	//读取订票信息
	public static Dataset<Row> DPXX(SparkSession sparkSession,String enddate) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(zjhm,flightno,flightdate))as addid_dp,rowkey as dpxx_id,rowkey,"
						+ "cnname as dpxx_cnName,flightno as dpxx_flightno,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as dpxx_flightDate,"
						+ "qfsj as dpxx_qfsj,ddsj as dpxx_ddsj,qfjcdm as dpxx_qfjcdm,ddjcdm as dpxx_ddjcdm,"
						+ "zjlx as dpxx_zjlx,zjhm as dpxx_zjhm,substr(xtlrsj,1,19) as xtlrsj_dpxx,substr(zjhm,1,6) as dpxx_xzqhdm,rksj as dpsj from "+TB_ST_DPXX+" where xtlrsj >= '"+enddate+"' ")
				;	
//		logEntity.addLogMsg("读取订票信息");		
		return data;	
	}
	//读取安检信息
	public static Dataset<Row> AJXX(SparkSession sparkSession,String enddate) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(idcard,flightno,flightdate))as addid_aj,rowkey as ajxx_id,idcard as ajxx_idcard,rowkey,"
						+ "name as ajxx_name,flightNo as ajxx_flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as ajxx_flightDate,"
						+ "brdno as ajxx_brdno,from_unixtime(unix_timestamp(sctm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as ajsj,"
						+ "channelNo as ajxx_channelNo,substr(xtlrsj,1,19) as xtlrsj_ajxx,substr(idcard,1,6) as ajxx_xzqhdm from "+TB_ST_AJXX+" where xtlrsj >= '"+enddate+"' ");
//		logEntity.addLogMsg("读取安检信息");		
		return data;	
	}
	
	//读取布控人员信息
	public static Dataset<Row> BKRY(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select * from "+TB_ST_BKRY+" ");	
//		logEntity.addLogMsg("读取布控人员信息");		
		return data;	
	}
	
	//读取字典表
	public static Dataset<Row> ZDB(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as itemId,itemName,itemValue,dictLabel,xtZxbz  from  "+SYS_DICT_ITEM+" ");	
//		logEntity.addLogMsg("读取字典信息");		
		return data;	
	}

	//读取柜台信息表
	public static Dataset<Row> GTXX(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select bkry_id,bkry_xm,bkry_xb,bkry_zjhm from  "+TB_ST_JC+" ");	
//		logEntity.addLogMsg("读取字典信息");		
		return data;	
	}
	
	//读取时间戳表
	public static Dataset<Row> TIMESTAMP(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select max(endtime) as timestampx from  "+TB_ST_TIMESTAMP+" where id ='endtimestamp' ");	
//		logEntity.addLogMsg("读取字典信息");		
		return data;	
	}
	
	//读取时间戳表
	public static Dataset<Row> TIMESTAMP2(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select max(endtime) as timestampx from  "+TB_ST_TIMESTAMP2+" ");	
//		logEntity.addLogMsg("读取字典信息");		
		return data;	
	}

}


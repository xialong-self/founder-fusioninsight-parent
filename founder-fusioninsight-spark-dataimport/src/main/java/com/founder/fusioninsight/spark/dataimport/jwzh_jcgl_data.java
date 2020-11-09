package com.founder.fusioninsight.spark.dataimport;

import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.founder.fusioninsight.spark.utils.DateTimeUtils;



public class jwzh_jcgl_data  {

	String	endDate = DateTimeUtils.DateToString(new Date(), DateTimeUtils.YYYY_MM_DD);
	String tjzq = DateTimeUtils.DateToString(DateTimeUtils.addDays(DateTimeUtils.StringToDate(endDate, DateTimeUtils.YYYY_MM_DD), -1), DateTimeUtils.YYYY_MM_DD);

	
	 static String TB_ST_AJXX="jwzh_jcgl.tb_st_ajxx";//安检信息
	 static String TB_ST_ZJXX="jwzh_jcgl.tb_st_zjxx";//值机信息
	 static String TB_ST_DPXX="jwzh_jcgl.tb_st_dpxx";//订票信息
	 static String TB_ST_DJXX="jwzh_jcgl.tb_st_djxx";//登机信息
	 static String TB_ST_ZDRY="jwzh_jcgl.tb_st_bdmx";//暴恐人员
	 static String SYS_DICT_ITEM="jwzh_jcgl.sys_dict_item";//字典表
	 static String TB_ST_JC="jwzh_jcgl.jcgl_gtxx";//柜台表
	 static String TB_ST_TIMESTAMP="jwzh_jcgl.xl_timestamp";//时间戳表
	 static String TB_ST_SJLY="jwzh_jcgl.tb_st_sjly";//数据来源表
	 static String TB_ST_JGXX="jwzh_jcgl.tb_st_jgxx";//进港来源表
	 static String TB_ST_BDMX="jwzh_jcgl.tb_st_bdmx";//比对明细表
	 static String MSG_STORAGE="jwzh_jcgl.msg_storage";//MSG_STORAGE表
	 static String MSG_STORAGE_LOG="jwzh_jcgl.msg_storage_log";//MSG_STORAGE_LOG表
	

	//读取登机信息
	public static Dataset<Row> DJXX(SparkSession sparkSession) {
		Dataset<Row> djxx=sparkSession
				.sql("select upper(concat(cNumber,flightno,flightdate))as addid_dj,rowkey,"
						+ "rowkey as djxx_id,cType as djxx_cType,cNumber as djxx_cNumber,"
						+ "cnName as djxx_cnName,flightNo as djxx_flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as djxx_flightDate,"
						+ "gate as djxx_gate,from_unixtime(unix_timestamp(optm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as djsj ,"
						+ "brdno as djxx_brdno,brdst as djxx_brdst,seat as djxx_seat,substr(xtlrsj,1,19) as xtlrsj_djxx,substr(cNumber,1,6) as djxx_xzqhdm from "+TB_ST_DJXX+" ");	
//		logEntity.addLogMsg("读取登机信息");		
		return djxx;	
	}
	//读取值机信息
	public static Dataset<Row> ZJXX(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(cNumber,flightno,flightdate))as addid_zj,rowkey as zjxx_id,rowkey,"
						+ "cType as zjxx_cType,cNumber as zjxx_cNumber,cnName as zjxx_cnName,"
						+ "gender as zjxx_gender,flightNo as zjxx_flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as zjxx_flightDate,"
						+ "seat as zjxx_seat,brdno as zjxx_brdno,concat(substr(ckitm,1,2),':',substr(ckitm,3,2)) as zjsj,loc as zjxx_loc,substr(xtlrsj,1,19) as xtlrsj_zjxx,substr(cNumber,1,6) as zjxx_xzqhdm from "+TB_ST_ZJXX+" ");	
//		logEntity.addLogMsg("读取从业人员表");		
		return data;	
	}
	//读取订票信息
	public static Dataset<Row> DPXX(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(zjhm,flightno,flightdate))as addid_dp,rowkey as dpxx_id,rowkey,"
						+ "cnname as dpxx_cnName,flightno as dpxx_flightno,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as dpxx_flightDate,"
						+ "qfsj as dpxx_qfsj,ddsj as dpxx_ddsj,qfjcdm as dpxx_qfjcdm,ddjcdm as dpxx_ddjcdm,"
						+ "zjlx as dpxx_zjlx,zjhm as dpxx_zjhm,substr(xtlrsj,1,19) as xtlrsj_dpxx,substr(zjhm,1,6) as dpxx_xzqhdm,rksj as dpsj from "+TB_ST_DPXX+" ");	
//		logEntity.addLogMsg("读取订票信息");		
		return data;	
	}
	//读取安检信息
	public static Dataset<Row> AJXX(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(idcard,flightno,flightdate))as addid_aj,rowkey as ajxx_id,idcard as ajxx_idcard,rowkey,"
						+ "name as ajxx_name,flightNo as ajxx_flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as ajxx_flightDate,"
						+ "brdno as ajxx_brdno,from_unixtime(unix_timestamp(sctm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as ajsj,"
						+ "channelNo as ajxx_channelNo,substr(xtlrsj,1,19) as xtlrsj_ajxx,substr(idcard,1,6) as ajxx_xzqhdm from "+TB_ST_AJXX+"  ");
//		logEntity.addLogMsg("读取安检信息");		
		return data;	
	}
	
	//读取布控人员信息
	public static Dataset<Row> ZDRY(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select upper(concat(zjhm,translate(flate,'-','')))as addid_bdmx,zjhm,sjly,substr(xtlrsj,1,19) as xtlrsj,xtzxbz from "+TB_ST_ZDRY+" ");
//		logEntity.addLogMsg("读取布控人员信息");		
		return data;	
	}
	//全字段预警人员
	public static Dataset<Row> YJRY(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,upper(concat(zjhm,translate(flate,'-','')))as addid_bdmx,jgid,jcid,sjlyid,flate,bzxxid,bkryid,fshj,fssj,name,cnName,"
						+ "zjlx,zjhm,sjly,rwgn,fsdd,rylx,ryxl,substr(xtlrsj,1,19) as xtlrsj,xtLrrxm,"
						+ "xtLrrid,xtLrip,xtLrrbm,xtLrrbmid,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,"
						+ "xtZhxgrid,xtZhxgip,xtZhxgrbm,xtZhxgrbmid,xtZxbz,xtZxyy,rydm,ztunique,data from "+TB_ST_ZDRY+" ");

		return data;	
	}

	//数据来源表
	public static Dataset<Row> SJLY(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,jcid,bzxxid,flate,source,sourceid,name,cnName,zjlx,zjhm,fshj,fssj,czdd,rysx,bdlj,zxlj,sjzt,ztbd,zdbd,bkbd,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrip,xtLrrbm,xtLrrbmid," +
						"substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid,xtZhxgip,xtZhxgrbm,xtZhxgrbmid,xtZxbz,xtZxyy from "+TB_ST_SJLY+" ");

		return data;
	}

	//JGXX表
	public static Dataset<Row> JGXX(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as id,upper(concat(zjhm,flightno,flightdate))as addid_jgxx,snId,flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate,qfjcdm,ddjcdm,qfsj,ddsj,name,cnName,lkzt,csrq,xb,zjhm,substr(zjhm,1,6) as xzqhdm,fjgj,rksj," +
						"mesState,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrrbm,xtLrrbmid,xtLrip,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid," +
						"xtZhxgrbm,xtZhxgrbmid,xtZhxgip,xtZxbz,xtZxyy from "+TB_ST_JGXX+" ");

		return data;
	}

	//MSG_STORAGE表
	public static Dataset<Row> MSG_STORAGE(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as msg_storage_id ,bussId as msg_storage_bussId ,sendChannel as msg_storage_sendChannel ,sendData as msg_storage_sendData ,sendStatus as msg_storage_sendStatus ," +
						"sendTime as msg_storage_sendTime ,recievers as msg_storage_recievers, recieversName as msg_storage_recieversName ,recieveStatus as msg_storage_recieveStatus ,recieverId as msg_storage_recieverId ," +
						"recieverName as msg_storage_recieverName ,recieverOrgCode as msg_storage_recieverOrgCode ,recieverOrgName as msg_storage_recieverOrgName ,receiveNote as msg_storage_receiveNote ,receiveTime as msg_storage_receiveTime ," +
						"disposers as msg_storage_disposers ,disposeStatus as msg_storage_disposeStatus ,disposeType as msg_storage_disposeType ,yjdj as msg_storage_yjdj ,disposeId as msg_storage_disposeId ," +
						"disposeName as msg_storage_disposeName ,disposeOrgCode as msg_storage_disposeOrgCode ,disposeOrgName as msg_storage_disposeOrgName ,disposeNote as msg_storage_disposeNote ," +
						"disposeTime as msg_storage_disposeTime ,disposeDeadLine as msg_storage_disposeDeadLine ,disposeDeadLineStatus as msg_storage_disposeDeadLineStatus ,disposeDeadLineNoticeStatus as msg_storage_disposeDeadLineNoticeStatus ," +
						"retryCounts as msg_storage_retryCounts ,msgType as msg_storage_msgType ,xtLrsj as msg_storage_xtLrsj ,xtLrrxm as msg_storage_xtLrrxm, xtLrrid as msg_storage_xtLrrid ,xtLrrbm as msg_storage_xtLrrbm ," +
						"xtLrrbmid as msg_storage_xtLrrbmid ,xtLrip as msg_storage_xtLrip ,xtZhxgsj as msg_storage_xtZhxgsj ,xtZhxgrxm as msg_storage_xtZhxgrxm , xtZhxgrid as msg_storage_xtZhxgrid ," +
						"xtZhxgrbm as msg_storage_xtZhxgrbm ,xtZhxgrbmid as msg_storage_xtZhxgrbmid ,xtZhxgip as msg_storage_xtZhxgip ,xtZxbz as msg_storage_xtZxbz ,xtZxyy as msg_storage_xtZxyy ," +
						"xtZxyybz as msg_storage_xtZxyybz ,czfs as msg_storage_czfs  from "+MSG_STORAGE+" ");

		return data;
	}

	//MSG_STORAGE_LOG表
	public static Dataset<Row> MSG_STORAGE_LOG(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as msg_storage_log_id,msgId as msg_storage_log_msgId ,bussId as msg_storage_log_bussId ,sendChannel as msg_storage_log_sendChannel ,recieverId as msg_storage_log_recieverId ,sendData as msg_storage_log_sendData ," +
						"sendStatus as msg_storage_log_sendStatus ,sendTime as msg_storage_log_sendTime ,recievers as msg_storage_log_recievers ,recieveStatus as msg_storage_log_recieveStatus ,receiveNote as msg_storage_log_receiveNote ,recieverName as msg_storage_log_recieverName, " +
						"recieverOrgCode as msg_storage_log_recieverOrgCode ,recieverOrgName as msg_storage_log_recieverOrgName ,recieveTime as msg_storage_log_recieveTime ,disposers as msg_storage_log_disposers," +
						"disposeAvailable as msg_storage_log_disposeAvailable ,disposeStatus as msg_storage_log_disposeStatus ,disposeType as msg_storage_log_disposeType ,disposeId as msg_storage_log_disposeId ," +
						"disposeName as msg_storage_log_disposeName, disposeOrgCode as msg_storage_log_disposeOrgCode, disposeOrgName as msg_storage_log_disposeOrgName ,disposeNote as msg_storage_log_disposeNote ," +
						"disposeTime as msg_storage_log_disposeTime ,disposeDeadLine as msg_storage_log_disposeDeadLine, disposeDeadLineStatus as msg_storage_log_disposeDeadLineStatus ,disposeDeadLineNoticeStatus as msg_storage_log_disposeDeadLineNoticeStatus ," +
						"retryCounts as msg_storage_log_retryCounts ,msgType as msg_storage_log_msgType ,xtLrsj as msg_storage_log_xtLrsj ,xtLrrxm as msg_storage_log_xtLrrxm, xtLrrid as msg_storage_log_xtLrrid ," +
						"xtLrrbm as msg_storage_log_xtLrrbm ,xtLrrbmid as msg_storage_log_xtLrrbmid ,xtLrip as msg_storage_log_xtLrip ,xtZhxgsj as msg_storage_log_xtZhxgsj, xtZhxgrxm as msg_storage_log_xtZhxgrxm, " +
						"xtZhxgrid as msg_storage_log_xtZhxgrid ,xtZhxgrbm as msg_storage_log_xtZhxgrbm ,xtZhxgrbmid as msg_storage_log_xtZhxgrbmid, xtZhxgip as msg_storage_log_xtZhxgip ,xtZxbz as msg_storage_log_xtZxbz ,xtZxyy as msg_storage_log_xtZxyy ,xtZxyybz as msg_storage_log_xtZxyybz  from "+MSG_STORAGE_LOG+" ");
		return data;
	}

	//BDMX表
	public static Dataset<Row> BDMX(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select rowkey as tb_st_bdmx_id ,jgid as tb_st_bdmx_jgid,jcid as tb_st_bdmx_jcid,sjlyid as tb_st_bdmx_sjlyid,flate as tb_st_bdmx_flate,bzxxid as tb_st_bdmx_bzxxid,bkryid as tb_st_bdmx_bkryid," +
						"fshj as tb_st_bdmx_fshj,fssj as tb_st_bdmx_fssj ,name as tb_st_bdmx_name,cnName as tb_st_bdmx_cnName,zjlx as tb_st_bdmx_zjlx,zjhm as tb_st_bdmx_zjhm,sjly as tb_st_bdmx_sjly," +
						"rwgn as tb_st_bdmx_rwgn,fsdd as tb_st_bdmx_fsdd,ztunique as tb_st_bdmx_ztunique,rylx as tb_st_bdmx_rylx,ryxl as tb_st_bdmx_ryxl,rydm as tb_st_bdmx_rydm,data as tb_st_bdmx_data," +
						"xtLrsj as tb_st_bdmx_xtLrsj,xtLrrxm as tb_st_bdmx_xtLrrxm,xtLrrid as tb_st_bdmx_xtLrrid,xtLrip as tb_st_bdmx_xtLrip,xtLrrbm as tb_st_bdmx_xtLrrbm,xtLrrbmid as tb_st_bdmx_xtLrrbmid," +
						"xtZhxgsj as tb_st_bdmx_xtZhxgsj,xtZhxgrxm as tb_st_bdmx_xtZhxgrxm,xtZhxgrid as tb_st_bdmx_xtZhxgrid,xtZhxgip as tb_st_bdmx_xtZhxgip,xtZhxgrbm as tb_st_bdmx_xtZhxgrbm," +
						"xtZhxgrbmid as tb_st_bdmx_xtZhxgrbmid,xtZxbz as tb_st_bdmx_xtZxbz,xtZxyy as tb_st_bdmx_xtZxyy from "+TB_ST_BDMX+" ");

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
				.sql("select rowkey as id,djk,djk_pid,zjgt,zjgt_pid,ajtdh,ajtdmc,jcszdm,jcm,sfxzqh,sfm from  "+TB_ST_JC+" ");	
//		logEntity.addLogMsg("读取字典信息");		
		return data;	
	}
	
	//读取时间戳表
	public static Dataset<Row> TIMESTAMP(SparkSession sparkSession) {
		Dataset<Row> data=sparkSession
				.sql("select id,max(endtime) from  "+TB_ST_TIMESTAMP+" ");	
//		logEntity.addLogMsg("读取字典信息");		
		return data;	
	}

}


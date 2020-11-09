package com.founder.fusioninsight.spark.dataimport.incremental;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;

/*
 *  机场项目：宽表增量计算中取出ES数据和新数据进行比对
 * 2020-07-01
 * */

public class esDataGlzh {
	
	//剔除无效证件号码
	public static Dataset<Row> DataZh(Dataset<Row> data,SparkSession sparkSession) {
		data.cache();
		System.out.println("------------------------ES开始------------------------");

	    Dataset<Row> esdata=EsSparkSQL.esDF(sparkSession, "jwzh_jcgl.all/_doc")
//				.filter("addid in (select addid from table)")
				.repartition(100);
	    
	    //字段重命名
	    Dataset<Row> field=data
	.selectExpr("addid as addid_new","dpxx_qfsj as dpxx_qfsj_new","dpxx_ddsj as dpxx_ddsj_new","zjhm as zjhm_new",
	    		"xtlrsj as xtlrsj_new","dpsj as dpsj_new","zjxx_gender as zjxx_gender_new","seat as seat_new","brdno as brdno_new",
	    		"zjxx_loc as zjxx_loc_new","ajsj as ajsj_new","ajxx_channelno as ajxx_channelno_new","djxx_gate as djxx_gate_new","djsj as djsj_new",
	    		"djxx_brdst as djxx_brdst_new","dpxx_qfjcdm as dpxx_qfjcdm_new","qfjcdm_mc as qfjcdm_mc_new","bs as bs_new",
	    		"dpxx_ddjcdm as dpxx_ddjcdm_new","ddjcdm_mc as ddjcdm_mc_new","zjlxdm_mc as zjlxdm_mc_new","zjlxdm as zjlxdm_new","xzqh as xzqh_new",
	    		"xb as xb_new","zjsj as zjsj_new","channelno_mc as channelno_mc_new","loc_mc as loc_mc_new","tjsj as tjsj_new","qfjcsfxzqh as qfjcsfxzqh_new","qfjcsfm as qfjcsfm_new",
			    "ddjcsfxzqh as ddjcsfxzqh_new","ddjcsfm as ddjcsfm_new","dpxx_id as dpxx_id_new","zjxx_id as zjxx_id_new","ajxx_id as ajxx_id_new ","djxx_id as djxx_id_new");
	    
	    //有效ES数据和新数据关联
	    Dataset<Row> EsdataJoinNew=field.join(esdata,field.col("addid_new").equalTo(esdata.col("addid")),"left").cache();
	    System.out.println("------------------------关联完成------------------------");
	    EsdataJoinNew.createOrReplaceTempView("es_table");
		Dataset<Row> dataxb=sparkSession
				.sql("select id,addid_new as addid,cnname,zjhm,flightno,flightdate,xzqhdm,zjlxdm_mc,zjlxdm,xzqh,xb,tjsj_new as tjsj,bs,"
						+ "case  when zjxx_id is null then zjxx_id_new else concat (zjxx_id,',',zjxx_id_new) end zjxx_id,"
						+ "case  when djxx_id is null then djxx_id_new else concat (djxx_id,',',djxx_id_new) end djxx_id,"
						+ "case  when dpxx_id is null then dpxx_id_new else concat (dpxx_id,',',dpxx_id_new) end dpxx_id,"
						+ "case  when ajxx_id is null then ajxx_id_new else concat (ajxx_id,',',ajxx_id_new) end ajxx_id,"
						+ "case  when dpxx_qfsj is null then dpxx_qfsj_new else dpxx_qfsj end dpxx_qfsj,"
						+ "case  when xtlrsj is null then xtlrsj_new else xtlrsj end xtlrsj,"
						+ "case  when dpsj is null then dpsj_new else dpsj end dpsj,"
						+ "case  when ajsj is null then ajsj_new else ajsj end ajsj,"
						+ "case  when djsj is null then djsj_new else djsj end djsj,"
						+ "case  when zjxx_gender is null then zjxx_gender_new else zjxx_gender end zjxx_gender,"
						+ "case  when seat is null then seat_new else seat end seat,"
						+ "case  when brdno is null then brdno_new else brdno end brdno,"
						+ "case  when zjxx_loc is null then zjxx_loc_new else zjxx_loc end zjxx_loc,"
						+ "case  when ajxx_channelno is null then ajxx_channelno_new else ajxx_channelno end ajxx_channelno,"
						+ "case  when djxx_gate is null then djxx_gate_new else djxx_gate end djxx_gate,"
						+ "case  when djxx_brdst is null then djxx_brdst_new else djxx_brdst end djxx_brdst,"
						+ "case  when dpxx_ddsj is null then dpxx_ddsj_new else dpxx_ddsj end dpxx_ddsj,"
						+ "case  when channelno_mc is null then channelno_mc_new else channelno_mc end channelno_mc,"
						+ "case  when loc_mc is null then loc_mc_new else loc_mc end loc_mc,"
						+ "case  when dpxx_qfjcdm is null then dpxx_qfjcdm_new else dpxx_qfjcdm end dpxx_qfjcdm,"
						+ "case  when qfjcdm_mc is null then qfjcdm_mc_new else qfjcdm_mc end qfjcdm_mc,"
						+ "case  when dpxx_ddjcdm is null then dpxx_ddjcdm_new else dpxx_ddjcdm end dpxx_ddjcdm,"
						+ "case  when ddjcdm_mc is null then ddjcdm_mc_new else ddjcdm_mc end ddjcdm_mc,"
//						+ "case  when rylx_zt is null then rylx_zt_new else rylx_zt end rylx_zt,"
//						+ "case  when rylx_bk is null then rylx_bk_new else rylx_bk end rylx_bk,"
//						+ "case  when rylx_zd is null then rylx_zd_new else rylx_zd end rylx_zd,"
						+ "case  when zjsj is null then zjsj_new else zjsj end zjsj, "	
						+ "case  when qfjcsfxzqh is null then qfjcsfxzqh_new else qfjcsfxzqh end qfjcsfxzqh, "	
						+ "case  when qfjcsfm is null then qfjcsfm_new else qfjcsfm end qfjcsfm, "	
						+ "case  when ddjcsfxzqh is null then ddjcsfxzqh_new else ddjcsfxzqh end ddjcsfxzqh, "	
						+ "case  when ddjcsfm is null then ddjcsfm_new else ddjcsfm end ddjcsfm "	
						+ " from es_table");
		System.out.println("------------------------筛选完成------------------------");
		//取出没关联上的部分
		data.createOrReplaceTempView("data_table");
		dataxb.select("zjhm").createOrReplaceTempView("data2_table");
		System.out.println("------------------------注册临时表完成------------------------");
		Dataset<Row> dataExcept_new=sparkSession.sql("select * from data_table where zjhm not in (select zjhm from data2_table)");

		//合并两部分数据
		Dataset<Row> dataAdd_all=dataxb.unionByName(dataExcept_new);
		
		System.out.println("------------------------输出------------------------");
		

	
		
		return dataAdd_all;
	}

}

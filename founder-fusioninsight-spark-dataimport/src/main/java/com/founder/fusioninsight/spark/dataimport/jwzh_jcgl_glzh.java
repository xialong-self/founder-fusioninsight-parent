package com.founder.fusioninsight.spark.dataimport;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class jwzh_jcgl_glzh {
	

	
		//dpxx为准
	public	static Dataset<Row> joinOne(SparkSession sparkSession,Dataset<Row> data1,Dataset<Row> data2,Dataset<Row> data3,Dataset<Row> data4) {
		//删除掉非当前主表的rowkey字段
				Dataset<Row> data2new=data2.drop("rowkey");
				Dataset<Row> data3new=data3.drop("rowkey");
				Dataset<Row> data4new=data4.drop("rowkey");
				String bsname="dpxx";
				
				Dataset<Row> data_join1=data1.join(data2new,data1.col("addid_dp").equalTo(data2new.col("addid_zj")),"left")
						.withColumnRenamed("addid_dp", "addid_dp2");
				Dataset<Row> data_join2=data_join1.join(data3new,data_join1.col("addid_dp2").equalTo(data3new.col("addid_aj")),"left")
						.withColumnRenamed("addid_dp2", "addid_dp3");
				Dataset<Row> data_join3=data_join2.join(data4new,data_join2.col("addid_dp3").equalTo(data4new.col("addid_dj")),"left")
						.withColumnRenamed("addid_dp3", "addid_dp");
				//优先表的id字段作为id 
				Dataset<Row> data_join_all=data_join3.withColumnRenamed("rowkey","id").withColumn("bs", lit(bsname)) 
						.withColumnRenamed("dpxx_zjlx", "zjlxdm")
						.withColumnRenamed("dpxx_zjhm", "zjhm")
						.withColumnRenamed("dpxx_cnName", "cnName")
						.withColumnRenamed("dpxx_flightno", "flightno")
						.withColumnRenamed("dpxx_flightDate", "flightDate")
						.withColumnRenamed("xtlrsj_dpxx", "xtlrsj")
						.withColumnRenamed("dpxx_xzqhdm", "xzqhdm")
						.withColumnRenamed("addid_dp", "addid")
						.withColumnRenamed("zjxx_brdno", "brdno")
						.withColumnRenamed("djxx_seat", "seat")
				.select("id","addid","cnName","flightno","flightDate","dpxx_qfsj","dpxx_ddsj","zjhm","xtlrsj",
						"xzqhdm","dpsj","zjxx_gender","seat","brdno","zjsj","zjxx_loc",
						"ajsj","ajxx_channelNo","djxx_gate","djsj","djxx_brdst",
						"dpxx_qfjcdm","bs","dpxx_ddjcdm","zjlxdm","dpxx_id","zjxx_id","ajxx_id","djxx_id");
				

				return data_join_all;
		    } 


	/**
	 * zjxx为准
	 * @param sparkSession
	 * @param data1
	 * @param data2
	 * @param data3
	 * @param data4
	 * @return
	 */
		public static Dataset<Row> joinTwo(SparkSession sparkSession,Dataset<Row> data1,Dataset<Row> data2,Dataset<Row> data3,Dataset<Row> data4) {
				//删除掉非当前主表的rowkey字段
				Dataset<Row> data2new=data2.drop("rowkey");
				Dataset<Row> data3new=data3.drop("rowkey");
				Dataset<Row> data4new=data4.drop("rowkey");
				String bsname="zjxx";

				Dataset<Row> data_join1=data1.join(data2new,data1.col("addid_zj").equalTo(data2new.col("addid_dp")),"left")
						.withColumnRenamed("addid_zj", "addid_zj2");
				Dataset<Row> data_join2=data_join1.join(data3new,data_join1.col("addid_zj2").equalTo(data3new.col("addid_aj")),"left")
						.withColumnRenamed("addid_zj2", "addid_zj3");
				Dataset<Row> data_join3=data_join2.join(data4new,data_join2.col("addid_zj3").equalTo(data4new.col("addid_dj")),"left")
						.withColumnRenamed("addid_zj3", "addid_zj");
				//优先表的id字段作为id 
				Dataset<Row> data_join_all=data_join3.withColumnRenamed("rowkey","id").withColumn("bs", lit(bsname))
						.withColumnRenamed("zjxx_cType", "zjlxdm")
						.withColumnRenamed("zjxx_cNumber", "zjhm")
						.withColumnRenamed("zjxx_cnName", "cnName")
						.withColumnRenamed("zjxx_flightNo", "flightno")
						.withColumnRenamed("zjxx_flightDate", "flightDate")
						.withColumnRenamed("xtlrsj_zjxx", "xtlrsj")
						.withColumnRenamed("zjxx_xzqhdm", "xzqhdm")
						.withColumnRenamed("addid_zj", "addid")
						.withColumnRenamed("zjxx_brdno", "brdno")
						.withColumnRenamed("zjxx_seat", "seat")
						.filter("addid_dp is null")

				.select("id","addid","cnName","flightno","flightDate","dpxx_qfsj","dpxx_ddsj","zjhm","xtlrsj",
						"xzqhdm","dpsj","zjxx_gender","seat","brdno","zjsj","zjxx_loc",
						"ajsj","ajxx_channelNo","djxx_gate","djsj","djxx_brdst",
						"dpxx_qfjcdm","bs","dpxx_ddjcdm","zjlxdm","dpxx_id","zjxx_id","ajxx_id","djxx_id");
				return data_join_all;
		    } 
			

	/**
	 * ajxx为准
	 * @param sparkSession
	 * @param data1
	 * @param data2
	 * @param data3
	 * @param data4
	 * @return
	 */
	public static Dataset<Row> joinThree(SparkSession sparkSession,Dataset<Row> data1,Dataset<Row> data2,Dataset<Row> data3,Dataset<Row> data4) {
				//删除掉非当前主表的rowkey字段
				Dataset<Row> data2new=data2.drop("rowkey");
				Dataset<Row> data3new=data3.drop("rowkey");
				Dataset<Row> data4new=data4.drop("rowkey");
				String bsname="ajxx";

				Dataset<Row> data_join1=data1.join(data2new,data1.col("addid_aj").equalTo(data2new.col("addid_zj")),"left")
						.withColumnRenamed("addid_aj", "addid_aj2");
				Dataset<Row> data_join2=data_join1.join(data3new,data_join1.col("addid_aj2").equalTo(data3new.col("addid_dp")),"left")
						.withColumnRenamed("addid_aj2", "addid_aj3");
				Dataset<Row> data_join3=data_join2.join(data4new,data_join2.col("addid_aj3").equalTo(data4new.col("addid_dj")),"left")
						.withColumnRenamed("addid_aj3", "addid_aj");
				//优先表的id字段作为id 
				Dataset<Row> data_join_all=data_join3.withColumnRenamed("rowkey","id").withColumn("bs", lit(bsname))
						.withColumn("zjlxdm",lit(""))
						.withColumnRenamed("ajxx_idcard", "zjhm")
						.withColumnRenamed("ajxx_name", "cnName")
						.withColumnRenamed("ajxx_flightNo", "flightno")
						.withColumnRenamed("ajxx_flightDate", "flightDate")
						.withColumnRenamed("xtlrsj_ajxx", "xtlrsj")
						.withColumnRenamed("ajxx_xzqhdm", "xzqhdm")
						.withColumnRenamed("addid_aj", "addid")
						.withColumnRenamed("ajxx_brdno", "brdno")
						.withColumnRenamed("djxx_seat", "seat")
						.filter("addid_dp is null and addid_zj is null ")

						.select("id","addid","cnName","flightno","flightDate","dpxx_qfsj","dpxx_ddsj","zjhm","xtlrsj",
								"xzqhdm","dpsj","zjxx_gender","seat","brdno","zjsj","zjxx_loc",
								"ajsj","ajxx_channelNo","djxx_gate","djsj","djxx_brdst",
								"dpxx_qfjcdm","bs","dpxx_ddjcdm","zjlxdm","dpxx_id","zjxx_id","ajxx_id","djxx_id");
				return data_join_all;
		    } 
			

	/**
	 * djxx为准
	 * @param sparkSession
	 * @param data1
	 * @param data2
	 * @param data3
	 * @param data4
	 * @return
	 */
	public static Dataset<Row> joinFour(SparkSession sparkSession,Dataset<Row> data1,Dataset<Row> data2,Dataset<Row> data3,Dataset<Row> data4) {
				//删除掉非当前主表的rowkey字段
				Dataset<Row> data2new=data2.drop("rowkey");
				Dataset<Row> data3new=data3.drop("rowkey");
				Dataset<Row> data4new=data4.drop("rowkey");
				String bsname="djxx";

				Dataset<Row> data_join1=data1.join(data2new,data1.col("addid_dj").equalTo(data2new.col("addid_zj")),"left")
						.withColumnRenamed("addid_dj", "addid_dj2");
				Dataset<Row> data_join2=data_join1.join(data3new,data_join1.col("addid_dj2").equalTo(data3new.col("addid_aj")),"left")
						.withColumnRenamed("addid_dj2", "addid_dj3");
				Dataset<Row> data_join3=data_join2.join(data4new,data_join2.col("addid_dj3").equalTo(data4new.col("addid_dp")),"left")
						.withColumnRenamed("addid_dj3", "addid_dj");
				//优先表的id字段作为id 
				Dataset<Row> data_join_all=data_join3.withColumnRenamed("rowkey","id").withColumn("bs", lit(bsname))
						.withColumnRenamed("djxx_cType", "zjlxdm")
						.withColumnRenamed("djxx_cNumber", "zjhm")
						.withColumnRenamed("djxx_cnName", "cnName")
						.withColumnRenamed("djxx_flightNo", "flightno")
						.withColumnRenamed("djxx_flightDate", "flightDate")
						.withColumnRenamed("xtlrsj_djxx", "xtlrsj")
						.withColumnRenamed("djxx_xzqhdm", "xzqhdm")
						.withColumnRenamed("addid_dj", "addid")
						.withColumnRenamed("djxx_brdno", "brdno")
						.withColumnRenamed("djxx_seat", "seat")
						.filter("addid_dp is null and addid_zj is null and addid_aj is null")

						.select("id","addid","cnName","flightno","flightDate","dpxx_qfsj","dpxx_ddsj","zjhm","xtlrsj",
								"xzqhdm","dpsj","zjxx_gender","seat","brdno","zjsj","zjxx_loc",
								"ajsj","ajxx_channelNo","djxx_gate","djsj","djxx_brdst",
								"dpxx_qfjcdm","bs","dpxx_ddjcdm","zjlxdm","dpxx_id","zjxx_id","ajxx_id","djxx_id");
				return data_join_all;
		    } 


}

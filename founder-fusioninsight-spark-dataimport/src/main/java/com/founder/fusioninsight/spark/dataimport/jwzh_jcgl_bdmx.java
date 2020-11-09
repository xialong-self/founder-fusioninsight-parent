package com.founder.fusioninsight.spark.dataimport;

import java.util.HashMap;

import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import com.founder.fusioninsight.config.FusioninsightConfig;

/**
 *  机场项目：jwzh_jcgl.tb_st_dbmx表迁移
 * @author xialong
 */


public class jwzh_jcgl_bdmx {
	
	public static void main(String[] args) {
		FusioninsightConfig.initArgs(args);
		new jwzh_jcgl_bdmx().run();
	}
	public void run() {
		SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl.bdmx");
	//原数据

	Dataset<Row> yjry_y=jwzh_jcgl_data.YJRY(sparkSession).cache();
	new SaveToEsUtils().saveTjjgToEs_zj(yjry_y,"jwzh_jcgl.tb_st_bdmx");
	yjry_y.unpersist();
    sparkSession.stop();
	}
	

}

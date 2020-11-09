package com.founder.fusioninsight.spark.dataimport.dataOnly;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_data;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.HashMap;

/**
 * @author 夏龙
 * @date 2020-08-31
 */
public class jwzh_jcgl_sjly {
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_sjly().run();
    }
    public void run() {

        SparkSession sparkSession =ConnectionUtils.sparkSessionConnection("jwzh_jcgl_sjly");
        Dataset<Row> sjly= jwzh_jcgl_data.SJLY(sparkSession)
                .selectExpr("id","source","sourceid","cnName","zjhm","fssj","xtLrsj","xtZxbz");
        Dataset<Row> bdmx=sparkSession.read().format("org.elasticsearch.spark.sql").load("jwzh_jcgl.tb_st_bdmx/_doc")
                .selectExpr("id as bdmxid","sjlyid","sjly as rylx");
        bdmx.createOrReplaceTempView("sjlyTable");
        Dataset<Row> bdmxyx=sparkSession.sql("select sjlyid,concat_ws(',',collect_set(rylx)) as rylx from sjlyTable group by sjlyid");
        Dataset<Row> data=bdmxJoinsjly(bdmxyx,sjly);
        new SaveToEsUtils().saveTjjgToEs_zj(data,"jwzh_jcgl.tb_st_sjly");
        sparkSession.stop();

    }


    public static Dataset<Row> bdmxJoinsjly(Dataset<Row> bdmx,Dataset<Row> sjly) {
        Dataset<Row> data=bdmx.join(sjly,bdmx.col("sjlyid").equalTo(sjly.col("id"))).filter("id is not null")
                .selectExpr("id","source","sourceid","rylx","cnName","zjhm","fssj","xtLrsj","xtZxbz");
      return data;
    }



}

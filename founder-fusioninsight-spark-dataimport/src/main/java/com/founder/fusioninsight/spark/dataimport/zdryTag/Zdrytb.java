package com.founder.fusioninsight.spark.dataimport.zdryTag;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;

/**
 * @author 夏龙
 * @date 2020-09-03
 */
public class Zdrytb {
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new Zdrytb().run();
    }

    public void run(){
        SparkSession sparkSession=ConnectionUtils.sparkSessionConnection("jwzh_jcgl_zdrytb");
        Dataset<Row> sjlyEsData= EsSparkSQL.esDF(sparkSession, "jwzh_jcgl.tb_st_sjly/_doc")
                .selectExpr("source","sourceid","rylx").cache();


        Dataset<Row> dpxx_sc=sjlyEsData.filter("source='tb_st_dpxx'").selectExpr("sourceid as id","rylx");
        Dataset<Row> ajxx_sc=sjlyEsData.filter("source='tb_st_ajxx'").selectExpr("sourceid as id","rylx");
        Dataset<Row> zjxx_sc=sjlyEsData.filter("source='tb_st_zjxx'").selectExpr("sourceid as id","rylx");
        Dataset<Row> djxx_sc=sjlyEsData.filter("source='tb_st_djxx'").selectExpr("sourceid as id","rylx");
        new SaveToEsUtils().saveTjjgToEs_Append(dpxx_sc,"jwzh_jcgl.tb_st_dpxx");
        new SaveToEsUtils().saveTjjgToEs_Append(ajxx_sc,"jwzh_jcgl.tb_st_ajxx");
        new SaveToEsUtils().saveTjjgToEs_Append(zjxx_sc,"jwzh_jcgl.tb_st_zjxx");
        new SaveToEsUtils().saveTjjgToEs_Append(djxx_sc,"jwzh_jcgl.tb_st_djxx");
        sparkSession.stop();

    }



}

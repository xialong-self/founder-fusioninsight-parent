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
public class jwzh_jcgl_jgxx {

    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_jgxx().run();
    }
    public void run() {
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl_jgxxALL");
        //原数据
        Dataset<Row> gtxxdata_y= jwzh_jcgl_data.GTXX(sparkSession);
        Dataset<Row> zdxxdata_y=jwzh_jcgl_data.ZDB(sparkSession).where("xtZxbz='0' ");
        Dataset<Row> yjry_y=jwzh_jcgl_data.JGXX(sparkSession);
        Dataset<Row> datafy_all=DataChange(yjry_y,zdxxdata_y,gtxxdata_y,sparkSession);
        new SaveToEsUtils().saveTjjgToEs_zj(datafy_all,"jwzh_jcgl.tb_st_jgxx");
        sparkSession.stop();
    }
    //字段翻译
    public static Dataset<Row> DataChange(Dataset<Row> data,Dataset<Row> zddata,Dataset<Row> gtdata,SparkSession sparkSession) {

        //获取字典表各个指标数据
        Dataset<Row> zddata_yx=zddata.where("dictLabel='DB_YW_JCDM'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
        Dataset<Row> zddata_xzqh=zddata.where("dictLabel='DB_GA_XZQH'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
        Dataset<Row> sfdata_zjtd=gtdata.select("jcszdm","sfxzqh","sfm").groupBy("jcszdm","sfxzqh","sfm").count().select("jcszdm","sfxzqh","sfm");
        //根据字典表翻译字段
        Dataset<Row> datajcOne=data.join(zddata_yx,data.col("qfjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","qfjcdm_mc").drop("itemValue");
        Dataset<Row> datajcTwo=datajcOne.join(zddata_yx,datajcOne.col("ddjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","ddjcdm_mc").drop("itemValue");
        Dataset<Row> datajcFour=datajcTwo.join(zddata_xzqh,datajcTwo.col("xzqhdm").equalTo(zddata_xzqh.col("itemValue")),"left").withColumnRenamed("itemName","xzqh").drop("itemValue");

        //人员起飞和到达省份
        Dataset<Row> datagtThree=datajcFour.join(sfdata_zjtd,datajcFour.col("qfjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
                .withColumnRenamed("sfxzqh", "qfjcsfxzqh").withColumnRenamed("sfm", "qfjcsfm").drop("jcszdm");
        Dataset<Row> datagtFour=datagtThree.join(sfdata_zjtd,datagtThree.col("ddjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
                .withColumnRenamed("sfxzqh", "ddjcsfxzqh").withColumnRenamed("sfm", "ddjcsfm").drop("jcszdm")
                .selectExpr("id","addid_jgxx","snId","flightNo","flightDate","qfjcdm","ddjcdm","qfsj","ddsj","name","cnName","lkzt","csrq","xb","zjhm","xzqhdm","fjgj","rksj",
                "mesState","xtLrsj","xtLrrxm","xtLrrid","xtLrrbm","xtLrrbmid","xtLrip","xtZhxgsj","xtZhxgrxm","xtZhxgrid",
                "xtZhxgrbm","xtZhxgrbmid","xtZhxgip","xtZxbz","xtZxyy","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");


        return datagtFour;
    }




}

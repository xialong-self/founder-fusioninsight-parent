package com.founder.fusioninsight.spark.dataimport.zdryTag;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_data;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import javax.xml.crypto.Data;
import java.util.HashMap;

/**
 * @author 夏龙
 * @date 2020-09-04
 * ES写入模式测试
 */
public class Test {
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new Test().run();

    }
    public void run(){
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("Test");



        String endTime = DateTimeUtils.getSystemDateTimeString();
        //取上次结束时间60s之前
        String beforetime= DateTimeUtils.DateToString(DateTimeUtils.addDays(DateTimeUtils.StringToDate(endTime, DateTimeUtils.YYYY_MM_DD_HH_MM_SS), -15), DateTimeUtils.YYYY_MM_DD_HH_MM_SS);


//        Dataset<Row> jgxxdata=JGXX(sparkSession,beforetime)
//                .filter("xtLrsj <='2020-09-05 00:00:00'")
//                .repartition(100).cache();
//        new SaveToEsUtils().saveTjjgToEs_Append(jgxxdata,"jwzh_jcgl.tb_st_jgxx");


        //jgxx更新，根据起飞机场代码更新字段
        Dataset<Row> gtxxdata_y= jwzh_jcgl_data.GTXX(sparkSession).cache();
        Dataset<Row> zdxxdata_y=jwzh_jcgl_data.ZDB(sparkSession).where("xtZxbz='0' ").cache();
        Dataset<Row> jgxx_yx= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_jgxx/_doc")
                .filter("qfjcdm_mc is null and  xtLrsj>='2020-08-30 00:00:00'  and xtLrsj<='2020-09-05 00:00:00' ")
                .selectExpr("id","qfjcdm","ddjcdm");
        Dataset<Row> jgxx_all=DataChange(jgxx_yx,zdxxdata_y,gtxxdata_y);
        new SaveToEsUtils().saveTjjgToEs_Append(jgxx_all,"jwzh_jcgl.tb_st_jgxx");

        sparkSession.stop();












//        Dataset<Row>  test=EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_dpxx/_doc",
//                "{\"constant_score\": {\"filter\": {\"term\": {\"zjhm\": \"\"}}}}")
//                .selectExpr("id","flightNo","flightDate");



//        Dataset<Row> dpxxsc=dpxx.selectExpr("upper(concat(zjhm,flightno,flightdate)) as id","flightNo","from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate");
//        Dataset<Row> ajxx=sparkSession.read().format("org.elasticsearch.spark.sql").load("jwzh_jcgl.tb_st_ajxx/_doc")
//                .selectExpr("sjlyid","id","zjhm");
//
//        Dataset<Row> zjxx=sparkSession.read().format("org.elasticsearch.spark.sql").load("jwzh_jcgl.tb_st_zjxx/_doc")
//                .selectExpr("sjlyid","id","zjhm");
//
//        Dataset<Row> djxx=sparkSession.read().format("org.elasticsearch.spark.sql").load("jwzh_jcgl.tb_st_djxx/_doc")
//                .selectExpr("sjlyid","id","zjhm");

//        Dataset<Row>  test1=sparkSession.sql("select 'alladata_timestamp' as id,'2020-09-07 14:00:00' as endtime,'2020-09-07 14:14:33' as tjzq from jwzh_jcgl.sjc limit 1");
//
//        Dataset<Row>  test2=sparkSession.sql("select 'endtimestamp' as id,'2020-08-31 11:41:59' as endtime,'2020-08-31 11:41:59' as tjzq from jwzh_jcgl.sjc limit 1");
//
//        Dataset<Row> test=test1.unionAll(test2);
//        EsSparkSQL.saveToEs(test,"jwzh_jcgl.sjc/_doc");

//        //存入三个字段
//        Dataset<Row> test1=test.where("id='4336fa82e78c4cb5bd43a074e63cb6d9'").selectExpr("id","flightNo","flightDate");
//        new SaveToEsUtils().saveTjjgToEs_zj(test1,"jwzh_jcgl.test");
//        //append模式写入两个字段，查看是否会覆盖flightNo字段
//        Dataset<Row> test2=test.where("id='4336fa82e78c4cb5bd43a074e63cb6d9'").selectExpr("id","'2020-01-01' as flightDate");
//        new SaveToEsUtils().saveTjjgToEs_Append(test2,"jwzh_jcgl.test");
//        //append模式写入三个字段
//        Dataset<Row> test3=test.where("id='40890a835e41497fb5118bef5a8bb9f4'");
//        new SaveToEsUtils().saveTjjgToEs_Append(test3,"jwzh_jcgl.test");

    }


    //JGXX表
    public static Dataset<Row> JGXX(SparkSession sparkSession,String beforetime) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,upper(concat(zjhm,flightno,flightdate))as addid_jgxx,snId,flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate," +
                        "qfjcdm,ddjcdm,qfsj,ddsj,name,cnName,lkzt,csrq,xb,zjhm,substr(zjhm,1,6) as xzqhdm,fjgj,rksj," +
                        "mesState,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrrbm,xtLrrbmid,xtLrip,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid," +
                        "xtZhxgrbm,xtZhxgrbmid,xtZhxgip,xtZxbz,xtZxyy from jwzh_jcgl.tb_st_jgxx  where xtLrsj >='"+beforetime+"' ");

        return data;
    }



    //字段翻译
    public static Dataset<Row> DataChange(Dataset<Row> data,Dataset<Row> zddata,Dataset<Row> gtdata) {
        //获取字典表各个指标数据
        Dataset<Row> zddata_yx=zddata.where("dictLabel='DB_YW_JCDM'").groupBy("itemValue","itemName").count().select("itemValue","itemName");
        Dataset<Row> sfdata_zjtd=gtdata.select("jcszdm","sfxzqh","sfm").groupBy("jcszdm","sfxzqh","sfm").count().select("jcszdm","sfxzqh","sfm");
        //根据字典表翻译字段
        Dataset<Row> datajcOne=data.join(zddata_yx,data.col("qfjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","qfjcdm_mc").drop("itemValue");
        Dataset<Row> datajcTwo=datajcOne.join(zddata_yx,datajcOne.col("ddjcdm").equalTo(zddata_yx.col("itemValue")),"left").withColumnRenamed("itemName","ddjcdm_mc").drop("itemValue");
        //人员起飞和到达省份
        Dataset<Row> datagtThree=datajcTwo.join(sfdata_zjtd,datajcTwo.col("qfjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
                .withColumnRenamed("sfxzqh", "qfjcsfxzqh").withColumnRenamed("sfm", "qfjcsfm").drop("jcszdm");
        Dataset<Row> datagtFour=datagtThree.join(sfdata_zjtd,datagtThree.col("ddjcdm").equalTo(sfdata_zjtd.col("jcszdm")),"left")
                .withColumnRenamed("sfxzqh", "ddjcsfxzqh").withColumnRenamed("sfm", "ddjcsfm").drop("jcszdm")
                .selectExpr("id","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");

        return datagtFour;
    }


}

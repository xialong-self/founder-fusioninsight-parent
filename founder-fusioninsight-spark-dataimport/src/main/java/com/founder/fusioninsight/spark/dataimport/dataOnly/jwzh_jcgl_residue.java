package com.founder.fusioninsight.spark.dataimport.dataOnly;

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

import java.util.HashMap;

/**
 * @author 夏龙
 * @date 2020-09-03
 */
public class jwzh_jcgl_residue {


    static String TB_ST_ZJXX="jwzh_jcgl.tb_st_zjxx";
    static String TB_ST_DPXX="jwzh_jcgl.tb_st_dpxx";
    static String TB_ST_SJLY="jwzh_jcgl.tb_st_sjly";

    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_residue().run();
    }



    public void run(){
        SparkSession sparkSession=ConnectionUtils.sparkSessionConnection("jwzh_jcgl_residue");

        Dataset<Row> dpxxdata=DPXX(sparkSession).repartition(100);
        Dataset<Row> zjxxdata=ZJXX(sparkSession).repartition(100);

        //原数据
        Dataset<Row> gtxxdata_y= jwzh_jcgl_data.GTXX(sparkSession);
        Dataset<Row> zdxxdata_y=jwzh_jcgl_data.ZDB(sparkSession).where("xtZxbz='0' ");
        new SaveToEsUtils().saveTjjgToEs_zj(dpxxdata,"jwzh_jcgl.tb_st_dpxx");
        new SaveToEsUtils().saveTjjgToEs_zj(zjxxdata,"jwzh_jcgl.tb_st_zjxx");

        sparkSession.stop();

    }

    //字段翻译
    public static Dataset<Row> dpxxChange(Dataset<Row> data,Dataset<Row> zddata,Dataset<Row> gtdata,SparkSession sparkSession) {
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
                .selectExpr("qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");

        return datagtFour;
    }

    //字段翻译
    public static Dataset<Row> zjxxChange(Dataset<Row> data,Dataset<Row> zddata,Dataset<Row> gtdata,SparkSession sparkSession) {
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
                .selectExpr("id","flightNo" ,"flightDate"  ,"name" ,"cnName" ,"cType" ,"cNumber"  ,"gender" ,"seat" ,"brdno" ,
                        "ckitm" ,"xzqhdm","dept" ,"dest" ,"leval" ,"depttm" ,"inf"  ,"dttm" ,"ckipid" ,"ckiofc" ,"ckiagt" ,"sndr" ,
                        "seqn" ,"type" ,"styp" ,"loc"  ,"prsta" ,"gates" ,"ctct" ,"etFl" ,"etNo" ,"isGroup"  ,"bsgs" ,"bagWgt" ,"sip"  ,
                        "wetm" ,"mesState" ,"appId" ,"xtLrsj" ,"xtLrrxm"  ,"xtLrrid"  ,"xtLrip" ,"xtLrrbm"  ,"xtLrrbmid" ,"xtZhxgsj" ,
                        "xtZhxgrxm" ,"xtZhxgrid" ,"xtZhxgip" ,"xtZhxgrbm" ,"xtZhxgrbmid" ,"xtZxbz" ,"xtZxyy","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");

        return datagtFour;
    }


    //读取值机信息
    public static Dataset<Row> ZJXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate  ,dept as qfjcdm,dest as ddjcdm,name ,cnName ,cType ,cNumber  ,gender ,seat ,brdno ,concat(substr(ckitm,1,2),':',substr(ckitm,3,2)) as ckitm ,substr(cNumber,1,6) as xzqhdm,dept ,dest ,leval ,depttm ,inf  ,dttm ,ckipid ,ckiofc ,ckiagt ,sndr ,seqn ,type ,styp ,loc  ,prsta ,gates ,ctct ,etFl ,etNo ,isGroup  ,bsgs ,bagWgt ,sip  ,wetm ,mesState ,appId ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm  ,xtLrrid  ,xtLrip ,xtLrrbm  ,xtLrrbmid ,substr(xtZhxgsj,1,19) as xtZhxgsj ,xtZhxgrxm ,xtZhxgrid ,xtZhxgip ,xtZhxgrbm ,xtZhxgrbmid ,xtZxbz ,xtZxyy from "+TB_ST_ZJXX+"  ");
        return data;
    }
    //读取订票信息
    public static Dataset<Row> DPXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,xxId ,flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate  ,ddsj   ,qfsj   ,qfjcdm ,ddjcdm ,name   ,substr(zjhm,1,6) as xzqhdm,cnName ,lkzt   ,zjlx   ,zjhm   ,rksj   ,mesState ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm  ,xtLrrid  ,xtLrrbm  ,xtLrrbmid ,xtLrip ,substr(xtZhxgsj,1,19) as xtZhxgsj ,xtZhxgrxm ,xtZhxgrid ,xtZhxgrbm ,xtZhxgrbmid ,xtZhxgip ,xtZxbz ,xtZxyy  from "+TB_ST_DPXX+"  ");

        return data;
    }






}

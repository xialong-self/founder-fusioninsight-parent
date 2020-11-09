package com.founder.fusioninsight.spark.dataimport.incremental;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_data;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.TimeZone;

/**
 * @author 夏龙
 * @date 2020-09-08
 */
public class jwzh_jcgl_UpdateField {
    /**
     * ajxx,djxx,jgxx三表更新起飞到达机场等字段
     * 默认为一次更新当天所有没更新的数据
     * 可修改为更新全部数据
     */
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_UpdateField().run();




    }


    public void run() {
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl_UpdateField");
        //设置开始时间
        String time = DateTimeUtils.getSystemDateTimeString();
        String beforetime= DateTimeUtils.DateToString(DateTimeUtils.addSecond(DateTimeUtils.StringToDate(time, DateTimeUtils.YYYY_MM_DD_HH_MM_SS), -60), DateTimeUtils.YYYY_MM_DD);


        //dpxx更新，根据起飞机场代码更新字段
        Dataset<Row> gtxxdata_y= jwzh_jcgl_data.GTXX(sparkSession).cache();
        Dataset<Row> zdxxdata_y=jwzh_jcgl_data.ZDB(sparkSession).where("xtZxbz='0' ").cache();
        Dataset<Row> dpxx_yx= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_dpxx/_doc")
                .filter("qfjcdm_mc is null and xtLrsj>='"+beforetime+"'")
                .selectExpr("id","qfjcdm","ddjcdm");
        Dataset<Row> dpxx_all=DataChange(dpxx_yx,zdxxdata_y,gtxxdata_y);
        new SaveToEsUtils().saveTjjgToEs_Append(dpxx_all,"jwzh_jcgl.tb_st_dpxx");


        Dataset<Row>  esdata=EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_dpxx/_doc")
                .filter("FlightDate = '"+beforetime+"' and qfjcdm_mc is not null")
                .selectExpr("addid_dp as addid","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm").cache();
        //jgxx更新，根据起飞机场代码更新字段
        Dataset<Row> jgxx_yx= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_jgxx/_doc")
                .filter("qfjcdm_mc is null and FlightDate = '"+beforetime+"'")
                .selectExpr("id","qfjcdm","ddjcdm");
        Dataset<Row> jgxx_all=DataChange(jgxx_yx,zdxxdata_y,gtxxdata_y);
        new SaveToEsUtils().saveTjjgToEs_Append(jgxx_all,"jwzh_jcgl.tb_st_jgxx");


        //zjxx更新，根据起飞机场代码更新字段
        Dataset<Row> zjxx_yx= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_zjxx/_doc")
                .filter("qfjcdm_mc is null and FlightDate ='"+beforetime+"'")
                .selectExpr("id","qfjcdm","ddjcdm");
        Dataset<Row> zjxx_all=DataChange(zjxx_yx,zdxxdata_y,gtxxdata_y);
        new SaveToEsUtils().saveTjjgToEs_Append(zjxx_all,"jwzh_jcgl.tb_st_zjxx");




        //djxx更新,根据all表更新字段，不需要起飞机场代码
        Dataset<Row> djxx_y= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_djxx/_doc")
                .filter("qfjcdm_mc is null and FlightDate ='"+beforetime+"'")
                .selectExpr("id","addid_dj");
        Dataset<Row> djxxChange=joinAjxx(djxx_y,esdata,"addid_dj");
        new SaveToEsUtils().saveTjjgToEs_Append(djxxChange,"jwzh_jcgl.tb_st_djxx");

        //ajxx更新,根据all表更新字段，不需要起飞机场代码
        Dataset<Row> ajxx_y= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.tb_st_ajxx/_doc")
                .filter("qfjcdm_mc is null and FlightDate ='"+beforetime+"'")
                .selectExpr("id","addid_aj");
        Dataset<Row> ajxxChange=joinAjxx(ajxx_y,esdata,"addid_aj");
        new SaveToEsUtils().saveTjjgToEs_Append(ajxxChange,"jwzh_jcgl.tb_st_ajxx");


        sparkSession.stop();
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



    public static Dataset<Row> joinAjxx(Dataset<Row> data1,Dataset<Row> data2,String JoinId) {
        Dataset<Row> data3=data2.dropDuplicates("addid","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm")
                .selectExpr("addid","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");
        Dataset<Row> djxxJoinall=data1.join(data3,data1.col(JoinId).equalTo(data3.col("addid")),"left")
                .selectExpr("id","qfjcdm_mc", "ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");
        return djxxJoinall;
    }



}

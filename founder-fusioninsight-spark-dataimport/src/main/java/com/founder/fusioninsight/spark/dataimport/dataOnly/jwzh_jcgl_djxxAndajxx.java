package com.founder.fusioninsight.spark.dataimport.dataOnly;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * @author 夏龙
 * @date 2020-08-31
 */
public class jwzh_jcgl_djxxAndajxx {
    /**
     * 登记信息表全量数据
     */
    static String TB_ST_AJXX="jwzh_jcgl.tb_st_ajxx";
    static String TB_ST_DJXX="jwzh_jcgl.tb_st_djxx";
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_djxxAndajxx().run();

    }

    public void run() {
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl_djxxAll");
//        Dataset<Row> djxxdata=DJXX(sparkSession).repartition(100);
        Dataset<Row> ajxxdata=AJXX(sparkSession).repartition(100);
        Dataset<Row>  esdata=sparkSession.read().format("org.elasticsearch.spark.sql").load("jwzh_jcgl.all/_doc")
                .filter("dpxx_id is not null")
                .select("addid","dpxx_qfjcdm","qfjcdm_mc","dpxx_ddjcdm","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm").repartition(100);
//        Dataset<Row> djxxChange=joinDjxx(djxxdata,esdata.filter("djxx_id is not null"));
//        new SaveToEsUtils().saveTjjgToEs_zj(djxxChange,"jwzh_jcgl.tb_st_djxx");
        Dataset<Row> ajxxChange=joinAjxx(ajxxdata,esdata.filter("ajxx_id is not null"));
        new SaveToEsUtils().saveTjjgToEs_zj(ajxxChange,"jwzh_jcgl.tb_st_ajxx");
        sparkSession.stop();

    }

    public static Dataset<Row> joinDjxx(Dataset<Row> data1,Dataset<Row> data2) {
        Dataset<Row> data3=data2.dropDuplicates("addid","dpxx_qfjcdm","qfjcdm_mc","dpxx_ddjcdm","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm")
                .selectExpr("addid","dpxx_qfjcdm","qfjcdm_mc","dpxx_ddjcdm","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");
        Dataset<Row> djxxJoinall=data1.join(data3,data1.col("addid_dj").equalTo(data3.col("addid")),"left")
                .selectExpr("id","addid_dj","flightNo" ,"flightDate" ,"gate" ,"brdno" ,"name" ,"cnName","brdst" ,"seat" ,"optm" ,"sndr" ,
                        "seqn" ,"dttm" ,"type" ,"mesState" ,"wetm" ,"cType" ,"cNumber" ,"appId" ,"xtLrrxm" ,"xtLrrid" ,"xtLrrbm","xtLrrbmid",
                        "xtLrip","xtZhxgsj","xtZhxgrxm" ,"xtZhxgrid" ,"xtZhxgrbm" ,"xtZhxgrbmid" ,"xtZxbz" ,"xtZxyy","dpxx_qfjcdm","qfjcdm_mc",
                        "dpxx_ddjcdm","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm","xtLrsj");
        return djxxJoinall;
    }

    public static Dataset<Row> joinAjxx(Dataset<Row> data1,Dataset<Row> data2) {
        Dataset<Row> data3=data2.dropDuplicates("addid","dpxx_qfjcdm","qfjcdm_mc","dpxx_ddjcdm","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm")
                .selectExpr("addid","qfjcdm_mc","ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");
        Dataset<Row> djxxJoinall=data1.join(data3,data1.col("addid_aj").equalTo(data3.col("addid")),"left")
                .selectExpr("id","sndr" ,"seqn" ,"dttm" ,"type" ,"styp" ,"flightNo" ,"flightDate" ,"brdno" ,"dept" ,"isInOut" ,"name" ,"idcard" ,"channelNo"  ,
                        "sctm" ,"mesState" ,"xtLrsj"  ,"xtLrrxm" ,"xtLrrid" ,"xtLrip"  ,"xtLrrbm" ,"xtLrrbmid"  ,"xtZhxgsj"   ,"xtZhxgrxm"  , "xtZhxgrid"  ,"xtZhxgip" ,
                        "xtZhxgrbm"  ,"xtZhxgrbmid"   ,"xtZxbz"  ,"xtZxyy","qfjcdm_mc",
                        "ddjcdm_mc","qfjcsfxzqh","qfjcsfm","ddjcsfxzqh","ddjcsfm");
        return djxxJoinall;
    }

    //读取登机信息
    public static Dataset<Row> DJXX(SparkSession sparkSession) {
        Dataset<Row> djxx=sparkSession
                .sql("select rowkey as id,upper(concat(cNumber,flightno,flightdate))as addid_dj,flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ,gate ,brdno ,name ,cnName ,brdst ,seat ,from_unixtime(unix_timestamp(optm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as optm ,sndr ,seqn ,dttm ,type ,mesState ,wetm ,cType ,cNumber ,appId ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm ,xtLrrid ,xtLrrbm ,xtLrrbmid ,xtLrip ,substr(xtZhxgsj,1,19) as xtZhxgsj ,xtZhxgrxm ,xtZhxgrid ,xtZhxgrbm ,xtZhxgrbmid ,xtZxbz ,xtZxyy  from "+TB_ST_DJXX+" ");

        return djxx;
    }
    //读取安检信息
    public static Dataset<Row> AJXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,upper(concat(idcard,flightno,flightdate))as addid_aj,sndr ,seqn ,dttm ,type ,styp ,flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ,brdno ,dept ,isInOut ,name ,idcard ,channelNo  ,from_unixtime(unix_timestamp(sctm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as sctm ,mesState ,substr(xtLrsj,1,19) as xtLrsj  ,xtLrrxm ,xtLrrid ,xtLrip  ,xtLrrbm ,xtLrrbmid  ,substr(xtZhxgsj,1,19) as xtZhxgsj   ,xtZhxgrxm  , xtZhxgrid  ,xtZhxgip   ,xtZhxgrbm  ,xtZhxgrbmid   ,xtZxbz  ,xtZxyy  from "+TB_ST_AJXX+"   ");

        return data;
    }




}

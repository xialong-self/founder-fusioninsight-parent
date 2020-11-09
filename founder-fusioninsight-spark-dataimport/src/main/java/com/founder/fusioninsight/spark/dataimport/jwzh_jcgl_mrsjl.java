package com.founder.fusioninsight.spark.dataimport;
import static org.apache.spark.sql.functions.lit;
import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;

/**
 * @author 夏龙
 * @date 2020-09-23
 * 每日数据量
 */
public class jwzh_jcgl_mrsjl {
    /**
     * 安检信息
     * 值机信息
     * 订票信息
     * 登机信息
     * 比对明细
     * 数据来源表
     * 进港来源表
     */
    static String TB_ST_AJXX="jwzh_jcgl.tb_st_ajxx";
    static String TB_ST_ZJXX="jwzh_jcgl.tb_st_zjxx";
    static String TB_ST_DPXX="jwzh_jcgl.tb_st_dpxx";
    static String TB_ST_DJXX="jwzh_jcgl.tb_st_djxx";
    static String TB_ST_BDMX="jwzh_jcgl.tb_st_bdmx";
    static String TB_ST_SJLY="jwzh_jcgl.tb_st_sjly";
    static String TB_ST_JGXX="jwzh_jcgl.tb_st_jgxx";
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_mrsjl().run();



    }

    public void run()  {
        //集群sparksession
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl.mrsjl");
        //设置开始时间
        String time = DateTimeUtils.getSystemDateTimeString();
        String tjzq= DateTimeUtils.DateToString(DateTimeUtils.addDays(DateTimeUtils.StringToDate(time, DateTimeUtils.YYYY_MM_DD_HH_MM_SS), -1), DateTimeUtils.YYYY_MM_DD);
        String tjsj = DateTimeUtils.getSystemDateTimeString();

        Dataset<Row> djxx=DJXX(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("djxx"))
                .selectExpr("datanum","bs","tjzq","tjsj");
        Dataset<Row> zjxx=ZJXX(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("zjxx"))
                .selectExpr("datanum","bs","tjzq","tjsj");
        Dataset<Row> dpxx=DPXX(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("dpxx"))
                .selectExpr("datanum","bs","tjzq","tjsj");
        Dataset<Row> ajxx=AJXX(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("ajxx"))
                .selectExpr("datanum","bs","tjzq","tjsj");
        Dataset<Row> bdmx=BDMX(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("bdmx"))
                .selectExpr("datanum","bs","tjzq","tjsj");
        Dataset<Row> sjly=SJLY(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("sjly"))
                .selectExpr("datanum","bs","tjzq","tjsj");
        Dataset<Row> jgxx=JGXX(sparkSession,tjzq)
                .withColumn("tjzq",lit(tjzq))
                .withColumn("tjsj",lit(tjsj))
                .withColumn("bs",lit("jgxx"))
                .selectExpr("datanum","bs","tjzq","tjsj");

        Dataset<Row> all=dpxx.unionAll(zjxx).unionAll(djxx).unionAll(ajxx).unionAll(bdmx).unionAll(sjly).unionAll(jgxx);

        EsSparkSQL.saveToEs(all,"jwzh_jcgl.mrsjl/_doc");
        sparkSession.stop();

    }


    //读取登机信息
    public static Dataset<Row> DJXX(SparkSession sparkSession,String date) {
        Dataset<Row> djxx=sparkSession
                .sql("select count(1) as datanum from "+TB_ST_DJXX+" where xtLrsj like '%"+date+"%' ");
        return djxx;
    }
    //读取值机信息
    public static Dataset<Row> ZJXX(SparkSession sparkSession,String date) {
        Dataset<Row> data=sparkSession
                .sql("select count(1) as datanum  from "+TB_ST_ZJXX+" where xtLrsj like '%"+date+"%' ");
        return data;
    }
    //读取订票信息
    public static Dataset<Row> DPXX(SparkSession sparkSession,String date) {
        Dataset<Row> data=sparkSession
                .sql("select count(1) as datanum  from "+TB_ST_DPXX+" where xtLrsj like '%"+date+"%' ");
        return data;
    }
    //读取安检信息
    public static Dataset<Row> AJXX(SparkSession sparkSession,String date) {
        Dataset<Row> data=sparkSession
                .sql("select count(1) as datanum  from "+TB_ST_AJXX+"  where xtLrsj like '%"+date+"%' ");
        return data;
    }

    //读取布控人员信息
    public static Dataset<Row> BDMX(SparkSession sparkSession,String date) {
        Dataset<Row> data=sparkSession
                .sql("select count(1) as datanum  from "+TB_ST_BDMX+" where xtLrsj like '%"+date+"%' ");
        return data;
    }


    //数据来源表
    public static Dataset<Row> SJLY(SparkSession sparkSession,String date) {
        Dataset<Row> data=sparkSession
                .sql("select count(1) as datanum  from "+TB_ST_SJLY+" where xtLrsj like '%"+date+"%' ");
        return data;
    }

    //JGXX表
    public static Dataset<Row> JGXX(SparkSession sparkSession,String date) {
        Dataset<Row> data=sparkSession
                .sql("select count(1) as datanum  from "+TB_ST_JGXX+" where xtLrsj like '%"+date+"%' ");
        return data;
    }
}

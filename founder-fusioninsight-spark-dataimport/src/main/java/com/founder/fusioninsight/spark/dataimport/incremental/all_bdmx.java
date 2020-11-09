package com.founder.fusioninsight.spark.dataimport.incremental;

import com.founder.fusioninsight.config.FusioninsightConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.HashMap;

/**
 * @author 夏龙
 * @date 2020-08-26
 */
public class all_bdmx {
    /**
     * 比对明细表
     */
    static String TB_ST_BDMX="jwzh_jcgl.tb_st_bdmx";
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new all_bdmx().run();
    }

    public void run(){
        //集群sparksession
        FusioninsightConfig.getInstance().login();
        SparkSession sparkSession = SparkSession.builder()
                .appName("jwzh_jcgl_bdmxEs")
                .config("spark.driver.host",FusioninsightConfig.getDriverHost())
                .config(ConfigurationOptions.ES_NET_USE_SSL,"true")
                .config(ConfigurationOptions.ES_NODES,FusioninsightConfig.getInstance().getEsConfig().getEsServerHost())
                .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, FusioninsightConfig.getInstance().getUserConfig().getUsername())
                .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, FusioninsightConfig.getInstance().getUserConfig().getPassword())
                .config("spark.sql.crossJoin.enabled",true)
                .config("spark.yarn.am.waitTime", 1000)
                .config("spark.sql.broadcastTimeout", 1000)
                .config(ConfigurationOptions.ES_INDEX_AUTO_CREATE,true)
                .config(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY,"yes")
                .getOrCreate();
        Dataset<Row> esdata= EsSparkSQL.esDF(sparkSession, "jwzh_jcgl.all/_doc").repartition(100).cache();
        Dataset<Row> bdmx=BDMX(sparkSession);
        Dataset<Row> zdry_all=bdmx.groupBy("addid_bdmx","sjly").count().selectExpr("addid_bdmx","sjly").cache();
        //关联dbmx和es数据
        Dataset<Row> BdmxJoinEsdata=zdry_all.join(esdata,zdry_all.col("addid_bdmx").equalTo(esdata.col("addid")),"left").cache();

        BdmxJoinEsdata.createOrReplaceTempView("es_table");
        Dataset<Row> dataNew=sparkSession.sql("select id,addid,cnname,zjhm,flightno,flightdate,xzqhdm,zjlxdm_mc,zjlxdm,xzqh,xb,tjsj,bs,"
                +"zjxx_id,djxx_id,dpxx_id,ajxx_id,dpxx_qfsj,xtlrsj,dpsj,ajsj,djsj,zjxx_gender,seat,brdno,zjxx_loc,ajxx_channelno,djxx_gate,"
                +"djxx_brdst,dpxx_ddsj,channelno_mc,loc_mc,dpxx_qfjcdm,qfjcdm_mc,dpxx_ddjcdm,ddjcdm_mc,zjsj,qfjcsfxzqh,qfjcsfm,ddjcsfxzqh,ddjcsfm, "
                +"case  when rylx_zd is null and sjly='重点' then sjly  else rylx_zd end rylx_zd,"
                +"case  when rylx_zt is null and sjly='在逃' then sjly  else rylx_zt end rylx_zt,"
                +"case  when rylx_bk is null and sjly='布控' then sjly  else rylx_bk end rylx_bk "
                +"  from es_table ");
        saveTjjgToEs(dataNew,"jwzh_jcgl.all");

    }

    public void saveTjjgToEs(Dataset<Row> tjjg,String indexname) {
        //指定ID字段，便于覆盖原有数据
        java.util.Map<String, String> map = new HashMap<String, String>();
        map.put("es.mapping.id","id");
        scala.collection.mutable.Map<String, String> salaMap = scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
        EsSparkSQL.saveToEs(tjjg,indexname+"/_doc",salaMap);
    }

    //读取时间戳表
    public static Dataset<Row> BDMX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select upper(concat(zjhm,translate(flate,'-','')))as addid_bdmx,zjhm,sjly,substr(xtlrsj,1,19) as xtlrsj,xtzxbz from "+TB_ST_BDMX+" ");

        return data;
    }
}

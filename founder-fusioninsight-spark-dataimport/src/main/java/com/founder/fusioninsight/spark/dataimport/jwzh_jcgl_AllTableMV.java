package com.founder.fusioninsight.spark.dataimport;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.dataimport.dataOnly.jwzh_jcgl_residue;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author 夏龙
 * @date 2020-09-08
 */
public class jwzh_jcgl_AllTableMV {
    /**
     * 安检信息、值机信息、订票信息登机信息、进港来源表 全量数据迁移
     */
    static String TB_ST_AJXX="jwzh_jcgl.tb_st_ajxx";
    static String TB_ST_ZJXX="jwzh_jcgl.tb_st_zjxx";
    static String TB_ST_DPXX="jwzh_jcgl.tb_st_dpxx";
    static String TB_ST_DJXX="jwzh_jcgl.tb_st_djxx";
    static String TB_ST_JGXX="jwzh_jcgl.tb_st_jgxx";

    public static void main(String[] args) {

        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_AllTableMV().run();


    }

    /**
     * 根据情况执行更新
     */
    public void run() {
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl_AllTableMV");
        //删除repartition预分区测试效率
        Dataset<Row> jgxxData=JGXX(sparkSession).repartition(64);
        Dataset<Row> dpxxData=DPXX(sparkSession).repartition(64);
        Dataset<Row> ajxxData=AJXX(sparkSession).repartition(16);
        Dataset<Row> zjxxData=ZJXX(sparkSession).repartition(32);
        Dataset<Row> djxxData=DJXX(sparkSession).repartition(16);


        new SaveToEsUtils().saveTjjgToEs_zj(jgxxData,"jwzh_jcgl.tb_st_jgxx");

        new SaveToEsUtils().saveTjjgToEs_zj(dpxxData,"jwzh_jcgl.tb_st_dpxx");

        new SaveToEsUtils().saveTjjgToEs_zj(ajxxData,"jwzh_jcgl.tb_st_ajxx");

        new SaveToEsUtils().saveTjjgToEs_zj(zjxxData,"jwzh_jcgl.tb_st_zjxx");

        new SaveToEsUtils().saveTjjgToEs_zj(djxxData,"jwzh_jcgl.tb_st_djxx");


        sparkSession.stop();
    }




    //读取登机信息
    public static Dataset<Row> DJXX(SparkSession sparkSession) {
        Dataset<Row> djxx=sparkSession
                .sql("select rowkey as id,upper(concat(cNumber,flightno,flightdate))as addid_dj,flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ," +
                        "gate ,brdno ,name ,cnName ,brdst ,seat ,from_unixtime(unix_timestamp(optm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as optm ," +
                        "sndr ,seqn ,dttm ,type ,mesState ,wetm ,cType ,cNumber ,appId ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm ,xtLrrid ,xtLrrbm ," +
                        "xtLrrbmid ,xtLrip ,substr(xtZhxgsj,1,19) as xtZhxgsj ,xtZhxgrxm ,xtZhxgrid ,xtZhxgrbm ,xtZhxgrbmid ,xtZxbz ,xtZxyy  from "+TB_ST_DJXX+" ");

        return djxx;
    }
    //读取值机信息
    public static Dataset<Row> ZJXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,flightNo ,upper(concat(cNumber,flightno,flightdate))as addid_zj," +
                        "from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ,dept as qfjcdm," +
                        "dest as ddjcdm,name,cnName,cType,cNumber,gender,seat,brdno,concat(substr(ckitm,1,2),':',substr(ckitm,3,2)) as ckitm," +
                        "substr(cNumber,1,6) as xzqhdm,dept,dest,leval,depttm,inf,dttm,ckipid,ckiofc,ckiagt,sndr,seqn,type,styp ,loc,prsta ," +
                        "gates,ctct,etFl,etNo,isGroup,bsgs,bagWgt,sip,wetm,mesState,appId,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrip," +
                        "xtLrrbm  ,xtLrrbmid ,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid,xtZhxgip,xtZhxgrbm,xtZhxgrbmid,xtZxbz ,xtZxyy from "+TB_ST_ZJXX+"  ");

        return data;
    }
    //读取订票信息
    public static Dataset<Row> DPXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,xxId,flightNo,upper(concat(zjhm,flightno,flightdate))as addid_dp," +
                        "from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate,ddsj," +
                        "qfsj   ,qfjcdm ,ddjcdm ,name   ,substr(zjhm,1,6) as xzqhdm,cnName,lkzt,zjlx,zjhm,rksj,mesState," +
                        "substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrrbm,xtLrrbmid,xtLrip,substr(xtZhxgsj,1,19) as xtZhxgsj," +
                        "xtZhxgrxm,xtZhxgrid ,xtZhxgrbm,xtZhxgrbmid,xtZhxgip,xtZxbz,xtZxyy  from "+TB_ST_DPXX+"  ");
        return data;
    }
    //读取安检信息
    public static Dataset<Row> AJXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,upper(concat(idcard,flightno,flightdate))as addid_aj,sndr ,seqn ,dttm ,type ,styp ," +
                        "flightNo ,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate ,brdno ,dept ,isInOut ," +
                        "name ,idcard ,channelNo  ,from_unixtime(unix_timestamp(sctm,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as sctm ," +
                        "mesState ,substr(xtLrsj,1,19) as xtLrsj ,xtLrrxm,xtLrrid,xtLrip,xtLrrbm ,xtLrrbmid,substr(xtZhxgsj,1,19) as xtZhxgsj," +
                        "xtZhxgrxm , xtZhxgrid ,xtZhxgip,xtZhxgrbm ,xtZhxgrbmid ,xtZxbz  ,xtZxyy  from "+TB_ST_AJXX+"   ");
        return data;
    }


    //JGXX表
    public static Dataset<Row> JGXX(SparkSession sparkSession) {
        Dataset<Row> data=sparkSession
                .sql("select rowkey as id,upper(concat(zjhm,flightno,flightdate))as addid_jgxx,snId,flightNo,from_unixtime(unix_timestamp(flightDate,'ddMMMyy'),'yyyy-MM-dd') as flightDate," +
                        "qfjcdm,ddjcdm,qfsj,ddsj,name,cnName,lkzt,csrq,xb,zjhm,substr(zjhm,1,6) as xzqhdm,fjgj,rksj," +
                        "mesState,substr(xtLrsj,1,19) as xtLrsj,xtLrrxm,xtLrrid,xtLrrbm,xtLrrbmid,xtLrip,substr(xtZhxgsj,1,19) as xtZhxgsj,xtZhxgrxm,xtZhxgrid," +
                        "xtZhxgrbm,xtZhxgrbmid,xtZhxgip,xtZxbz,xtZxyy from "+TB_ST_JGXX+" ");

        return data;
    }



}

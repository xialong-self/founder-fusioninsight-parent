package com.founder.fusioninsight.spark.dataimport;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.spark.utils.ConnectionUtils;
import com.founder.fusioninsight.spark.utils.DateTimeUtils;
import com.founder.fusioninsight.spark.utils.SaveToEsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import static org.apache.spark.sql.functions.lit;

import static com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_data.*;

/**
 * @author 夏龙
 * @date 2020-10-09
 */
public class jwzh_jcgl_msg_log_bdmx {
    public static void main(String[] args) {
        FusioninsightConfig.initArgs(args);
        new jwzh_jcgl_msg_log_bdmx().run();

    }

    public void run() {
        SparkSession sparkSession= ConnectionUtils.sparkSessionConnection("jwzh_jcgl_allDate");
        String tjsj = DateTimeUtils.getSystemDateTimeString();

        String endTime = DateTimeUtils.getSystemDateTimeString();
        //获取时间戳
        Dataset<Row> sjcdata_y= EsSparkSQL.esDF(sparkSession,"jwzh_jcgl.sjc/_doc")
                .filter("id='alladata_timestamp'")
                .selectExpr("max(endtime)");
//		Dataset<Row> sjcdata_y=TIMESTAMP(sparkSession);
        String timestamp=sjcdata_y.first().get(0).toString();
        //取上次结束时间60s之前
        String beforetime=DateTimeUtils.DateToString(DateTimeUtils.addHour24(DateTimeUtils.StringToDate(timestamp, DateTimeUtils.YYYY_MM_DD_HH_MM_SS), -1), DateTimeUtils.YYYY_MM_DD_HH_MM_SS);


        Dataset<Row> bdmx_data=BDMX(sparkSession);
        Dataset<Row> msg_storagedata=MSG_STORAGE(sparkSession);
        Dataset<Row> msg_storage_log_data=MSG_STORAGE_LOG(sparkSession);

        Dataset<Row> bdmx_msg_storage=bdmx_data
                .join(msg_storagedata,bdmx_data.col("tb_st_bdmx_id").equalTo(msg_storagedata.col("msg_storage_bussId")),"left");
        Dataset<Row> bdmx_msg_storage_msg_storage_log=bdmx_msg_storage
                .join(msg_storage_log_data,bdmx_msg_storage.col("msg_storage_id").equalTo(msg_storage_log_data.col("msg_storage_log_msgId")),"left");

        bdmx_msg_storage_msg_storage_log.createOrReplaceTempView("table");
        String sql="select t.*, " +
                "case when msg_storage_log_msgId is not null then msg_storage_log_id " +
                " when msg_storage_log_msgId is null and msg_storage_bussId is not null then msg_storage_id " +
                "else tb_st_bdmx_id " +
                "end id " +
                "from table  t";
        Dataset<Row> data=sparkSession.sql(sql);

        new SaveToEsUtils().saveTjjgToEs_zj(data,"jwzh_jcgl.storage_log_bdmx");
//        EsSparkSQL.saveToEs(bdmx_msg_storage_msg_storage_log,"jwzh_jcgl.storage_log_bdmx/_doc");
    }

}

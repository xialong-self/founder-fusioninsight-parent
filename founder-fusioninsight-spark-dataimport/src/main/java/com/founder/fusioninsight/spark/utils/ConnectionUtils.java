package com.founder.fusioninsight.spark.utils;

import com.founder.fusioninsight.config.FusioninsightConfig;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import java.util.Calendar;
import java.util.Date;

/**
 * @author 夏龙
 * @date 2020-09-04
 */
public class ConnectionUtils {
    /**
     * 创建sparksession连接
     * @return
     */
    public static SparkSession sparkSessionConnection(String appName){
        FusioninsightConfig.getInstance().login();
        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config("spark.driver.host", FusioninsightConfig.getDriverHost())
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
        return sparkSession;
    }





}

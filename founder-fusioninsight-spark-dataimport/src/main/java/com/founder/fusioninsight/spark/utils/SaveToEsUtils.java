package com.founder.fusioninsight.spark.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.HashMap;

/**
 * @author 夏龙
 * @date 2020-09-04
 */
public class SaveToEsUtils {

    /**
     * 直接覆盖存入ES
     */
    public void saveTjjgToEs_zj(Dataset<Row> tjjg, String indexname) {
        //指定ID字段，便于覆盖原有数据
        java.util.Map<String, String> map = new HashMap<String, String>();
        map.put("es.mapping.id","id");
        scala.collection.mutable.Map<String, String> salaMap = scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
        EsSparkSQL.saveToEs(tjjg,indexname+"/_doc",salaMap);
    }

    /**
     * 指定覆盖类型Append
     */
    public void saveTjjgToEs_Append(Dataset<Row> tjjg, String indexname) {
        //指定ID字段，便于覆盖原有数据
        java.util.Map<String, String> map = new HashMap<String, String>();
        map.put("es.mapping.id","id");
        map.put("es.write.operation","upsert");
        scala.collection.mutable.Map<String, String> salaMap = scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
        EsSparkSQL.saveToEs(tjjg,indexname+"/_doc",salaMap);
    }




}

spark-submit \
--class com.founder.fusioninsight.spark.dataimport.jwzh_jcgl_bdmx \
--master yarn \
--deploy-mode client \
--jars ./lib/*  \
founder-fusioninsight-spark-dataimport-1.0.0.jar \
--userProperties=/opt/server/founder-fusioninsight-web/config/user.properties \
--esProperties=/opt/server/founder-fusioninsight-web/config/es.properties \
--driverHost=80.2.33.96
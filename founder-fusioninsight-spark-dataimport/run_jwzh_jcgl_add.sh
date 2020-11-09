spark-submit \
--class com.founder.fusioninsight.spark.dataimport.incremental.jwzh_jcgl_add \
--master yarn \
--deploy-mode client \
--jars ./lib/*  \
founder-fusioninsight-spark-dataimport-1.0.0.jar \
--userProperties=/opt/server/founder-fusioninsight-web/config/user.properties \
--esProperties=/opt/server/founder-fusioninsight-web/config/es.properties \
--driverHost=80.2.33.96
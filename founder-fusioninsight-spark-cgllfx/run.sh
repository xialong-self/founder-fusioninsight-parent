spark-submit \
--class com.founder.fusioninsight.spark.cgllfx.Application \
--master yarn \
--deploy-mode client \
--jars ./lib/*  \
founder-fusioninsight-spark-cgllfx-1.0.0.jar \
--userProperties=/opt/server/founder-fusioninsight-web/config/user.properties \
--esProperties=/opt/server/founder-fusioninsight-web/config/es.properties \
--driverHost=80.2.33.96
#--runDate=202002 \


# 简介 
	项目名称：《方正-华为大数据平台-spark程序-数据导入》
	1.机场数据从mysql到hbase，然后到ES的处理程序
	参数：
		--userProperties=Fusioninsight用户信息配置文件
		--esProperties=Fusioninsight的ES配置文件		
		--driverHost=当前运行机器的对外IP
	2.注意打包的时候使用的assembly插件，	把founder-fusioninsight-config.jar合并到了新的jar里，其他的jar都没有包含。spark程序在集群上运行的时候，环境中有依赖的基础包，业务相关的自定义包必须打包到一个jar才能执行。
	3.基础环境可能没有elasticseatch-spark-*的jar包，如果缺少，请复制到lib目录下，参考run.sh
				

**作者**：[zhang.hai@founder.com.cn]

## Changelog
	
### 1.0.0.20200717
	1.基础版本
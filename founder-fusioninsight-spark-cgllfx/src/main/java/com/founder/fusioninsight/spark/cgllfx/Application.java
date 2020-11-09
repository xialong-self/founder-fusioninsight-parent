package com.founder.fusioninsight.spark.cgllfx;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.sql.EsSparkSQL;

import com.founder.fusioninsight.config.FusioninsightConfig;

import scala.collection.JavaConverters;

/**
 * ****************************************************************************
 * @Package:      [com.founder.fusioninsight.spark.cgllfx.Application.java]  
 * @ClassName:    [Application]   
 * @Description:  [出港流量分析]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月17日 上午11:50:28]   
 * @UpdateUser:   [zhanghai(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月17日 上午11:50:28，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class Application {

	public static void main(String[] args) throws UnknownHostException {
		//初始化配置参数，返回spark的driverHost
		String driverHost=FusioninsightConfig.initArgs(args);
		
		//--runDate=统计日期，格式"yyyyMM",例如"202007"	，如果不传就默认取昨天
		String runDateStr = null;
		if(args!=null) {
    		for(String arg:args) {
    			if(arg.trim().startsWith("--runDate=")) {
    				runDateStr = arg.trim().substring(10);
    				System.out.println("统计配置的日期："+runDateStr);
    			}
    		}
    	}
		Date runDate;
		if(runDateStr==null || runDateStr.trim().length() == 0) {
			runDate = addDays(new Date(),-1);
		}else {
			runDate = StringToDate(runDateStr,"yyyyMM");
		}
		System.out.println("统计日期："+runDate);
		
		new Application().run(runDate,driverHost);

	}

	public void run(Date runDate,String driverHost) {
		String flightDate = DateToString(runDate,"MMMyy");//航班日期月份
		String tjzq = DateToString(runDate,"yyyyMM");//统计月份
		
		FusioninsightConfig.getInstance().login();
		SparkSession session = SparkSession.builder()
	              .appName("FounderFusioninsightSparkCgllfx")	        
	              .config("spark.driver.host",driverHost)
	              .config(ConfigurationOptions.ES_NET_USE_SSL,"true")
	              .config(ConfigurationOptions.ES_NODES,FusioninsightConfig.getInstance().getEsConfig().getEsServerHost())
	              .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, FusioninsightConfig.getInstance().getUserConfig().getUsername())
	              .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, FusioninsightConfig.getInstance().getUserConfig().getPassword())
	             // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	              //.config("spark.kryo.registrator", "com.huawei.hadoop.security.HuaweiRegistrator")
	              .getOrCreate();
		
		session.log().info("SparkSession create");
		
		Dataset<Row> allData = session.sql("select '"+tjzq+"' as tjzq,count(1) as num from jwzh_jcgl.tb_st_djxx where flightdate like '%"+flightDate+"'");
		
		Map<String,String> mapping = new HashMap<String,String>();
		mapping.put("es.mapping.id", "tjzq");
		EsSparkSQL.saveToEs(allData,"jwzh_jcgl.cgllfx/_doc",JavaConverters.mapAsScalaMapConverter(mapping).asScala());
		
		session.log().info("SparkSession stop");
		session.stop();
	}
	
	public static Date addDays(Date date,int amount){
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(date);
		gc.add(Calendar.DAY_OF_YEAR, amount);
		return gc.getTime();
	}
	
	/**
	 * 
	 * @Title: StringToDate
	 * @Description: TODO(将日期格式的字符串转成日期)
	 * @param @param dateStr 日期格式的字符串
	 * @param @param dateModel 日期格式
	 * @param @return
	 * @param @throws ParseException    设定文件
	 * @return Date    返回类型
	 * @throw
	 */
	public static Date StringToDate(String dateStr,String dateModel){
		SimpleDateFormat sdf = new SimpleDateFormat(dateModel,Locale.UK);
		try {
			return sdf.parse(dateStr);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 
	 * @Title: DateToString
	 * @Description: TODO(将日期转成字符串)
	 * @param @param date 日期
	 * @param @param dateModel 日期格式
	 * @param @return
	 * @param @throws ParseException    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String DateToString(Date date,String dateModel){
		SimpleDateFormat sdf = new SimpleDateFormat(dateModel,Locale.UK);
		return sdf.format(date);
	}
}

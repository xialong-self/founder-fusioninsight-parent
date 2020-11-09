package com.founder.scheduler.runnable.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.founder.db.DbTools;
import com.founder.framework.utils.DateTimeUtils;
import com.founder.scheduler.runnable.SchedulerRunnable;

/**
 * ****************************************************************************
 * @Package:      [com.founder.scheduler.runnable.impl.MysqlRunnable.java]  
 * @ClassName:    [MysqlRunnable]   
 * @Description:  [定时执行mysql语句]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2019年12月20日 上午9:27:29]   
 * @UpdateUser:   [Dagger(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2019年12月20日 上午9:27:29，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class DbSqlRunnable extends SchedulerRunnable{
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public String execute() {
		/*
		 * 	支持的sql参数类型：
		 *	日期：
		 *		字符串格式的日期，#{Date(日期格式)+或者-天数}，例如#{Date(yyyy-MM-dd)}、#{Date(yyyy-MM-dd)-1}、#{Date(yyyy-MM-dd)+1}等
		 *		数字型日期：#{DateLong()+或者-天数}，例如#{DateLong()}、#{DateLong()+1}、#{DateLong()-1}
		 * 	其它的待开发
		 */
		
		String configData = super.getEntity().getConfigdata();
		if(StringUtils.isEmpty(configData)) {
			throw new RuntimeException("configData is null");
		}
		
		JSONObject configJson = JSON.parseObject(configData);
		String source_type = configJson.getString("source_type");
		if(StringUtils.isEmpty(source_type)) {
			throw new RuntimeException("source_type is null");
		}
		
		String jdbc_url = configJson.getString("jdbc_url");
		if(StringUtils.isEmpty(jdbc_url)) {
			throw new RuntimeException("jdbc_url is null");
		}
		
		String jdbc_username = configJson.getString("jdbc_username");
		if(StringUtils.isEmpty(jdbc_username)) {
			throw new RuntimeException("jdbc_username is null");
		}
		
		String jdbc_password = configJson.getString("jdbc_password");
		if(StringUtils.isEmpty(jdbc_password)) {
			throw new RuntimeException("jdbc_password is null");
		}
		
		String sql = configJson.getString("sql");
		if(StringUtils.isEmpty(sql)) {
			throw new RuntimeException("sql is null");
		}
		
		DbTools dbTools = new DbTools(source_type,jdbc_url,jdbc_username,jdbc_password);
		logger.warn(sql);
		dbTools.execute(formateParam(sql));
		
		return sql;
	}
	
	/**
	 * 
	 * @Title: formateParam
	 * @Description: sql参数处理
	 * @param @param sql
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	private String formateParam(String sql) {
		Pattern pattern = Pattern.compile("#\\{.*?\\}");
		Matcher matcher = pattern.matcher(sql);
		String paramStr,param;
		
		Map<String,String> temp = new HashMap<String,String>();
		while (matcher.find()) {
			paramStr = matcher.group();
			if(temp.containsKey(paramStr)) {
				continue;
			}else {
				param = paramStr.substring(2, paramStr.length()-1);
				temp.put(paramStr, getParam(param));
			}
			
		}
		if(!temp.isEmpty()) {
			Set<String> keySey = temp.keySet();
			for(String key:keySey) {
				sql = sql.replaceAll(key.replaceAll("\\{", "\\\\{").replaceAll("\\}", "\\\\}").replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)"), temp.get(key));
			}
		}
		return sql;
	}
	
	private String getParam(String param) {
		if(param.startsWith("Date(")) {
			Integer endIndex = param.indexOf(")");
			String dateModel = param.substring(5,endIndex);
			if(endIndex == param.length()-1) {
				return DateTimeUtils.DateToString(new Date(), dateModel);
			}else {
				int amount = Integer.valueOf(param.substring(endIndex+1).trim().replaceAll(" ",""));
				return DateTimeUtils.DateToString(DateTimeUtils.addDays(new Date(), amount), dateModel);
			}
		}else if(param.startsWith("DateLong(")){
			Date current = DateTimeUtils.getTimesmorning();
			
			Integer endIndex = param.indexOf(")");
			if(endIndex == param.length()-1) {
				return String.valueOf(current.getTime());
			}else {
				int amount = Integer.valueOf(param.substring(endIndex+1).trim().replaceAll(" ",""));
				return String.valueOf(DateTimeUtils.addDays(current, amount).getTime());
			}
		}else {
			throw new RuntimeException("暂不支持的参数类型："+param);
		}
	}

	
	public static void main(String[] args) {
		String content = "select XXX #{Date(yyyy-MM-dd)} where #{Date(yyyy-MM-dd)-1} and #{DateLong()} and #{DateLong()-1}";
		System.out.println(new DbSqlRunnable().formateParam(content));
	}
}

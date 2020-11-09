package com.founder.scheduler.runnable.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.founder.scheduler.runnable.SchedulerRunnable;
import com.founder.scheduler.utils.HttpRequestBean;

/**
 * ****************************************************************************
 * @Package:      [com.founder.scheduler.runnable.impl.HttpRequestRunnable.java]  
 * @ClassName:    [HttpRequestRunnable]   
 * @Description:  [定时请求http接口]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2019年12月16日 下午5:17:54]   
 * @UpdateUser:   [Dagger(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2019年12月16日 下午5:17:54，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class HttpRequestRunnable extends SchedulerRunnable{
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public String execute() {
		String configData = super.getEntity().getConfigdata();
		if(StringUtils.isEmpty(configData)) {
			throw new RuntimeException("configData is null");
		}
		
		JSONObject configJson = JSON.parseObject(configData);
		String url = configJson.getString("url");
		if(StringUtils.isEmpty(url)) {
			throw new RuntimeException("url is null");
		}
		
		int connectTimeout = 30*1000;
		int connectionRequestTimeout=30*1000;
		int socketTimeout=5*60*1000;
		
		String connectTimeoutStr = configJson.getString("connectTimeout");
		if(!StringUtils.isEmpty(connectTimeoutStr)) {
			connectTimeout = Integer.valueOf(connectTimeoutStr);
		}
		
		String connectionRequestTimeoutStr = configJson.getString("connectionRequestTimeout");
		if(!StringUtils.isEmpty(connectionRequestTimeoutStr)) {
			connectionRequestTimeout = Integer.valueOf(connectionRequestTimeoutStr);
		}
		
		String socketTimeoutStr = configJson.getString("socketTimeout");
		if(!StringUtils.isEmpty(socketTimeoutStr)) {
			socketTimeout = Integer.valueOf(socketTimeoutStr);
		}
		
		Map<String,Object> paramMap = JSON.parseObject(configJson.getString("paramMap"),Map.class);
		
		HttpRequestBean httpRequestBean = new HttpRequestBean(connectTimeout,connectionRequestTimeout,socketTimeout);
		String methodType = configJson.getString("methodType");
		if(StringUtils.isEmpty(methodType)) {
			methodType="GET";
		}
		
		String res = null;
		if("POST".equalsIgnoreCase(methodType)) {
			res = httpRequestBean.doHttpPost(url, paramMap);
		}else {
			res = httpRequestBean.doHttpGet(url, paramMap);
		}
		
		 
		logger.warn(res);
		return res;
	}
	

}

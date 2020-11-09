package com.founder.scheduler.utils;

import java.util.Map;

import org.apache.http.client.methods.HttpPost;

public class HttpUtil {
	private static HttpRequestBean httpRequestBean = new HttpRequestBean(30*1000,30*1000,5*60*1000);

	/**
	 * 
	 * @Title: doHttpRequest
	 * @Description: 执行http请求
	 * @param @param httppost
	 * @param @param paramMap
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String doHttpRequest(HttpPost httppost,Map<String,Object> paramMap){
		httppost.setEntity(httpRequestBean.getPostEntity(paramMap));
		return httpRequestBean.doHttp(httppost);
	}
	
	/**
	 * 
	 * @Title: dotMultipartRequest
	 * @Description: 执行带文件的http请求
	 * @param @param httppost
	 * @param @param paramMap
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String dotMultipartRequest(HttpPost httppost,Map<String,Object> paramMap){
		httppost.setEntity(httpRequestBean.getMultipartEntity(paramMap));
		return httpRequestBean.doHttp(httppost);
	}

	/**
	 * 
	 * @Title: doHttpRequest
	 * @Description: 执行http请求
	 * @param @param url
	 * @param @param methodType
	 * @param @param paramMap
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String doHttpRequest(String url,String methodType,Map<String,Object> paramMap){
		if("GET".equalsIgnoreCase(methodType)){
			return httpRequestBean.doHttpGet(url, paramMap);
		}else{
			return httpRequestBean.doHttpPost(url, paramMap);
		}
	}
}

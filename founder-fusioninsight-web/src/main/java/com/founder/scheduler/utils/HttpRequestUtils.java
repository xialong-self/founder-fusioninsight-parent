//package com.founder.scheduler.utils;
//
//
//import java.io.*;
//import java.nio.charset.Charset;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import javax.servlet.http.HttpServletResponse;
//
//import org.apache.http.Header;
//import org.apache.http.HttpEntity;
//import org.apache.http.HttpResponse;
//import org.apache.http.NameValuePair;
//import org.apache.http.client.HttpClient;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.config.RequestConfig.Builder;
//import org.apache.http.client.entity.UrlEncodedFormEntity;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.client.methods.HttpRequestBase;
//import org.apache.http.entity.ContentType;
//import org.apache.http.entity.mime.MultipartEntityBuilder;
//import org.apache.http.impl.client.HttpClients;
//import org.apache.http.message.BasicNameValuePair;
//import org.apache.http.util.EntityUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.web.multipart.commons.CommonsMultipartFile;
//
///**
// * ****************************************************************************
// * @Package:      [com.founder.drools.core.request.HttpRequestBean.java]  
// * @ClassName:    [HttpRequestBean]   
// * @Description:  [httpclient调用的简单封装]   
// * @Author:       [zhang.hai@founder.com.cn]  
// * @CreateDate:   [2015年11月24日 上午11:08:48]   
// * @UpdateUser:   [ZhangHai(如多次修改保留历史记录，增加修改记录)]   
// * @UpdateDate:   [2015年11月24日 上午11:08:48，(如多次修改保留历史记录，增加修改记录)]   
// * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
// * @Version:      [v1.0]
// */
//public class HttpRequestUtils {
//	private Logger logger = LoggerFactory.getLogger(this.getClass());
//	
//	private int connectTimeout=10000;
//	private int connectionRequestTimeout=10000;
//	private int socketTimeout=60000;
//	private HttpClient httpClient=null;
//	private RequestConfig requestConfig;
//	
//	/**
//	 * 
//	 * <p>Title:HttpRequestBean </p>
//	 * <p>Description: 构造HttpRequestBean对象</p>
//	 * @param connectTimeout 连接超时时间，毫秒
//	 * @param connectionRequestTimeout 请求超时时间，毫秒
//	 * @param socketTimeout 返回超时时间，毫秒
//	 */
//	public HttpRequestUtils(int connectTimeout,int connectionRequestTimeout,int socketTimeout){
//		this.connectTimeout = connectTimeout;		
//		this.connectionRequestTimeout = connectionRequestTimeout;
//		this.socketTimeout=socketTimeout;
//		init();
//	}
//	
//	/**
//	 * 
//	 * <p>Title: HttpRequestBean</p>
//	 * <p>Description:构造HttpRequestBean对象 </p>
//	 */
//	public HttpRequestUtils(){
//		init();
//	}	
//	
//	private void init(){
//		Builder builder = RequestConfig.custom();
//		builder.setConnectTimeout(connectTimeout);//连接超时			
//		builder.setConnectionRequestTimeout(connectionRequestTimeout);//请求超时
//		builder.setSocketTimeout(socketTimeout);//返回超时
//		this.requestConfig = builder.build();
//	}
//		
//	/**
//	 * 
//	 * @Title: doHttpGet
//	 * @Description: 通过HttpGet请求服务方法
//	 * @param @param params GET方式的参数String
//	 * @param @return
//	 * @param @throws Exception    设定文件
//	 * @return String    返回类型
//	 * @throw
//	 */
//	public String doHttpGet(String serviceUrl,Map<String,Object> paramMap) {
//		
//		serviceUrl = formatUrl(serviceUrl);
//		
//		String params = this.getGetParams(paramMap);
//		
//		if(params!=null && params.length()>0)
//			serviceUrl =serviceUrl+"?"+params;
//		System.out.println("Get request from:"+serviceUrl);
//		
//		HttpGet httpRequst = new HttpGet(serviceUrl);
//
//		return this.doHttp(httpRequst);
//	}
//	
//	public String formatUrl(String serviceUrl){
//		if(serviceUrl==null || serviceUrl.trim().length()==0){
//			throw new RuntimeException("ServiceUrl can not be null!");			
//		}
//		serviceUrl=serviceUrl.trim();
//		if(serviceUrl.endsWith("/"))
//			serviceUrl=serviceUrl.substring(0,serviceUrl.length()-1);
//		
//		return serviceUrl;
//	}
//	
//	public HttpRequestBase getHttpGet(String serviceUrl,Map<String,Object> paramMap){
//		serviceUrl = formatUrl(serviceUrl);
//		
//		String params = this.getGetParams(paramMap);
//		
//		if(params!=null && params.length()>0)
//			serviceUrl =serviceUrl+"?"+params;
//		HttpGet httpRequst = new HttpGet(serviceUrl);	
//		
//		return httpRequst;
//	}
//	
//	public HttpRequestBase getHttpPost(String serviceUrl,Map<String,Object> paramMap){
//		serviceUrl = formatUrl(serviceUrl);
//		
//		HttpPost httpRequst = new HttpPost(serviceUrl);//创建HttpPost对象
//		
//		httpRequst.setEntity(this.getPostEntity(paramMap));
//		
//		return httpRequst;
//	}
//	
//	public HttpRequestBase getHttpPostMultipart(String serviceUrl,Map<String,Object> paramMap){
//		serviceUrl = formatUrl(serviceUrl);
//		
//		HttpPost httpRequst = new HttpPost(serviceUrl);//创建HttpPost对象
//		
//		httpRequst.setEntity(this.getMultipartEntity(paramMap));;	
//		
//		return httpRequst;
//	}
//	
//	/**
//	 * 
//	 * @Title: getGetParams
//	 * @Description: get方法的参数处理
//	 * @param @param paramMap
//	 * @param @return
//	 * @param @throws Exception    设定文件
//	 * @return String    返回类型
//	 * @throw
//	 */
//	private String getGetParams(Map<String,Object> paramMap){		
//		if(paramMap==null || paramMap.isEmpty())
//			return null;
//		
//		StringBuffer params=new StringBuffer();  	
//		Set<String> keySet = paramMap.keySet();
//		for(String key:keySet){
//			params.append("&").append(key).append("=").append(paramMap.get(key));
//		}
//    	return params.toString().substring(1);
//	}
//	
//	/**
//	 * 
//	 * @Title: doHttpPost
//	 * @Description: 通过HttpPost请求服务方法
//	 * @param @param httpEntity
//	 * @param @return
//	 * @param @throws Exception    设定文件
//	 * @return String    返回类型
//	 * @throw
//	 */
//	public String doHttpPost(String serviceUrl,Map<String,Object> paramMap){
//		
//		serviceUrl = formatUrl(serviceUrl);
//		
//		HttpPost httpRequst = new HttpPost(serviceUrl);//创建HttpPost对象
//		
//		httpRequst.setEntity(this.getPostEntity(paramMap));
//		
//    	return this.doHttp(httpRequst);
//	}
//	
//	public String doHttp(HttpRequestBase httpRequst) {
//		try {
//			
//			httpRequst.setConfig(this.requestConfig);
//			HttpResponse httpResponse = getHttpClient().execute(httpRequst);
//		    if(httpResponse.getStatusLine().getStatusCode() == 200){
//		    	HttpEntity httpEntity = httpResponse.getEntity();
//		    	String res=EntityUtils.toString(httpEntity);//取出应答字符串
//		    	return 	res;	    	    	  
//		    }else{
//		    	String errorMsg = "";
//		    	try{
//		    		errorMsg = EntityUtils.toString(httpResponse.getEntity());
//		    		int beginIndex = errorMsg.indexOf("<p style=\"font-size: 20px;color: #999999; text-align: left;padding: 150px 10px 0 200px;overflow: hidden;\">");
//		        	if(beginIndex>0){
//		        		errorMsg = errorMsg.substring(beginIndex);
//		        		errorMsg = errorMsg.substring(106,errorMsg.indexOf("</p>"));
//		        	}
//		    	}catch(Exception e){}
//		    	throw new RuntimeException("Service request faild["+httpResponse.getStatusLine().getStatusCode()+"]:"+errorMsg);	
//		    }
//		    
//		} catch (Exception e) {			
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		}finally{
//			httpRequst.releaseConnection();	
//		}
//	}
//
//	public void doHttpResult(HttpRequestBase httpRequst,HttpServletResponse response){
//
//		try {
//			httpRequst.setConfig(this.requestConfig);
//			HttpResponse httpResponse = getHttpClient().execute(httpRequst);
//			HttpEntity httpEntity = httpResponse.getEntity();
//			String s = EntityUtils.toString(httpEntity);
//			logger.warn(s);
//			int totalIndex = s.indexOf("total");
//			String totalNum = "0";
//			if(totalIndex >=0) {
//				totalNum = s.substring(totalIndex+7,totalIndex+8);
//			}
//			if(s.indexOf("success") != -1 || !"0".equals(totalNum)){
//				InputStream in = null;
//				OutputStream out = null;
//				try {
////					HttpEntity httpEntity = httpResponse.getEntity();
//					in = new ByteArrayInputStream(s.getBytes());
//
//					Header[] headers = httpResponse.getAllHeaders();
//					for(Header header:headers){
//						response.addHeader(header.getName(),header.getValue());
//					}
//					out = response.getOutputStream();
//					byte[] readAry=new byte[1024];
//					int readLength;
//					while((readLength = in.read(readAry)) > 0){
//						out.write(readAry,0,readLength);
//					}
//					out.flush();
//				} catch (Exception e) {
//					throw new RuntimeException(e.getLocalizedMessage());
//				} finally {
//					if (out != null) {
//						try {
//							out.close();
//						} catch (Exception e) {
//						}
//						try {
//							in.close();
//						} catch (Exception e) {
//						}
//					}
//				}
//			}else{
//				String errorMsg = "";
//				if(totalIndex != -1 && "0".equals(totalNum)) {
//					errorMsg = "未查询到相关信息！";
//				}else {
//					try{
//						errorMsg = s;
//						int beginIndex = errorMsg.indexOf("errors");
//						if(beginIndex>0){
//							errorMsg = errorMsg.substring(beginIndex+9,errorMsg.indexOf(",")-1);
//						}
//					}catch(Exception e){}
//				}
//				response.sendError(500, errorMsg);
//			}
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}finally{
//			httpRequst.releaseConnection();
//		}
//	}
//	public void doHttpIO(HttpRequestBase httpRequst,HttpServletResponse response) {
//		try {
//			httpRequst.setConfig(this.requestConfig);
//			HttpResponse httpResponse = getHttpClient().execute(httpRequst);
//			if(httpResponse.getStatusLine().getStatusCode() == 200){
//				InputStream in = null;
//				OutputStream out = null;
//		        try {
//		        	HttpEntity httpEntity = httpResponse.getEntity();
//		        	in = httpEntity.getContent();
//
//		        	Header[] headers = httpResponse.getAllHeaders();
//		        	for(Header header:headers){
//		        		response.addHeader(header.getName(),header.getValue());
//		        	}
//		            out = response.getOutputStream();
//		            byte[] readAry=new byte[1024];
//		            int readLength;
//		            while((readLength = in.read(readAry)) > 0){
//		            	out.write(readAry,0,readLength);
//		            }
//		            out.flush();
//		        } catch (Exception e) {
//		        	throw new RuntimeException(e.getLocalizedMessage());
//		        } finally {
//		            if (out != null) {
//		                try {
//		                    out.close();
//		                } catch (Exception e) {
//		                }
//		                try {
//		                	in.close();
//		                } catch (Exception e) {
//		                }
//		            }
//		        }    	    	  
//		    }else{
//		    	String errorMsg = "";
//		    	try{
//		    		errorMsg = EntityUtils.toString(httpResponse.getEntity());
//		        	int beginIndex = errorMsg.indexOf("<p style=\"font-size: 20px;color: #999999; text-align: left;padding: 150px 10px 0 200px;overflow: hidden;\">");
//		        	if(beginIndex>0){
//		        		errorMsg = errorMsg.substring(beginIndex);
//		        		errorMsg = errorMsg.substring(106,errorMsg.indexOf("</p>"));
//		        	}
//		    	}catch(Exception e){}
//		    	response.sendError(httpResponse.getStatusLine().getStatusCode(), errorMsg);
//		    }
//		} catch (Exception e) {			
//			throw new RuntimeException(e);
//		}finally{
//			httpRequst.releaseConnection();	
//		}
//	}
//	
//	public HttpClient getHttpClient() {
//		if(this.httpClient==null){
//			return HttpClients.createDefault();
//		}else{
//			return this.httpClient;
//		}
//	}
//
//	public void setHttpClient(HttpClient httpClient) {
//		this.httpClient = httpClient;
//	}
//	
//	/**
//	 * 
//	 * @Title: getPostEntity
//	 * @Description: 生成post对象
//	 * @param @param paramMap
//	 * @param @return
//	 * @param @throws Exception    设定文件
//	 * @return HttpEntity    返回类型
//	 * @throws UnsupportedEncodingException 
//	 * @throw
//	 */
//	public HttpEntity getPostEntity(Map<String,Object> paramMap){		
//		if(paramMap==null || paramMap.isEmpty())
//			return null;
//		
//		List<NameValuePair> params = new ArrayList<NameValuePair>();    	
//		Set<String> keySet = paramMap.keySet();
//		for(String key:keySet){
//			params.add(new BasicNameValuePair(key, paramMap.get(key).toString()));
//		}
//		try {
//			return new UrlEncodedFormEntity(params,"UTF-8");
//		} catch (UnsupportedEncodingException e) {
//			throw new RuntimeException(e);
//		}
//	}
//	
//	/**
//	 * 
//	 * @Title: getMultipartEntity
//	 * @Description: 需要上传文件时的方法
//	 * @param @param paramMap
//	 * @param @return    设定文件
//	 * @return HttpEntity    返回类型
//	 * @throw
//	 */
//	public HttpEntity getMultipartEntity(Map<String,Object> paramMap){	
//		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
//		multipartEntityBuilder.setCharset(Charset.forName("UTF-8"));
//		Set<String> keySet = paramMap.keySet();
//		for(String key:keySet){
//			Object val = paramMap.get(key);
//			if(val == null){
//				continue;
//			}
//			
//			if(val instanceof String){
//				multipartEntityBuilder.addTextBody(key, (String)val,ContentType.create("text/plain","UTF-8"));
//			}else if(val instanceof CommonsMultipartFile){
//				CommonsMultipartFile file = (CommonsMultipartFile)val;
//				try {
//					multipartEntityBuilder.addBinaryBody(key, file.getInputStream(), ContentType.MULTIPART_FORM_DATA, file.getName());
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//			}else if(val instanceof CommonsMultipartFile[]){
//				CommonsMultipartFile[] files = (CommonsMultipartFile[])val;
//				for(CommonsMultipartFile file:files){
//					try {
//						multipartEntityBuilder.addBinaryBody(key, file.getInputStream(), ContentType.MULTIPART_FORM_DATA, file.getName());
//					} catch (IOException e) {
//						throw new RuntimeException(e);
//					}
//				}
//			}else{
//				throw new RuntimeException("不支持的参数类型");
//			}
//		}
//		
//		return multipartEntityBuilder.build();
//	}
//
//}

package com.founder.fusioninsight.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.founder.fusioninsight.config.FusioninsightConfig;

/**
 * ****************************************************************************
 * @Package:      [com.founder.fusioninsight.es.EsRequest.java]  
 * @ClassName:    [EsRequest]   
 * @Description:  [ES 请求]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月8日 下午2:21:53]   
 * @UpdateUser:   [zhanghai(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月8日 下午2:21:53，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class EsRequest {
	private static final Logger logger = LoggerFactory.getLogger(EsRequest.class);
	
	private static RestClient restClient = null;

    /**
     * Get hostArray by esServerHost of cluster in property file
     * @param esServerHost
     * @return
     */
    public static HttpHost[] getHostArray(String esServerHost){
        List<HttpHost> hosts=new ArrayList<HttpHost>();
        String[] hostArray1 = esServerHost.split(",");

        for(String host:hostArray1) {
            String[] ipPort= host.split(":");
            HttpHost hostNew =new HttpHost(ipPort[0],Integer.valueOf(ipPort[1]),FusioninsightConfig.getInstance().getEsConfig().getSchema());
            hosts.add(hostNew);
        }
        return hosts.toArray(new HttpHost[] {});
    }

    /**
     * Get one rest client instance.
     * @return
     * @throws Exception
     */
    private static RestClientBuilder getRestClientBuilder(HttpHost[] HostArray) {
    	final CredentialsProvider credentialsProvider =
    		    new BasicCredentialsProvider();
    		credentialsProvider.setCredentials(AuthScope.ANY,
    		    new UsernamePasswordCredentials("dcjt_user", "Dcjt@123"));
    		
        RestClientBuilder builder = null;
        if (FusioninsightConfig.getInstance().getEsConfig().isSecureMode()) {
            System.setProperty("es.security.indication", "true");
            System.setProperty("elasticsearch.kerberos.jaas.appname", "EsClient");
            builder = RestClient.builder(HostArray).setHttpClientConfigCallback(new HttpClientConfigCallback() {
            	        @Override
            	        public HttpAsyncClientBuilder customizeHttpClient(
            	                HttpAsyncClientBuilder httpClientBuilder) {
            	            return httpClientBuilder
            	                .setDefaultCredentialsProvider(credentialsProvider);
            	        }
            	    });
        } else {
            System.setProperty("es.security.indication", "false");
            builder = RestClient.builder(HostArray);
        }
        Header[] defaultHeaders = new Header[] { new BasicHeader("Accept", "application/json"),
                new BasicHeader("Content-type", "application/json") };
        builder.setDefaultHeaders(defaultHeaders);
        builder.setMaxRetryTimeoutMillis(FusioninsightConfig.getInstance().getEsConfig().getMaxRetryTimeoutMillis());
        builder.setFailureListener(new RestClient.FailureListener(){
            public void onFailure(HttpHost host){
                //trigger some actions when failure occurs
            }
        });
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback(){

            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(FusioninsightConfig.getInstance().getEsConfig().getConnectTimeout()).setSocketTimeout(FusioninsightConfig.getInstance().getEsConfig().getSocketTimeout());
            }
        });

        return builder;
    }
    
    /**
     * 
     * @Title: getRestClient
     * @Description: 获取RestClient
     * @param @return
     * @param @throws Exception    设定文件
     * @return RestClient    返回类型
     * @throw
     */
    public static RestClient getRestClient(){
        if(restClient == null) {
        	HttpHost[] HostArray = getHostArray(FusioninsightConfig.getInstance().getEsConfig().getEsServerHost());
	        restClient = getRestClientBuilder(HostArray).build();
	        logger.info("The RestClient has been created !");
        }
        return restClient;
    }
    
    /**
     * 
     * @Title: closeRestClient
     * @Description: 关闭RestClient,如果一直使用，可以不关闭
     * @param     设定文件
     * @return void    返回类型
     * @throw
     */
    public static void closeRestClient() {
    	if(restClient != null) {
    		try {
				restClient.close();
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
    		restClient = null;
    	}
    }
    
    /**
     * 
     * @Title: queryEsBySql
     * @Description: 通过SQL查询ES
     * @param @param sql
     * @param @return    设定文件
     * @return String    返回类型
     * @throw
     */
	public static String queryEsBySql(String sql) {
		logger.warn(sql);
		try {
			restClient = getRestClient();
			HttpEntity entity = new NStringEntity(sql, ContentType.APPLICATION_JSON) ;
			Request request = new Request("GET", "_sql");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
			Response response = restClient.performRequest(request);
			if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode() ||
	                HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
				return EntityUtils.toString(response.getEntity());
            } else {
            	String errors = EntityUtils.toString(response.getEntity());
            	throw new RuntimeException("es request fail: "+errors);
            }
		}catch (IOException e){
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 
	 * @Title: formateEs
	 * @Description: 格式化ES返回值
	 * @param @param data
	 * @param @return    设定文件
	 * @return JSONArray    返回类型
	 * @throw
	 */
	public static JSONArray formateEs(String data) {
		JSONObject json = JSON.parseObject(data);
		Integer took = json.getInteger("took");//查询时间
		logger.info("es query took:"+took);
		
		JSONObject hits = json.getJSONObject("hits");
		Integer total = hits.getInteger("total");
		logger.info("es res total num:"+total);
		return hits.getJSONArray("hits");
	}
	
	/**
	 * 
	 * @Title: formateEsJSONArray
	 * @Description: 格式化ES返回值，处理业务数据
	 * @param @param data
	 * @param @return    设定文件
	 * @return JSONArray    返回类型
	 * @throw
	 */
	public static JSONArray formateEsJSONArray(JSONArray hitsAry) {
		JSONObject json;
		String es_id;
		for(Object obj:hitsAry) {
			json = (JSONObject)obj;
			es_id = json.getString("_id");
			JSONObject entity = json.getJSONObject("_source");
			entity.put("es_id", es_id);
			obj = entity;
		}
		
		return hitsAry;
	}
	
	/**
	 * 
	 * @Title: formateEsJSONObject
	 * @Description: 格式化ES返回值，返回一条数据
	 * @param @param data
	 * @param @return    设定文件
	 * @return JSONObject    返回类型
	 * @throw
	 */
	public static JSONObject formateEsJSONObject(String data) {
		JSONArray hitsAry = formateEs(data);
		if(hitsAry.size()>1) {
			throw new RuntimeException("result data has too many size "+hitsAry.size());
		}
		
		JSONObject json = hitsAry.getJSONObject(0);
		String es_id;
		es_id = json.getString("_id");
		JSONObject entity = json.getJSONObject("_source");
		entity.put("es_id", es_id);
		
		return entity;
	}

	/**
	 * 
	 * @Title: loadIndex
	 * @Description: 查询index
	 * @param @param index
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String loadIndex() {
		try {
			restClient = getRestClient();
			Request request = new Request("GET", "_cat/indices");            
			Response response = restClient.performRequest(request);
			if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode() ||
	                HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
				return EntityUtils.toString(response.getEntity());
            } else {
            	String errors = EntityUtils.toString(response.getEntity());
            	throw new RuntimeException("es request fail: "+errors);
            }
		}catch (IOException e){
			throw new RuntimeException(e);
		}
		
	}
	
	public static String queryEsByJson(String index,String json) {
		logger.warn(json);
		try {
			restClient = getRestClient();
			HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON) ;
			Request request = new Request("GET", index+"/_doc/_search");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
			Response response = restClient.performRequest(request);
			if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode() ||
	                HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
				return EntityUtils.toString(response.getEntity());
            } else {
            	String errors = EntityUtils.toString(response.getEntity());
            	throw new RuntimeException("es request fail: "+errors);
            }
		}catch (IOException e){
			throw new RuntimeException(e);
		}
	}		
}

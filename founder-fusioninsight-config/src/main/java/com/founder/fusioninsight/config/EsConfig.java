package com.founder.fusioninsight.config;

import java.io.Serializable;

/**
 * ****************************************************************************
 * @Package:      [com.founder.fusioninsight.config.EsConfig.java]  
 * @ClassName:    [EsConfig]   
 * @Description:  [华为大数据平台ES配置]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月8日 下午1:48:10]   
 * @UpdateUser:   [zhanghai(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月8日 下午1:48:10，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class EsConfig implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private boolean isSecureMode = false;
    private String esServerHost;
    private int maxRetryTimeoutMillis = 60000;
    private int connectTimeout = 5000;
    private int socketTimeout = 60000;
    private String schema = "https";
	public boolean isSecureMode() {
		return isSecureMode;
	}
	public void setSecureMode(boolean isSecureMode) {
		this.isSecureMode = isSecureMode;
	}
	public String getEsServerHost() {
		return esServerHost;
	}
	public void setEsServerHost(String esServerHost) {
		this.esServerHost = esServerHost;
	}
	public int getMaxRetryTimeoutMillis() {
		return maxRetryTimeoutMillis;
	}
	public void setMaxRetryTimeoutMillis(int maxRetryTimeoutMillis) {
		this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
	}
	public int getConnectTimeout() {
		return connectTimeout;
	}
	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
	public int getSocketTimeout() {
		return socketTimeout;
	}
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
}

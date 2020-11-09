package com.founder.fusioninsight.config;

import java.io.Serializable;

/**
 * ****************************************************************************
 * @Package:      [com.founder.fusioninsight.config.UserConfig.java]  
 * @ClassName:    [UserConfig]   
 * @Description:  [华为大数据平台用户配置]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月8日 下午1:47:56]   
 * @UpdateUser:   [zhanghai(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月8日 下午1:47:56，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class UserConfig implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private String home;
	private String logpath;
    private String username;
    private String password;
    private String authpath;
    private boolean isSecureMode = false;
	public String getHome() {
		return home;
	}
	public void setHome(String home) {
		this.home = home;
	}
	public String getLogpath() {
		return logpath;
	}
	public void setLogpath(String logpath) {
		this.logpath = logpath;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getAuthpath() {
		return authpath;
	}
	public void setAuthpath(String authpath) {
		this.authpath = authpath;
	}
	public boolean isSecureMode() {
		return isSecureMode;
	}
	public void setSecureMode(boolean isSecureMode) {
		this.isSecureMode = isSecureMode;
	}
	
}

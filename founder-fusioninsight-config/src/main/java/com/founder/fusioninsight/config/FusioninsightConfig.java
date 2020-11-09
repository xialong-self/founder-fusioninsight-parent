package com.founder.fusioninsight.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.huawei.hadoop.security.LoginUtil;

/**
 * ****************************************************************************
 * @Package:      [com.founder.fusioninsight.config.FusioninsightConfig.java]  
 * @ClassName:    [FusioninsightConfig]   
 * @Description:  [华为Fusioninsight平台配置文件]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年8月7日 上午10:04:54]   
 * @UpdateUser:   [zhanghai(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年8月7日 上午10:04:54，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class FusioninsightConfig {
	private static FusioninsightConfig instance = null;
	
	private EsConfig esConfig;
	private UserConfig userConfig;
	
	private static String driverHost = null;//driver主机IP，默认取当前机器IP吗，可通过启动参数传入
	
	/**
	 * 
	 * @Title: initArgs
	 * @Description: 初始化配置参数，返回spark的driverHost
	 * @param @param args
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String initArgs(String[] args) {
		/*
		 * 参数：
		--userProperties=Fusioninsight用户信息配置文件
		--esProperties=Fusioninsight的ES配置文件
		--driverHost=当前运行机器的对外IP
		 */
		if(args!=null) {
    		for(String arg:args) {
    			if(arg.trim().startsWith("--userProperties=")) {
    				String userProperties = arg.trim().substring(17);
    				System.out.println("使用外部配置文件："+userProperties);
    				
    				System.setProperty("userProperties",userProperties);
    			}
    			
    			if(arg.trim().startsWith("--esProperties=")) {
    				String esProperties = arg.trim().substring(15);
    				System.out.println("使用外部配置文件："+esProperties);
    				System.setProperty("esProperties",esProperties);
    			}
    			
    			if(arg.trim().startsWith("--driverHost=")) {
    				driverHost = arg.trim().substring(13);
    				System.out.println("统计配置IP："+driverHost);
    			}
    		}
    	}
		
		if(driverHost==null || driverHost.trim().length()==0) {
			try {
				driverHost = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		}
		
		return driverHost;
	}
	
	/**
	 * 
	 * @Title: getInstance
	 * @Description: 单例模式
	 * @param @return    设定文件
	 * @return FusioninsightConfig    返回类型
	 * @throw
	 */
	public static FusioninsightConfig getInstance() {
		if(instance == null) {
			instance = new FusioninsightConfig();
		}
		
		return instance;
	}
	
	private FusioninsightConfig() {
		loadUserConfig();
		loadEsConfig();
	}
	
	/**
	 * 
	 * @Title: loadEsConfig
	 * @Description: 加载ES配置
	 * @param     设定文件
	 * @return void    返回类型
	 * @throw
	 */
	private void loadEsConfig() {
		Properties properties = new Properties();
		
		try {
			InputStream in;
			String esProperties = System.getProperty("esProperties");
			if(StringUtils.isEmpty(esProperties)) {
				in = ClassLoader.getSystemResourceAsStream("es.properties");
			}else {
				in = new FileInputStream(new File(esProperties));
			}
			
			properties.load(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		esConfig = new EsConfig();
		String esServerHost = properties.getProperty("esServerHost");
		if(StringUtils.isEmpty(esServerHost)) {
			throw new RuntimeException("esServerHost can not be null!");
		}
		esConfig.setEsServerHost(esServerHost);
		
        String maxRetryTimeoutMillis = properties.getProperty("maxRetryTimeoutMillis");
        if(!StringUtils.isEmpty(maxRetryTimeoutMillis)) {
        	esConfig.setMaxRetryTimeoutMillis(Integer.valueOf(maxRetryTimeoutMillis));
		}
        
        
        String connectTimeout = properties.getProperty("connectTimeout");
        if(!StringUtils.isEmpty(connectTimeout)) {
        	esConfig.setConnectTimeout(Integer.valueOf(connectTimeout));
		}
        
        
        String socketTimeout = properties.getProperty("socketTimeout");
        if(!StringUtils.isEmpty(socketTimeout)) {
        	esConfig.setSocketTimeout(Integer.valueOf(socketTimeout));
		}
        
        
        String isSecureMode = properties.getProperty("isSecureMode");
        if("true".equals(isSecureMode)) {
        	esConfig.setSecureMode(true);
        	esConfig.setSchema("https");
		}else {
			esConfig.setSecureMode(false);
        	esConfig.setSchema("http");
		}
	}
	
	/**
	 * 
	 * @Title: loadUserConfig
	 * @Description: 加载用户配置
	 * @param     设定文件
	 * @return void    返回类型
	 * @throw
	 */
	private void loadUserConfig() {
		Properties properties = new Properties();
		
		try {
			InputStream in;
			String userProperties = System.getProperty("userProperties");
			if(StringUtils.isEmpty(userProperties)) {
				in = ClassLoader.getSystemResourceAsStream("user.properties");
			}else {
				in = new FileInputStream(new File(userProperties));
			}
			
			properties.load(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		userConfig = new UserConfig();
		
		String home = properties.getProperty("home");
		if(StringUtils.isEmpty(home)) {
			throw new RuntimeException("home can not be null!");
		}
		if(!home.endsWith("/")) {
			home+="/";
		}
		userConfig.setHome(home);
		
		String username = properties.getProperty("username");
		if(StringUtils.isEmpty(username)) {
			throw new RuntimeException("username can not be null!");
		}
		userConfig.setUsername(username);
		
		String password = properties.getProperty("password");
		if(StringUtils.isEmpty(password)) {
			throw new RuntimeException("password can not be null!");
		}
		userConfig.setPassword(password);
		
		String logpath = properties.getProperty("logpath");
		if(!StringUtils.isEmpty(logpath)) {
			userConfig.setLogpath(logpath);
		}
		
		String isSecureMode = properties.getProperty("isSecureMode");
        if("true".equals(isSecureMode)) {
        	userConfig.setSecureMode(true);
		}else {
			userConfig.setSecureMode(false);
		}
        
        
        String authpath = properties.getProperty("authpath");
		if(StringUtils.isEmpty(authpath)) {
			throw new RuntimeException("authpath can not be null!");
		}
		
		if(!authpath.endsWith("/")) {
			authpath+="/";
		}
		userConfig.setAuthpath(authpath);
	}
	
	/**
	 * 
	 * @Title: login
	 * @Description: 登录华为大数据平台
	 * @param     设定文件
	 * @return void    返回类型
	 * @throw
	 */
	public void login() {
	    try {
	    	//String classpath = ClassLoader.getSystemResource("").getFile();
	    	String userKeytabPath = userConfig.getAuthpath()+"user.keytab";
	 	    String krb5ConfPath = userConfig.getAuthpath()+"/krb5.conf";
	        //generate jaas file, which is in the same dir with keytab file
	        LoginUtil.setJaasFile(userConfig.getUsername(),userKeytabPath);
	        Configuration hadoopConf = new Configuration();
	        LoginUtil.login(userConfig.getUsername(), userKeytabPath, krb5ConfPath, hadoopConf);
	    }catch(Exception e) {
	    	throw new RuntimeException(e);
	    }
	}

	public EsConfig getEsConfig() {
		return esConfig;
	}

	public UserConfig getUserConfig() {
		return userConfig;
	}
	
	private Random random = new Random();
	private String[] esServerHosts = null;
	
	/**
	 * 
	 * @Title: getRandomEsServerHost
	 * @Description: 获取随机的ES结点
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public String getRandomEsServerHost() {
		if(esServerHosts == null) {
			esServerHosts = esConfig.getEsServerHost().split(",");
		}
		
		if(esServerHosts.length==1) {
			return esServerHosts[0];
		}else {
			return esServerHosts[random.nextInt(esServerHosts.length)];
		}
	}
	
	/**
	 * 
	 * @Title: getDriverHost
	 * @Description: 获取driver主机IP，默认取当前机器IP吗，可通过启动参数传入
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String getDriverHost() {
		return driverHost;
	}
}

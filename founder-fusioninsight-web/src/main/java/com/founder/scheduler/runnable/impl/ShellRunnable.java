package com.founder.scheduler.runnable.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.founder.scheduler.runnable.SchedulerRunnable;
import com.founder.scheduler.utils.RuntimeUtils;
import com.founder.scheduler.utils.SystemUtils;

/**
 * ****************************************************************************
 * @Package:      [com.founder.scheduler.runnable.impl.ShellRunnable.java]  
 * @ClassName:    [ShellRunnable]   
 * @Description:  [定时执行shell脚本]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月2日 上午9:46:05]   
 * @UpdateUser:   [42027(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月2日 上午9:46:05，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class ShellRunnable extends SchedulerRunnable{
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public String execute() {
		/*
		 * 	定时执行shell或者cmd脚本
		 */
		
		String configData = super.getEntity().getConfigdata();
		if(StringUtils.isEmpty(configData)) {
			throw new RuntimeException("configData is null");
		}
		
		JSONObject configJson = JSON.parseObject(configData);
		JSONArray scriptAry = configJson.getJSONArray("scripts");//执行的脚本
		if(scriptAry == null || scriptAry.isEmpty()) {
			throw new RuntimeException("scripts is null");
		}
		String logPath = configJson.getString("logPath");//日志路径
		if(!logPath.endsWith("/")) {
			logPath+="/";
		}
		String workDir = configJson.getString("workDir");//工作目录
		if(StringUtils.isEmpty(workDir)) {
			workDir = null;
		}else if(!workDir.endsWith("/")) {
			workDir+="/";
		}
		
		List<String> cmdList = new ArrayList<String>();
		String charsetName;
		if(SystemUtils.isWindows()) {
			logger.warn("run script on windows");
			cmdList.add("cmd");
			cmdList.add("/c");
			charsetName="GBK";
		}else {
			logger.warn("run script on linux");
			cmdList.add("/bin/bash");
			cmdList.add("-c");
			charsetName="UTF8";
		}
		
		for(Object script:scriptAry) {
			cmdList.add((String)script);
			cmdList.add("&&");
		}
		cmdList.remove(cmdList.size()-1);
		
		return RuntimeUtils.exec(workDir, cmdList, null, charsetName, logPath);
	}
	
}

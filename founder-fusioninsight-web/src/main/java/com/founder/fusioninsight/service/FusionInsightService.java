package com.founder.fusioninsight.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.founder.fusioninsight.config.FusioninsightConfig;
import com.founder.fusioninsight.config.UserConfig;

@Service
public class FusionInsightService{
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * 
	 * @Title: runLogin
	 * @Description: 登录华为FusionInsight平台
	 * @param     设定文件
	 * @return void    返回类型
	 * @throw
	 */
	@Scheduled(cron = "0 0 0,12 * * ?")
	public void runLogin() {
		logger.warn("登录 FusionInsight HD");
		/*
		 * 
		 * FusionInsight HD 平台每天需要登陆一次，每次新的会话也要登录，然后才可以使用大数据平台
		 * 可以反复登录
		 * 系统启动的时候先执行一次大数据平台的登录
		 * 然后定时启动
		 * 
		 */
		
		UserConfig userConfig = FusioninsightConfig.getInstance().getUserConfig();
		if(!userConfig.isSecureMode()) {
			return;//没有开启用户认证，不执行
		}
		
		/*
		 * List<String> cmdList = new ArrayList<String>();
		 * cmdList.add("cd "+userConfig.getHome()); cmdList.add("&&");
		 * cmdList.add("source bigdata_env"); cmdList.add("&&");
		 * cmdList.add("kinit "+FusioninsightConfig.getInstance().getUserConfig().
		 * getUsername()); cmdList.add("&&"); cmdList.add("klist");
		 * 
		 * RuntimeUtils.exec(userConfig.getHome(), cmdList, userConfig.getPassword(),
		 * "UTF8", userConfig.getLogpath());
		 */
		
		FusioninsightConfig.getInstance().login();
	}
	
}

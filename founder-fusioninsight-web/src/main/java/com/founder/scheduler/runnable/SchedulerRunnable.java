package com.founder.scheduler.runnable;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.founder.framework.utils.DateTimeUtils;
import com.founder.scheduler.config.SpringUtil;
import com.founder.scheduler.model.SchedulerConfig;
import com.founder.scheduler.model.SchedulerLog;
import com.founder.scheduler.service.SchedulerLogService;

import lombok.Data;

@Data
public abstract class SchedulerRunnable implements Runnable{
	private Logger logger = LoggerFactory.getLogger(SchedulerRunnable.class);
	
	private SchedulerConfig entity;

	@Override
	public void run() {
		logger.warn("执行定时任务["+entity.getId()+"]: " + LocalDateTime.now().toLocalTime());
		SchedulerLogService schedulerLogService = SpringUtil.getBean(SchedulerLogService.class);
		
		SchedulerLog log = new SchedulerLog();
		log.setTask_id(entity.getId());
		log.setStart_time(DateTimeUtils.getSystemDateTimeString());
		schedulerLogService.insert(log, null);
		try {
			log.setMsg(execute());
			log.setResult("SUCCESS");
		}catch(Exception e) {
			logger.error(e.getLocalizedMessage(),e);
			log.setResult("FAILED");
			log.setMsg(e.getLocalizedMessage());
		}
		log.setEnd_time(DateTimeUtils.getSystemDateTimeString());
		
		
		schedulerLogService.update(log, null);
	}
	
	public abstract String execute();//子类需要实现此方法
}

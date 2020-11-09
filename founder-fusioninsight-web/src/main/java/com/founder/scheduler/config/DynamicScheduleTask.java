package com.founder.scheduler.config;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import com.founder.framework.base.entity.WebCommonEntity;
import com.founder.fusioninsight.service.FusionInsightService;
import com.founder.scheduler.dicts.Dict_scheduleStatus;
import com.founder.scheduler.model.SchedulerConfig;
import com.founder.scheduler.runnable.SchedulerRunnable;
import com.founder.scheduler.service.SchedulerConfigService;

@Component
public class DynamicScheduleTask implements ApplicationRunner{
	private Logger logger = LoggerFactory.getLogger(DynamicScheduleTask.class);
	
	@Autowired
	private SchedulerConfigService schedulerConfigService;
	
	@Autowired
	private FusionInsightService fusionInsightService;

	@Autowired
	private ThreadPoolTaskScheduler threadPoolTaskScheduler ;
	
	private Map<String,ScheduledFuture<?>> scheduledFutures = new HashMap<String,ScheduledFuture<?>>();
	
	@Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        return new ThreadPoolTaskScheduler();
    }
    /**
     * 执行定时任务.
     */
    public void startSchedule() {
    	fusionInsightService.runLogin();//系统启动的时候先执行一次大数据平台的登录
    	
    	threadPoolTaskScheduler.setRemoveOnCancelPolicy(true);
    	
    	SchedulerConfig entity = new SchedulerConfig();
    	entity.setXt_zxbz("0");
    	List<WebCommonEntity> list = schedulerConfigService.queryListByEntity(entity);
    	if(list == null || list.isEmpty()) {
    		logger.warn("没有定时任务配置");
    		return;
    	}
    	
    	
    	for(WebCommonEntity task:list) {
    		addTask((SchedulerConfig)task);
    	}
    }

    public void addTask(SchedulerConfig task) {
    	SchedulerConfig updateEntity = new SchedulerConfig();
    	
		try {
			SchedulerRunnable runnable = getSchedulerRunnable(task);
			
			updateEntity.setId(task.getId());
			updateEntity.setStatus(Dict_scheduleStatus.REGISTERED);
			updateEntity.setMsg(null);
			schedulerConfigService.update(updateEntity, null);
			
    		if("CRON".equals(runnable.getEntity().getType())) {
    			scheduledFutures.put(task.getId(),threadPoolTaskScheduler.schedule(runnable, new Trigger(){
    	    		@Override            
    	    		public Date nextExecutionTime(TriggerContext triggerContext){
    	    			return new CronTrigger(runnable.getEntity().getScheduled()).nextExecutionTime(triggerContext);            
    	    		}
    	    	}));
    		}else {
    			scheduledFutures.put(task.getId(),threadPoolTaskScheduler.scheduleWithFixedDelay(runnable, Long.valueOf(runnable.getEntity().getScheduled())));
    		}
		}catch(Exception e) {
			logger.error("定时任务（"+task.getId()+"）注册失败");
			logger.error(e.getLocalizedMessage(),e);
			
			updateEntity.setId(task.getId());
			updateEntity.setStatus(Dict_scheduleStatus.FAILED);
			updateEntity.setMsg(e.getLocalizedMessage());
			schedulerConfigService.update(updateEntity, null);
		}
    }
    
    public void delete(String id) {
    	ScheduledFuture<?> scheduledFuture = scheduledFutures.get(id);
    	if(scheduledFuture==null) {
    		return;
    	}
    	
    	if(!scheduledFuture.isCancelled()) {
    		scheduledFuture.cancel(true);
    		scheduledFutures.remove(id);
    	}
    }
    
    /**
     * springboot 服务启动完成后执行
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {
    	logger.warn("服务启动成功，开始加载定时任务");
    	startSchedule();
    }
    
    /**
     * 
     * @Title: getSchedulerRunnable
     * @Description: 获取运行的实例
     * @param @return    设定文件
     * @return SchedulerRunnable    返回类型
     * @throw
     */
    public SchedulerRunnable getSchedulerRunnable(SchedulerConfig task) {
    	try {
    		SchedulerRunnable entity = (SchedulerRunnable)Class.forName(task.getRunnable()).newInstance();
    		entity.setEntity(task);
    		return entity;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
}
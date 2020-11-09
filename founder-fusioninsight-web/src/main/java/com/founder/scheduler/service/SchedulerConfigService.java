package com.founder.scheduler.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.founder.framework.base.dao.WebCommonDao;
import com.founder.framework.base.entity.SessionBean;
import com.founder.framework.base.entity.WebCommonEntity;
import com.founder.framework.base.service.WebCommonService;
import com.founder.scheduler.config.DynamicScheduleTask;
import com.founder.scheduler.dao.SchedulerConfigDao;
import com.founder.scheduler.model.SchedulerConfig;

@Service
@Transactional
public class SchedulerConfigService extends WebCommonService {

    @Autowired
    private SchedulerConfigDao commonDao;
    
    @Autowired
	private DynamicScheduleTask dynamicScheduleTask;

	@Override
	public WebCommonDao getDao() {
		return commonDao;
	}

	@Override
	public void delete(WebCommonEntity entity, SessionBean sessionBean) {
		super.delete(entity, sessionBean);
		dynamicScheduleTask.delete(entity.getId());
	}

	@Override
	public void insert(WebCommonEntity entity, SessionBean sessionBean) {
		super.insert(entity, sessionBean);
		dynamicScheduleTask.addTask((SchedulerConfig)entity);
	}
	
	public void updateAndReload(WebCommonEntity entity, SessionBean sessionBean) {
		super.update(entity, sessionBean);
		dynamicScheduleTask.delete(entity.getId());
		dynamicScheduleTask.addTask((SchedulerConfig)entity);
	}
	
}

package com.founder.scheduler.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.founder.scheduler.dao.SchedulerLogDao;
import com.founder.framework.base.dao.WebCommonDao;
import com.founder.framework.base.service.WebCommonService;

@Service
@Transactional
public class SchedulerLogService extends WebCommonService {

    @Autowired
    private SchedulerLogDao commonDao;

	@Override
	public WebCommonDao getDao() {
		return commonDao;
	}
}

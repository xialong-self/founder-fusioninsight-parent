package com.founder.scheduler.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.founder.framework.base.controller.BaseController;
import com.founder.framework.base.entity.PageEntity;
import com.founder.framework.base.entity.WebCommonEntity;
import com.founder.framework.components.AppConst;
import com.founder.scheduler.dicts.Dict_zxbz;
import com.founder.scheduler.model.SchedulerConfig;
import com.founder.scheduler.model.SchedulerLog;
import com.founder.scheduler.service.SchedulerConfigService;
import com.founder.scheduler.service.SchedulerLogService;

@RestController
@RequestMapping("/schedulerConfig")
public class SchedulerConfigController extends BaseController{
	
	@Autowired
	private SchedulerConfigService schedulerConfigService;
	
	@Autowired
	private SchedulerLogService schedulerLogService;
	
	/**
	 * 
	 * @Title: queryPageList
	 * @Description: 查询分页列表
	 * @param @param page
	 * @param @param rows
	 * @param @param entity
	 * @param @return    设定文件
	 * @return PageEntity    返回类型
	 * @throw
	 */
	@RequestMapping("/queryPageList")
	public PageEntity queryPageList(PageEntity page,Integer rows,SchedulerConfig entity) {
		page.setPagePara(rows);
		entity.setXt_zxbz(Dict_zxbz.YXSJ);
		return schedulerConfigService.queryPageList(page, entity);
	}
	
	@RequestMapping("/insert")
	public Map<String, Object> insert(SchedulerConfig entity) {
		Map<String, Object> model = new HashMap<>();
        try {
        	schedulerConfigService.insert(entity, null);
            model.put(AppConst.STATUS, AppConst.SUCCESS);
            model.put(AppConst.MESSAGES, super.getAddSuccess());
        } catch (Exception e) {
            if(e instanceof RuntimeException) {
            	throw e;
            }
            
            model.put(AppConst.STATUS, AppConst.FAIL);
            model.put(AppConst.MESSAGES, super.getAddFail());
            model.put(AppConst.ERRORS, e.getMessage());
            throw new RuntimeException(JSON.toJSONString(model));
        }
        return model;
	}
	
	@RequestMapping("/update")
	public Map<String, Object> update(SchedulerConfig entity) {
		super.validateEmpty("id", entity.getId());
		
		Map<String, Object> model = new HashMap<>();
        try {
        	schedulerConfigService.updateAndReload(entity, null);
            model.put(AppConst.STATUS, AppConst.SUCCESS);
            model.put(AppConst.MESSAGES, super.getUpdateSuccess());
        } catch (Exception e) {
            if(e instanceof RuntimeException) {
            	throw e;
            }
            
            model.put(AppConst.STATUS, AppConst.FAIL);
            model.put(AppConst.MESSAGES, super.getUpdateFail());
            model.put(AppConst.ERRORS, e.getMessage());
            throw new RuntimeException(JSON.toJSONString(model));
        }
        return model;
	}
	
	@RequestMapping("/delete")
	public Map<String, Object> delete(SchedulerConfig entity) {
		super.validateEmpty("id", entity.getId());
		
		Map<String, Object> model = new HashMap<>();
        try {
        	schedulerConfigService.delete(entity, null);
            model.put(AppConst.STATUS, AppConst.SUCCESS);
            model.put(AppConst.MESSAGES, super.getDeleteSuccess());
        } catch (Exception e) {
            if(e instanceof RuntimeException) {
            	throw e;
            }
            
            model.put(AppConst.STATUS, AppConst.FAIL);
            model.put(AppConst.MESSAGES, super.getDeleteFail());
            model.put(AppConst.ERRORS, e.getMessage());
            throw new RuntimeException(JSON.toJSONString(model));
        }
        return model;
	}
	
	@RequestMapping("/queryByZj")
	public WebCommonEntity queryByZj(String zj) {
		super.validateEmpty("zj", zj);
		
        return schedulerConfigService.queryByZj(zj);
	}
	
	@RequestMapping("/queryLogPageList")
	public PageEntity queryLogPageList(PageEntity page,Integer rows,SchedulerLog entity) {
		page.setPagePara(rows);
		page.setOrder("DESC");
		page.setSort("START_TIME");
		return schedulerLogService.queryPageList(page, entity);
	}
}

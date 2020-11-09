package com.founder.es.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.founder.framework.base.controller.BaseController;
import com.founder.fusioninsight.es.EsRequest;

@RestController
@RequestMapping("/es")
public class EsController extends BaseController{
	
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
	@RequestMapping("/queryEsBySql")
	public String queryEsBySql(String sql) {
		return EsRequest.queryEsBySql(sql);
	}
	
	@RequestMapping("/loadIndex")
	public String loadIndex() {
		return EsRequest.loadIndex();
	}
	
	/**
	 * 
	 * @Title: queryEsByJson
	 * @Description: 通过JSON查询
	 * @param @param index
	 * @param @param json
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	@RequestMapping("/queryEsByJson")
	public String queryEsByJson(String index,String json) {
		return EsRequest.queryEsByJson(index,json);
	}
	
}

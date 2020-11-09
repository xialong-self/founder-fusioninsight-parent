package com.founder.scheduler.model;

import java.io.Serializable;

import com.founder.framework.base.entity.WebCommonEntity;

public class SchedulerLog extends WebCommonEntity implements Serializable {

	private static final long serialVersionUID = 1L;
    
    private String id;//主键ID
    
    private String task_id;//配置表ID
    
    private String start_time;//开始时间
    
    private String end_time;//结束时间
    
    private String result;//执行结果：success,failed
    
    private String msg;//消息
    public String getId(){
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    public String getTask_id(){
        return task_id;
    }

    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }
    public String getStart_time(){
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }
    public String getEnd_time(){
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }
    public String getResult(){
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
    public String getMsg(){
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
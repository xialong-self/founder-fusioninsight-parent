package com.founder.scheduler.model;

import java.io.Serializable;

import com.founder.framework.base.entity.WebCommonEntity;

public class SchedulerConfig extends WebCommonEntity implements Serializable {

	private static final long serialVersionUID = 1L;
    
    private String id;//主键ID
    
    private String type;//定时任务类型：CRON、FIXEDDELAY
    
    private String scheduled;//定时任务配置
    
    private String runnable;//执行的线程class
    
    private String configdata;//配置数据
    
    private String bz;//备注
    
    private String status;//状态：READY，REGISTERED，CANCELLED，FAILED
    
    private String msg;//消息
    public String getId(){
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    public String getType(){
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    public String getScheduled(){
        return scheduled;
    }

    public void setScheduled(String scheduled) {
        this.scheduled = scheduled;
    }
    public String getRunnable(){
        return runnable;
    }

    public void setRunnable(String runnable) {
        this.runnable = runnable;
    }
    public String getConfigdata(){
        return configdata;
    }

    public void setConfigdata(String configdata) {
        this.configdata = configdata;
    }
    public String getBz(){
        return bz;
    }

    public void setBz(String bz) {
        this.bz = bz;
    }
    public String getStatus(){
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    public String getMsg(){
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
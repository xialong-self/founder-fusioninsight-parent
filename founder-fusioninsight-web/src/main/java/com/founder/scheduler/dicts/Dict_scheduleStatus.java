package com.founder.scheduler.dicts;

/**
 * ****************************************************************************
 * @Package:      [com.founder.scheduler.dicts.Dict_scheduleStatus.java]  
 * @ClassName:    [Dict_scheduleStatus]   
 * @Description:  [任务状态子弟啊]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2019年10月18日 下午2:35:51]   
 * @UpdateUser:   [Dagger(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2019年10月18日 下午2:35:51，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class Dict_scheduleStatus {
	public static String READY="READY";//准备中
	
	public static String REGISTERED="REGISTERED";//注册成功，后台等待执行
	
	public static String CANCELLED="CANCELLED";//已取消，停止
	
	public static String FAILED="FAILED";//失败，停止
}

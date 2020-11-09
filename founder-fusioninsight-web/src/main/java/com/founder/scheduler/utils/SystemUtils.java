package com.founder.scheduler.utils;

/**
 * ****************************************************************************
 * @Package:      [com.founder.scheduler.utils.SystemUtils.java]  
 * @ClassName:    [SystemUtils]   
 * @Description:  [系统工具类]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月2日 上午10:51:43]   
 * @UpdateUser:   [42027(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月2日 上午10:51:43，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class SystemUtils {

	/**
	 * 
	 * @Title: isWindows
	 * @Description: 是否是windows系统
	 * @param @return    设定文件
	 * @return boolean    返回类型
	 * @throw
	 */
	public static boolean isWindows() {
		String os = System.getProperty("os.name");  

		if(os.toLowerCase().indexOf("windows") > -1){  
		  return true;
		}
		
		return false;
	}
}

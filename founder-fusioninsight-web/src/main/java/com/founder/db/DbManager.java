package com.founder.db;

import java.sql.Connection;

/**
 * ****************************************************************************
 * @Package:      [com.founder.db.DbManager.java]  
 * @ClassName:    [DbManager]   
 * @Description:  [数据库管理基类]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2019年11月6日 下午4:48:43]   
 * @UpdateUser:   [JWZH(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2019年11月6日 下午4:48:43，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public abstract class DbManager {
	
	/**
	 * 
	 * @Title: getConnection
	 * @Description: 获取刑事案件的数据库连接
	 * @param @return
	 * @param @throws SQLException    设定文件
	 * @return Connection    返回类型
	 * @throw
	 */
	public abstract Connection getConnection();

	public abstract void closeConnection();
}

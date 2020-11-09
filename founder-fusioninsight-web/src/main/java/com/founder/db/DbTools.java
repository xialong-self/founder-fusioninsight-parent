package com.founder.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.founder.db.mysql.MysqlManager;

/**
 * ****************************************************************************
 * @Package:      [com.founder.jdbc.DbTools.java]  
 * @ClassName:    [DbTools]   
 * @Description:  [数据库工具类，jdbc连接]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2018年1月22日 下午2:40:37]   
 * @UpdateUser:   [ZhangHai(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2018年1月22日 下午2:40:37，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class DbTools {
	private Log logger = LogFactory.getLog(this.getClass());
	
	private DbManager dbManager;
	public DbTools(String source_type,String jdbc_url,String jdbc_username,String jdbc_password) {
		if("MYSQL".contentEquals(source_type)) {
			dbManager = new MysqlManager(jdbc_url,jdbc_username,jdbc_password);
		}else {
			throw new RuntimeException("not support source_type");
		}
		
//		if("ORACLE".contentEquals(source_type)) {
//			//return "oracle.jdbc.driver.OracleDriver";
//		}
		
		
		
	}
	
	public Map<String,Object> queryForMap(String sql) {
		Statement smt = null;
		try{
			smt = dbManager.getConnection().createStatement();
			
			logger.info(sql);
			
			ResultSet rs = smt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			Map<String,Object> map = new HashMap<String,Object>();
			int resNum=0;
			while (rs.next()) {
				if(resNum>0) {
					throw new RuntimeException("too manly result");
				}else {
					resNum++;
				}
				for(int i=0;i<columnCount;i++) {
					map.put(rsmd.getColumnName(i), rs.getObject(i));
				}
			}
			
			return map;
		}catch(Exception e){
			throw new RuntimeException(e);
		}finally{
			try{
				if(smt != null)
					smt.close();
			}catch(Exception e){}
		}
	}
	
	public Object queryForObject(String sql) {
		Statement smt = null;
		try{
			smt = dbManager.getConnection().createStatement();
			
			logger.info(sql);
			
			ResultSet rs = smt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			if(columnCount>1) {
				throw new RuntimeException("too manly column");
			}
			
			Object resObj = null;
			int resNum=0;
			while (rs.next()) {
				if(resNum>0) {
					throw new RuntimeException("too manly result");
				}else {
					resNum++;
				}
				resObj = rs.getObject(0);
			}
			
			return resObj;
		}catch(Exception e){
			throw new RuntimeException(e);
		}finally{
			try{
				if(smt != null)
					smt.close();
			}catch(Exception e){}
		}
	}
	
	
	/**
	 * 
	 * @Title: execute
	 * @Description: 执行sql
	 * @param @param conn
	 * @param @param sql    设定文件
	 * @return void    返回类型
	 * @throw
	 */
	public void execute(String sql) {
		Statement smt = null;
		try{
			smt = dbManager.getConnection().createStatement();
			String[] sqls = sql.split(";");
			for(String sqlStr : sqls) {
				if(sqlStr.length()>5) {
					logger.warn("execute sql"+sqlStr);
					smt.execute(sqlStr);
				}
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}finally{
			try{
				if(smt != null)
					smt.close();
			}catch(Exception e){}
		}
	}
	
	public int executeUpdate(String sql,Object[] params){
		PreparedStatement ps = null;
		try {
			ps = dbManager.getConnection().prepareStatement(sql);
			if(params!=null) {
				int parameterIndex = 1;
				for(Object param:params) {
					if(param == null) {
						ps.setNull(parameterIndex, java.sql.Types.NULL);
					}else if(param instanceof byte[]) {
						ps.setBytes(parameterIndex, (byte[])param);
					}else if(param instanceof String) {
						ps.setString(parameterIndex, (String)param);
					}else if(param instanceof Integer) {
						ps.setInt(parameterIndex, (int)param);
					}else if(param instanceof Date) {
						ps.setTimestamp(parameterIndex, new java.sql.Timestamp(((Date)param).getTime()));
					}else {
						throw new RuntimeException("不支持的数据库类型:"+param.getClass().getSimpleName());
					}
					
					parameterIndex++;
				}
			}
			
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}finally{
			try{
				if(ps != null)
					ps.close();
			}catch(Exception se){}
		}
	}
	
//	public void closeConnection(){
//		dbManager.closeConnection();
//	}
	
	public static String createZj()
	{
		String uuid=java.util.UUID.randomUUID().toString();
		StringBuilder bf=new StringBuilder(32);
		for(int i=0;i<uuid.length();++i){
			char c=uuid.charAt(i);
			if(c!='-'&&c!='_'){
				bf.append(c);
			}
		}
		return bf.toString();
	}
}

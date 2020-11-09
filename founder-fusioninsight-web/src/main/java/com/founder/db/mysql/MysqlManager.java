package com.founder.db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;

import com.founder.db.DbManager;

public class MysqlManager extends DbManager{
	public  String jdbc_url = "jdbc:mysql://80.2.21.211:32001/big_data?characterEncoding=utf8";
	public  String jdbc_username = "root";
	public  String jdbc_password = "jwzh#123";
	
	private  Connection conn = null;
	
	public MysqlManager(String jdbc_url,String jdbc_username,String jdbc_password) {
		this.jdbc_url = jdbc_url;
		this.jdbc_username = jdbc_username;
		this.jdbc_password = jdbc_password;
	}
	
	
	/**
	 * 
	 * @Title: getConnection
	 * @Description: 获取刑事案件的数据库连接
	 * @param @return
	 * @param @throws SQLException    设定文件
	 * @return Connection    返回类型
	 * @throw
	 */
	public Connection getConnection(){
		try{
			if(conn==null || conn.isClosed()){
				Class.forName("com.mysql.cj.jdbc.Driver");
				//2.获得数据库的连接
				conn = DriverManager.getConnection(jdbc_url, jdbc_username, jdbc_password);
				conn.setAutoCommit(true);
			}
			return conn;
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void closeConnection() {
		try{
			if(conn!=null && !conn.isClosed()){
				conn.close();
			}			
		}catch(Exception e){
			
		}
	}
}

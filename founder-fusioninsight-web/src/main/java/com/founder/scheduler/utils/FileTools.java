package com.founder.scheduler.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.util.ResourceUtils;

/**
 * ****************************************************************************
 * @Package:      [com.founder.ajdy.utils.FileTools.java]  
 * @ClassName:    [FileTools]   
 * @Description:  [文件相关的工具类]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2018年12月20日 下午5:03:32]   
 * @UpdateUser:   [42027(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2018年12月20日 下午5:03:32，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class FileTools {
	public static String getClasspath() {
		try {
			String classpath = ResourceUtils.getURL("classpath:").getPath();
			if(classpath.startsWith("/")) {
				classpath = classpath.substring(1);
			}
			return classpath;
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String getServerpath() {
		return System.getProperty("user.dir");
	}
	
	/**
	 * 
	 * @Title: getContent
	 * @Description: 读取文件内容
	 * @param @param file
	 * @param @return
	 * @param @throws IOException    设定文件
	 * @return byte[]    返回类型
	 * @throw
	 */
	public static byte[] getContent(File file){
		FileInputStream in = null;
		try {
			in = new FileInputStream(file);
			int byteread = 0;
			byte[] buffer = new byte[1024];
			byte[] content = new byte[0];
			while((byteread = in.read(buffer)) != -1) {
				byte[] content2 = new byte[content.length+byteread];
				System.arraycopy(content,0,content2,0,content.length);
				System.arraycopy(buffer,0,content2,content.length,byteread);
				content = content2;
			}
			
			return content;
		}catch(IOException e) {
			throw new RuntimeException(e);
		}finally {
			try {
				in.close();
			}catch(Exception ee) {}
		}
	}
	public static byte[] getContent(InputStream is){
		try {
			int byteread = 0;
			byte[] buffer = new byte[1024];
			byte[] content = new byte[0];
			while((byteread = is.read(buffer)) != -1) {
				byte[] content2 = new byte[content.length+byteread];
				System.arraycopy(content,0,content2,0,content.length);
				System.arraycopy(buffer,0,content2,content.length,byteread);
				content = content2;
			}
			is.close();
			return content;
		}catch(IOException e) {
			throw new RuntimeException(e);
		}finally {
			try {
				is.close();
			}catch(Exception ee) {}
		}
	}
	
	public static void writeFile(File file,byte[] content) {
		OutputStream out = null;
		try {
			out = new FileOutputStream(file);
			out.write(content);
		}catch(IOException e) {
			throw new RuntimeException(e);
		}finally {
			try {
				out.close();
			}catch(Exception ee) {}
		}
	}
	
	public static void main(String args[]) {
		writeFile(new File("E:\\test.txt"),"test".getBytes());
	}
}

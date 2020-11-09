package com.founder.scheduler.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.StringUtils;

/**
 * ****************************************************************************
 * @Package:      [com.founder.scheduler.utils.RuntimeUtils.java]  
 * @ClassName:    [RuntimeUtils]   
 * @Description:  [系统命令执行工具]   
 * @Author:       [zhang.hai@founder.com.cn]  
 * @CreateDate:   [2020年7月9日 上午10:01:04]   
 * @UpdateUser:   [Founder(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateDate:   [2020年7月9日 上午10:01:04，(如多次修改保留历史记录，增加修改记录)]   
 * @UpdateRemark: [说明本次修改内容,(如多次修改保留历史记录，增加修改记录)]  
 * @Version:      [v1.0]
 */
public class RuntimeUtils {
	private static Logger logger = LoggerFactory.getLogger(RuntimeUtils.class);
	
	/**
	 * 
	 * @Title: exec
	 * @Description: 执行shell或者cmd命令
	 * @param @param workDir 工作目录
	 * @param @param cmdList 命令列表
	 * @param @param inputStr 命令需要输入的字符串
	 * @param @param charsetName 返回字符的编码
	 * @param @param logPath 日志文件路径
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String exec(String workDir,List<String> cmdList,String inputStr,String charsetName,String logPath) {
		try {
			File dir = null;
			if(!StringUtils.isEmpty(workDir)) {
				dir = new File(workDir);
			}
			
			Process pro = Runtime.getRuntime().exec(cmdList.toArray(new String[cmdList.size()]),null,dir);
			String res = "run success,log in dir "+logPath;
			
			StringBuffer inputSB = new StringBuffer();
			StringBuffer errorSB = new StringBuffer();
			
			if(!StringUtils.isEmpty(logPath)) {//日志记录到文件中
				printMessage(pro.getInputStream(),"catalina",charsetName,logPath);
				printMessage(pro.getErrorStream(),"error",charsetName,logPath);
			}else {
				printMessage(pro.getInputStream(),"InputStream:",charsetName,inputSB);
				printMessage(pro.getErrorStream(),"ErrorStream:",charsetName,errorSB);
			}
			
			
			if(!StringUtils.isEmpty(inputStr)) {//有需要输入到命令行中的文字
				OutputStream outputStream = pro.getOutputStream();
				try {
					outputStream.write(inputStr.getBytes());
				} catch (IOException e) {
					logger.error(e.getMessage(),e);
				}finally {
					try {
						outputStream.close();
					} catch (IOException e) {}
				}
			}
			
			pro.waitFor();
			//Thread.sleep(1000);//睡1秒是为了流能够读取完毕
			if(StringUtils.isEmpty(logPath)) {//日志记录到文件中
				res = "ErrorStream:"+errorSB.append(" InputStream:").append(inputSB).toString();
			}
			logger.warn(res);
			return res;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 
	 * @Title: printMessage
	 * @Description: 日志写入文件
	 * @param @param is
	 * @param @param logName
	 * @param @param charsetName
	 * @param @param logPath    设定文件
	 * @return void    返回类型
	 * @throw
	 */
	public static void printMessage(final InputStream is,String logName,String charsetName,String logPath) {
		new Thread(new Runnable() {
			public void run() {
				String catalina = logPath+logName+System.currentTimeMillis()+".log";
				
				OutputStream out = null;
				try {
					out = new FileOutputStream(new File(catalina));
					int byteread = 0;
					byte[] buffer = new byte[1024];
					while((byteread = is.read(buffer)) != -1) {
						byte[] content = new byte[byteread];
						System.arraycopy(buffer,0,content,0,byteread);
						out.write(content);
					}

				} catch (Exception e) {
	        		  e.printStackTrace();
				}finally {
					try {
						is.close();
						out.close();
					} catch (IOException e) {}
				}
				
				System.out.println("red end");
			}
		}).start();
	}
	
	public static void printMessage(final InputStream input,String type,String charsetName,StringBuffer sb) {
		new Thread(new Runnable() {
			public void run() {
				Reader reader = null;
				try {
					reader = new InputStreamReader(input,charsetName);
					BufferedReader bf = new BufferedReader(reader);
					String line = null;
					
					while((line=bf.readLine())!=null) {
						sb.append(line).append("\r\n");
						System.out.println(type+line);
					}
				} catch (Exception e) {
	        		  e.printStackTrace();
				}finally {
					try {
						reader.close();
					} catch (IOException e) {}
				}
				
				System.out.println("red end");
			}
		}).start();
	}
	
}

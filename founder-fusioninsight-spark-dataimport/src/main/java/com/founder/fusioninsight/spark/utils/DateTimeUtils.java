package com.founder.fusioninsight.spark.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.lang.StringUtils;

/**
 * @类名: DateTimeUtils 
 * @描述: 时间DateTime公共类(lib包里DateTimeHelper类，不满足需求)
 * @作者: zhang_guoliang@founder.com 
 * @日期: 2015-5-13 下午4:33:11 
 */
public class DateTimeUtils {
	public static String YYYY_MM_DD="yyyy-MM-dd";
	public static String YYYY_MM_DD_HH_MM_SS="yyyy-MM-dd HH:mm:ss";
	
	//获得去年第一天时间
	public static Date getQnYearmorning(){
		Calendar cal = Calendar.getInstance();
		int currenYear = cal.get(Calendar.YEAR)-1;
		return getQnCurrYearFirst(currenYear);
	}

	public static Date getQnCurrYearFirst(int year){
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat longSdf = new SimpleDateFormat("");
	  	SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");  
	  	Date now =null;
		cal.clear();
		cal.set(Calendar.YEAR, year); 
		try{  
	      now = longSdf.parse(shortSdf.format(cal.getTime()) + " 00:00:00");
	      } catch (Exception e) {
	        e.printStackTrace();
	      }
	    return now;   
	}
	//获得去年最后一天时间
	public static Date getQnYearnight(){
		Calendar cal = Calendar.getInstance();
		int currenYear = cal.get(Calendar.YEAR)-1;
		return getQnCurrYearLast(currenYear);
	}

	public static Date getQnCurrYearLast(int year){
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	  	SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");  
	  	Date now =null;
		cal.clear();
		cal.set(Calendar.YEAR, year);
		cal.roll(Calendar.DAY_OF_YEAR, -1);
		try{  
	      now = longSdf.parse(shortSdf.format(cal.getTime()) + " 23:59:59");
	      } catch (Exception e) {
	        e.printStackTrace();
	      }
	    return now;   
	}
	
	//获取当月第一天  
    public static Date getFirstDayOfMonth(){    
       Calendar lastDate = Calendar.getInstance();  
       SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  	   SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");  
  	   Date now =null;
       lastDate.set(Calendar.DATE,1);//设为当前月的1号  
       try{  
  	      now = longSdf.parse(shortSdf.format(lastDate.getTime()) + " 00:00:00");
  	      } catch (Exception e) {
  	        e.printStackTrace();
  	      }
       return now;      
    }  
    // 计算当前月最后一天时间  
    public static Date getDefaultDay(){    
       Calendar lastDate = Calendar.getInstance();  
       SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd"); 
	   Date now =null;
       lastDate.set(Calendar.DATE,1);//设为当前月的1号  
       lastDate.add(Calendar.MONTH,1);//加一个月，变为下月的1号  
       lastDate.add(Calendar.DATE,-1);//减去一天，变为当月最后一天  
       try{  
 	      now = longSdf.parse(shortSdf.format(lastDate.getTime()) + " 23:59:59");
 	      } catch (Exception e) {
 	        e.printStackTrace();
 	      }
       return now;      
    }  
    
    
  //获取去年当月第一天  
    public static Date getQnFirstDayOfMonth(){    
       Calendar lastDate = Calendar.getInstance();  
       SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  	   SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");  
  	   Date now =null;
       lastDate.set(Calendar.DATE,1);//设为当前月的1号  
       lastDate.set(Calendar.YEAR, lastDate.get(Calendar.YEAR)-1);
       try{  
  	      now = longSdf.parse(shortSdf.format(lastDate.getTime()) + " 00:00:00");
  	      } catch (Exception e) {
  	        e.printStackTrace();
  	      }
       return now;      
    }  
    // 计算去年当前月最后一天时间  
    public static Date getQnDefaultDay(){    
       Calendar lastDate = Calendar.getInstance();  
       SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd"); 
	   Date now =null;
       lastDate.set(Calendar.DATE,1);//设为当前月的1号  
       lastDate.add(Calendar.MONTH,1);//加一个月，变为下月的1号  
       lastDate.add(Calendar.DATE,-1);//减去一天，变为当月最后一天  
       lastDate.set(Calendar.YEAR, lastDate.get(Calendar.YEAR)-1);
       try{  
 	      now = longSdf.parse(shortSdf.format(lastDate.getTime()) + " 23:59:59");
 	      } catch (Exception e) {
 	        e.printStackTrace();
 	      }
       return now;      
    }  
	
	 //获得上月第一天的日期  
	 public static Date getPreviousMonthFirst(){    
		  Calendar cal = Calendar.getInstance();
	      SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		  SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd"); 
		  Date now =null;
		  cal.set(Calendar.DATE,1);//设为当前月的1号  
		  cal.add(Calendar.MONTH,-1);//减一个月，变为下月的1号  
	       //lastDate.add(Calendar.DATE,-1);//减去一天，变为当月最后一天  
	      try{  
	      now = longSdf.parse(shortSdf.format(cal.getTime()) + " 00:00:00");
	      } catch (Exception e) {
	        e.printStackTrace();
	      }
	      return now;    
	  }
	
	 //获得上月最后一天的日期  
     public static Date getPreviousMonthEnd(){  
		  Calendar cal = Calendar.getInstance();
	      SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		  SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd"); 
	      Date now =null;
	      cal.add(Calendar.MONTH,-1);//减一个月  
	      cal.set(Calendar.DATE, 1);//把日期设置为当月第一天   
	      cal.roll(Calendar.DATE, -1);//日期回滚一天，也就是本月最后一天   
	      try{
	      now = longSdf.parse(shortSdf.format(cal.getTime()) + " 23:59:59");
	      } catch (Exception e) {
	          e.printStackTrace();
	      }
	      return now;
     } 
	/**
     * @Title: getStAft 
     * @描述: 获得三天时间
     * @作者: zhang_guoliang@founder.com 
     * @参数: 传入参数定义 
     * @日期： 2015-12-11 下午4:50:32 
     * @返回值: Date    返回类型 
     * @throws
     */
	public static Date getStAft(){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, -72);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}
    /**
     * @Title: getTimesmorning 
     * @描述: 获得当天0点时间
     * @作者: zhang_guoliang@founder.com 
     * @参数: 传入参数定义 
     * @日期： 2015-5-13 下午5:04:03 
     * @返回值: Date    返回类型 
     * @throws
     */
	public static Date getTimesmorning(){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}
	/**
	 * @Title: getTimesnight 
	 * @描述: 获得当天24点时间
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-13 下午6:10:50 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesnight(){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 24);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}
	/**
	 * @Title: getTimesWeekmorning 
	 * @描述: 获得本周一0点时间
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 上午8:58:22 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesWeekmorning(){
		Calendar cal = Calendar.getInstance();
		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONDAY),cal.get(Calendar.DAY_OF_MONTH),0,0,0);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		return cal.getTime();
	}
	/**
	 * @Title: getTimesWeeknight 
	 * @描述: 获得本周日24点时间
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 上午9:21:30 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesWeeknight(){
		Calendar cal = Calendar.getInstance();
		cal.setTime(getTimesWeekmorning());
		cal.add(Calendar.DAY_OF_WEEK, 7);
		return cal.getTime();
	}
	/**
	 * @Title: getTimesMonthmorning 
	 * @描述: 获得本月第一天0点时间 
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 上午9:23:59 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesMonthmorning(){
		Calendar cal = Calendar.getInstance();
		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONDAY),cal.get(Calendar.DAY_OF_MONTH),0,0,0);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
		return cal.getTime();
	}
	/**
	 * @Title: getTimesMonthnight 
	 * @描述: 获得本月最后一天24点时间 
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 上午10:14:03 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesMonthnight(){
		Calendar cal = Calendar.getInstance();
		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONDAY),cal.get(Calendar.DAY_OF_MONTH),0,0,0);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
		cal.set(Calendar.HOUR_OF_DAY, 24);
		return cal.getTime();
	}
	/**
	 * @Title: getTimesQuartermorning 
	 * @描述: 获得季度初
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesQuartermorning(){
		Calendar cal = Calendar.getInstance();
		int month = cal.get(Calendar.MONTH);
		if(month>=Calendar.JANUARY && month<=Calendar.MARCH){
			cal.set(Calendar.MONTH, Calendar.JANUARY);
		}
		if(month>=Calendar.APRIL && month<=Calendar.JUNE){
			cal.set(Calendar.MONTH, Calendar.APRIL);
		}
		if(month>=Calendar.JUNE && month<=Calendar.AUGUST){
			cal.set(Calendar.MONTH, Calendar.JULY);
		}
		if(month>=Calendar.OCTOBER && month<=Calendar.DECEMBER){
			cal.set(Calendar.MONTH, Calendar.OCTOBER);
		}
		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONDAY),cal.get(Calendar.DAY_OF_MONTH),0,0,0);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
		return cal.getTime();
	}
	/**
	 * @Title: getSjStartTime 
	 * @描述: 获得上一季度初
	 * @作者: liang_lihe@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-12-22 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static  Date getSjStartTime() {
		SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        int month = c.get(Calendar.MONTH);
        Date now = null;
        if(month>=0&&month<=2){
        	c.set(Calendar.MONTH, 9);
        	c.set(Calendar.DATE, 1);
            c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
            try {
            now = longSdf.parse(shortSdf.format(c.getTime()) + " 00:00:00");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
        int currentMonth = c.get(Calendar.MONTH)-2;
        try {
            if (currentMonth >= 1 && currentMonth <= 3)
                c.set(Calendar.MONTH, 0);
            else if (currentMonth >= 4 && currentMonth <= 6)
                c.set(Calendar.MONTH, 3);
            else if (currentMonth >= 7 && currentMonth <= 9)
                c.set(Calendar.MONTH, 5);
            else if (currentMonth >= 10 && currentMonth <= 12)
                c.set(Calendar.MONTH, 9);
            c.set(Calendar.DATE, 1);
            now = longSdf.parse(shortSdf.format(c.getTime()) + " 00:00:00");
        } catch (Exception e) {
            e.printStackTrace();
        }
        }
        return now;
    }
	
	/**
	 * @Title: getSjEndTime 
	 * @描述: 获得上一季度末
	 * @作者: liang_lihe@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-12-22 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	  public static  Date getSjEndTime() {
	        Calendar c = Calendar.getInstance();
	        SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
			int month = c.get(Calendar.MONTH);
	        Date now = null;
	        if(month>=0&&month<=2){
	        	c.set(Calendar.MONTH, 11);
                c.set(Calendar.DATE, 31);
	            c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
	            try {
	            now = longSdf.parse(shortSdf.format(c.getTime()) + " 00:00:00");
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }else{
	        int currentMonth = c.get(Calendar.MONTH) -2;
	        try {
	            if (currentMonth >= 1 && currentMonth <= 3) {
	                c.set(Calendar.MONTH, 2);
	                c.set(Calendar.DATE, 31);
	            } else if (currentMonth >= 4 && currentMonth <= 6) {
	                c.set(Calendar.MONTH, 5);
	                c.set(Calendar.DATE, 30);
	            } else if (currentMonth >= 7 && currentMonth <= 9) {
	                c.set(Calendar.MONTH, 8);
	                c.set(Calendar.DATE, 30);
	            } else if (currentMonth >= 10 && currentMonth <= 12) {
	                c.set(Calendar.MONTH, 11);
	                c.set(Calendar.DATE, 31);
	            }
	            now = longSdf.parse(shortSdf.format(c.getTime()) + " 23:59:59");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        }
	        return now;
	    }
	  
	/**
	 * @Title: getBjStartTime 
	 * @描述: 获得本季度初
	 * @作者: liang_lihe@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-12-22 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	  public static  Date getBjStartTime() {
			SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
	        Calendar c = Calendar.getInstance();
	        int currentMonth = c.get(Calendar.MONTH)+1;
	        Date now = null;
	        try {
	            if (currentMonth >= 1 && currentMonth <= 3)
	                c.set(Calendar.MONTH, 0);
	            else if (currentMonth >= 4 && currentMonth <= 6)
	                c.set(Calendar.MONTH, 3);
	            else if (currentMonth >= 7 && currentMonth <= 9)
	                c.set(Calendar.MONTH, 6);
	            else if (currentMonth >= 10 && currentMonth <= 12)
	                c.set(Calendar.MONTH, 9);
	            c.set(Calendar.DATE, 1);
	            now = longSdf.parse(shortSdf.format(c.getTime()) + " 00:00:00");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return now;
	    }
	  
	 /**
	 * @Title: getBjEndTime 
	 * @描述: 获得本季度末
	 * @作者: liang_lihe@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-12-22 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	  public static  Date getBjEndTime() {
	        Calendar c = Calendar.getInstance();
	        SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
	        int currentMonth = c.get(Calendar.MONTH) +1;
	        Date now = null;
	        try {
	            if (currentMonth >= 1 && currentMonth <= 3) {
	                c.set(Calendar.MONTH, 2);
	                c.set(Calendar.DATE, 31);
	            } else if (currentMonth >= 4 && currentMonth <= 6) {
	                c.set(Calendar.MONTH, 5);
	                c.set(Calendar.DATE, 30);
	            } else if (currentMonth >= 7 && currentMonth <= 9) {
	                c.set(Calendar.MONTH, 8);
	                c.set(Calendar.DATE, 30);
	            } else if (currentMonth >= 10 && currentMonth <= 12) {
	                c.set(Calendar.MONTH, 11);
	                c.set(Calendar.DATE, 31);
	            }
	            now = longSdf.parse(shortSdf.format(c.getTime()) + " 23:59:59");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return now;
	    }
	 /**
	 * @Title: getQnDqJdStartTime 
	 * @描述: 获得去年当前季度初
	 * @作者: liang_lihe@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-12-22 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */  
	  public static  Date getQnDqJdStartTime() {
			SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
	        Calendar c = Calendar.getInstance();
	        int currentMonth = c.get(Calendar.MONTH)+1;
	        Date now = null;
	        try {
	            if (currentMonth >= 1 && currentMonth <= 3)
	                c.set(Calendar.MONTH, 0);
	            else if (currentMonth >= 4 && currentMonth <= 6)
	                c.set(Calendar.MONTH, 3);
	            else if (currentMonth >= 7 && currentMonth <= 9)
	                c.set(Calendar.MONTH, 6);
	            else if (currentMonth >= 10 && currentMonth <= 12)
	                c.set(Calendar.MONTH, 9);
	            c.set(Calendar.DATE, 1);
	            c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
	            now = longSdf.parse(shortSdf.format(c.getTime()) + " 00:00:00");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return now;
	    }
    /**
	 * @Title: getQnDqJdEndTime 
	 * @描述: 获得去年当前季度末
	 * @作者: liang_lihe@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-12-22 上午10:19:14 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	  public static  Date getQnDqJdEndTime() {
	        Calendar c = Calendar.getInstance();
	        SimpleDateFormat longSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat shortSdf = new SimpleDateFormat("yyyy-MM-dd");
	        int currentMonth = c.get(Calendar.MONTH) +1;
	        Date now = null;
	        try {
	            if (currentMonth >= 1 && currentMonth <= 3) {
	                c.set(Calendar.MONTH, 2);
	                c.set(Calendar.DATE, 31);
	            } else if (currentMonth >= 4 && currentMonth <= 6) {
	                c.set(Calendar.MONTH, 5);
	                c.set(Calendar.DATE, 30);
	            } else if (currentMonth >= 7 && currentMonth <= 9) {
	                c.set(Calendar.MONTH, 8);
	                c.set(Calendar.DATE, 30);
	            } else if (currentMonth >= 10 && currentMonth <= 12) {
	                c.set(Calendar.MONTH, 11);
	                c.set(Calendar.DATE, 31);
	            }
	            c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
	            now = longSdf.parse(shortSdf.format(c.getTime()) + " 23:59:59");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return now;
	    }
	
	/**
	 * @Title: getTimesQuarternight 
	 * @描述: 获得季度末
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 上午10:47:41 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesQuarternight(){
		Calendar cal = Calendar.getInstance();
		int month = cal.get(Calendar.MONTH);
		if(month>=Calendar.JANUARY && month<=Calendar.MARCH){
			cal.set(Calendar.MONTH, Calendar.MARCH);
		}
		if(month>=Calendar.APRIL && month<=Calendar.JUNE){
			cal.set(Calendar.MONTH, Calendar.JUNE);
		}
		if(month>=Calendar.JUNE && month<=Calendar.AUGUST){
			cal.set(Calendar.MONTH, Calendar.AUGUST);
		}
		if(month>=Calendar.OCTOBER && month<=Calendar.DECEMBER){
			cal.set(Calendar.MONTH, Calendar.DECEMBER);
		}
		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONDAY),cal.get(Calendar.DAY_OF_MONTH),0,0,0);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
		return cal.getTime();
	}
	/**
	 * @Title: getTimesYearmorning 
	 * @描述: 获得本年第一天
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 下午2:19:30 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesYearmorning(){
		Calendar cal = Calendar.getInstance();
		int currenYear = cal.get(Calendar.YEAR);
		return getCurrYearFirst(currenYear);
	}
	/**
	 * @Title: getCurrYearFirst 
	 * @描述: 计算本年第一天
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 下午2:28:53 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getCurrYearFirst(int year){
		Calendar cal = Calendar.getInstance();
		cal.clear();
		cal.set(Calendar.YEAR, year); 
		return cal.getTime();
	}
	/**
	 * @Title: getTimesYearnight 
	 * @描述: 获得本年最后一天
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 下午2:19:09 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getTimesYearnight(){
		Calendar cal = Calendar.getInstance();
		int currenYear = cal.get(Calendar.YEAR);
		return getCurrYearLast(currenYear);
	}
	/**
	 * @Title: getCurrYearLast 
	 * @描述: 计算本年最后一天
	 * @作者: zhang_guoliang@founder.com 
	 * @参数: 传入参数定义 
	 * @日期： 2015-5-14 下午2:28:27 
	 * @返回值: Date    返回类型 
	 * @throws
	 */
	public static Date getCurrYearLast(int year){
		Calendar cal = Calendar.getInstance();
		cal.clear();
		cal.set(Calendar.YEAR, year);
		cal.roll(Calendar.DAY_OF_YEAR, -1);
		return cal.getTime();
	}
	
	/**
	 * 
	 * @Title: addSecond
	 * @Description: 将某个日期延后amount秒
	 * @param @param date
	 * @param @param amount
	 * @param @return    设定文件
	 * @return Date    返回类型
	 * @throw
	 */
	public static Date addSecond(Date date,int amount){
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(date);
		gc.add(Calendar.SECOND, amount);
		return gc.getTime();
	}
	
	/**
	 * 
	 * @Title: addHour24
	 * @Description: 将某个日期延后amount小时（24小时制）
	 * @param @param date
	 * @param @param amount
	 * @param @return    设定文件
	 * @return Date    返回类型
	 * @throw
	 */
	public static Date addHour24(Date date,int amount){
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(date);
		gc.add(Calendar.HOUR_OF_DAY, amount);
		return gc.getTime();
	}
	
	/**
	 * 
	 * @Title: addDays
	 * @Description: TODO(将某个日期延后amount天)
	 * @param @param date 需要延后的日期
	 * @param @param iDays 延后的天数
	 * @param @return    设定文件
	 * @return Date    返回类型
	 * @throw
	 */
	public static Date addDays(Date date,int amount){
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(date);
		gc.add(Calendar.DAY_OF_YEAR, amount);
		return gc.getTime();
	}
	
	/**
	 * 
	 * @Title: addMonths
	 * @Description: TODO(将某个日期延后amount月)
	 * @param @param date
	 * @param @param amount
	 * @param @return    设定文件
	 * @return Date    返回类型
	 * @throw
	 */
	public static Date addMonths(Date date,int amount){
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(date);
		gc.add(Calendar.MONTH, amount);
		return gc.getTime();
	}
	
	/**
	 * 
	 * @Title: StringToDate
	 * @Description: TODO(将日期格式的字符串转成日期)
	 * @param @param dateStr 日期格式的字符串
	 * @param @param dateModel 日期格式
	 * @param @return
	 * @param @throws ParseException    设定文件
	 * @return Date    返回类型
	 * @throw
	 */
	public static Date StringToDate(String dateStr,String dateModel){
		if(StringUtils.isEmpty(dateModel) || StringUtils.isEmpty(dateStr))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat(dateModel);
		try {
			return sdf.parse(dateStr);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 
	 * @Title: DateToString
	 * @Description: TODO(将日期转成字符串)
	 * @param @param date 日期
	 * @param @param dateModel 日期格式
	 * @param @return
	 * @param @throws ParseException    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String DateToString(Date date,String dateModel) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateModel);
		return sdf.format(date);
	}
	
	/**
	 * 
	 * @Title: daysBetween
	 * @Description: TODO(计算两个日期相差的天数)
	 * @param @param smdate 小的日期
	 * @param @param bdate 大的日期
	 * @param @return
	 * @param @throws ParseException    设定文件
	 * @return int    返回类型
	 * @throw
	 */
	public static int daysBetween(Date smdate,Date bdate) throws ParseException{    
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  
		smdate=sdf.parse(sdf.format(smdate));  
		bdate=sdf.parse(sdf.format(bdate));  
		Calendar cal = Calendar.getInstance();    
		cal.setTime(smdate);    
		long time1 = cal.getTimeInMillis();                 
		cal.setTime(bdate);    
		long time2 = cal.getTimeInMillis();         
		long between_days=(time2-time1)/(1000*3600*24);  
		return Integer.parseInt(String.valueOf(between_days));           
	} 

	/**
	 * 
	 * @Title: getSystemDateTimeString
	 * @Description: 取得系统当前的日期时间字符表达式,格式如: (2008-05-12 14:28:36)
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throw
	 */
	public static String getSystemDateTimeString() {
		return DateToString(new Date(),YYYY_MM_DD_HH_MM_SS);
	}
	
	public static String getSystemDateString() {
		return DateToString(new Date(),YYYY_MM_DD);
	}
}


//package com.founder.scheduler.utils;
//
//import java.lang.reflect.Field;
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import com.alibaba.fastjson.JSON;
//import com.founder.ajdy.vo.ZjckBaseInfo;
//import com.founder.ajdy.vo.ZjckCaseInfo;
//import com.founder.ajdy.vo.ZjckCkAccounts;
//import com.founder.ajdy.vo.ZjckCkObjs;
//import com.founder.ajdy.vo.ZjckSjb;
//import com.founder.framework.utils.DateTimeUtils;
//
//public class ObjectUtil {
//	public static Map<String,Object> objtoUpperCaseMap(Object obj) {
//		Field[] fields = obj.getClass().getDeclaredFields();
//		Map<String,Object> map = new HashMap<String,Object>();
//		String fieldName;
//		Object fieldValue;
//		for(Field field:fields) {
//			fieldName = field.getName();
//			if(field.getType().isArray()) {
//				Object[] ary = (Object[])getFieldValue(obj,field);
//				if(ary != null && ary.length > 0) {
//					List<Map<String,Object>> valList = new ArrayList<Map<String,Object>>(ary.length);
//					for(int i = 0;i<ary.length;i++) {
//						valList.add(objtoUpperCaseMap(ary[i]));
//					}
//					fieldValue = valList;
//				}else {
//					fieldValue = new ArrayList<Map<String,Object>>();
//				}
//				
//			}else  if(field.getType() == String.class){
//				fieldValue = getFieldValue(obj,field);
//			}else {
//				fieldValue = objtoUpperCaseMap(getFieldValue(obj,field));
//			}
//			
//			map.put(fieldName.toUpperCase(), fieldValue);
//		}
//		return map;
//	}
//	
//	public static Object getFieldValue(Object obj,Field field) {
//		try {
//			return getGetMethod(obj.getClass(),field).invoke(obj);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//	
//	public static String getGetMethodName(String fieldName) {
//		return "get"+fieldName.substring(0, 1).toUpperCase()+fieldName.substring(1);
//	}
//	
//	public static Method getGetMethod(Class<?> cs,Field field) {
//		try {
//			return cs.getDeclaredMethod(getGetMethodName(field.getName()));
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//	
//	public static void main(String[] args) {
//		ZjckBaseInfo base_info = new ZjckBaseInfo();
//		base_info.setQqdbs("");
//		base_info.setJjcd("01");
//		base_info.setMbjgdm("P100H101150104001");
//		base_info.setCslxdm("01");
//		base_info.setFssj(DateTimeUtils.DateToString(new Date(), DateTimeUtils.YYYY_MM_DD_HH_MM_SS));
//		//zjckBaseInfo.setXcrjh("37088538");
//		//zjckBaseInfo.setXcrxm("李四");
//		
//		ZjckCaseInfo case_info = new ZjckCaseInfo();
//		case_info.setAjbh("A2305020819442016061315");
//		case_info.setAjmc("XXX被诈骗案（尖山网安20170216-2）");
//		case_info.setAjzt("立  案");
//		case_info.setAjlx("219");
//		case_info.setLasj("20150914");
//		case_info.setJyaq("2015年2月，XXXXXXXXXXXX");
//		
//		ZjckCkObjs ck_objs = new ZjckCkObjs();
//		ck_objs.setRwlsh("");
//		ck_objs.setZzlx("01");
//		ck_objs.setCkztlb("22");
//		ck_objs.setZzhm("35622256555555555");
//		ck_objs.setZtmc("主体名称");
//		ck_objs.setCxnr("01");
//		ck_objs.setMxsdlx("01");
//		ck_objs.setMxqssj("20180817");
//		ck_objs.setMxjzsj("20180819");
//		ZjckCkObjs[] ck_objs_ary = new ZjckCkObjs[1];
//		ck_objs_ary[0]= ck_objs;
//		
//		ZjckCkAccounts ck_accounts = new ZjckCkAccounts();
//		ck_accounts.setRwlsh("");
//		ck_accounts.setCxzh("621234567891221111");
//		ck_accounts.setCkztlb("01");
//		ck_accounts.setCxnr("01");
//		ck_accounts.setMxsdlx("01");
//		ck_accounts.setMxqssj("20180801");
//		ck_accounts.setMxjzsj("20180831");
//		ZjckCkAccounts[] ck_accounts_ary = new ZjckCkAccounts[1];
//		ck_accounts_ary[0]=ck_accounts;
//		
//		ZjckSjb zjckSjb = new ZjckSjb();
//		zjckSjb.setBase_info(base_info);
//		zjckSjb.setCase_info(case_info);
//		zjckSjb.setCk_objs(ck_objs_ary);
//		zjckSjb.setCk_accounts(ck_accounts_ary);
//		
//		String  classPath = FileTools.getClasspath();
//		String basePath = classPath+"testfiles/";
//		
//		zjckSjb.setWspdfpath(basePath+"wspdf.pdf");
//		zjckSjb.setCxr_jgz_path(basePath+"cxr_jgz.jpg");
//		zjckSjb.setXcr_jgz_path(basePath+"xcr_jgz.jpg");
//		Map<String,Object> map = objtoUpperCaseMap(zjckSjb);
//		System.out.println(JSON.toJSONString(map));
//	}
//}

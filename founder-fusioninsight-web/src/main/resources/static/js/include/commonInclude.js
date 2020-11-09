/*
 * 公共的引入的js方法
 * 
 */

/**
 * 日期格式化
 * @param date 日期
 * @param filter 格式,例如（yyyy-mm-dd hh:mi:ss）
 * @returns
 */
function getDateStr(date,filter){
	var yyyy=date.getFullYear();
	var mm=(date.getMonth()+1) >= 10 ? date.getMonth()+1 : '0'+(date.getMonth()+1);
	var dd=date.getDate() >= 10 ? date.getDate() : '0'+date.getDate();
	
	var hh=date.getHours() >= 10 ? date.getHours() : '0'+date.getHours();
	var mi=date.getMinutes() >= 10 ? date.getMinutes() : '0'+ date.getMinutes();
	var ss=date.getSeconds() >= 10 ? date.getSeconds() : '0'+ date.getSeconds();
	
	filter=filter.replace('yyyy',yyyy);
	filter=filter.replace('mm',mm);
	filter=filter.replace('dd',dd);
	filter=filter.replace('hh',hh);
	filter=filter.replace('mi',mi);
	filter=filter.replace('ss',ss);
	
	return filter;
}

/**
 * UUID随机数生成
 * @returns
 */
function guid() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
        return v.toString(16);
    });
}

var localVersion = getDateStr(new Date(),'yyyymmddhh');//以日期作为版本号，可通过修改日期格式确定版本号
var basePath = location.origin;//服务的基本路径

//开发
if(basePath == "file://"){
	var currentUrl=location.href;
	basePath = currentUrl.substr(0,currentUrl.lastIndexOf("jwzh-bootstrap-ui")+17);
}

var pathname = location.pathname;
if(pathname && pathname.indexOf('/jwzh-bigdata-jcgl')>=0){
	//有context-path
	basePath+='/jwzh-bigdata-jcgl';
}

var dictBasePath = basePath+'/js/dict/';//字典js的目录

document.write('\<script src="'+basePath+'/js/common/jquery.min.js?v=' + localVersion + '"\>\<\/script\>');//最基本的jquery
document.write('\<script src="'+basePath+'/js/common/jquery.blockUI.js?v=' + localVersion + '"\>\<\/script\>');//“加载中”的蒙层
document.write('\<link href="'+basePath+'/bootstrap/css/bootstrap.min.css?v=' + localVersion + '" rel="stylesheet" />');//bootstrap样式
document.write('\<script src="'+basePath+'/bootstrap/js/bootstrap.min.js?v=' + localVersion + '"\>\<\/script\>');//bootstrap
document.write('\<link href="'+basePath+'/bootstrap/css/bootstrap-datetimepicker.min.css?v=' + localVersion + '" rel="stylesheet" />');//bootstrap样式
document.write('\<script src="'+basePath+'/bootstrap/js/bootstrap-datetimepicker.min.js?v=' + localVersion + '"\>\<\/script\>');//bootstrap
document.write('\<script src="'+basePath+'/bootstrap/js/bootstrap-datetimepicker.zh-CN.js?v=' + localVersion + '"\>\<\/script\>');//bootstrap
document.write('\<script src="'+basePath+'/js/founder/founder.modal.js?v=' + localVersion + '"\>\<\/script\>');//基于bootstrap的模态框组件
document.write('\<script src="'+basePath+'/js/founder/founder.loading.js?v=' + localVersion + '"\>\<\/script\>');//基于bootstrap和blockUI的加载组件
document.write('\<script src="'+basePath+'/js/founder/founder.datebox.js?v=' + localVersion + '"\>\<\/script\>');//基于bootstrap的日期选择
document.write('\<script src="'+basePath+'/js/founder/founder.dictTools.js?v=' + localVersion + '"\>\<\/script\>');//基于bootstrap的日期选择
document.write('\<link href="'+basePath+'/css/base.css?v=' + localVersion + '" rel="stylesheet" />');//全局基本的css

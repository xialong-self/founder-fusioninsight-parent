/*
 * 字典相关的工具方法
 */

/**
 * 获取字典URL
 * @param dictName
 * @returns
 */
function getDictUrl(dictName){
	if(!dictName){
		console.error('dictName is null');
		return null;
	}
	
	var dictUrl = dictBasePath+dictName+'.js?v=' + localVersion;
	return dictUrl;
}

/**
 * 获取字典翻译，同步执行，请不要大批量翻译，如果大批量翻译，请用异步方法
 * @param dictName
 * @param val
 * @returns
 */
function getDictText(dictName,val){
	if(!dictName) return val;
	
	var dictUrl = getDictUrl(dictName);//获取字典URL
	
	var dictData = null;
	$.ajaxSettings.async = false;//同步执行
	$.getJSON(dictUrl,function(json){
		dictData = json;
    });
	$.ajaxSettings.async = true;//异步执行
	
	if(dictData){
		for(var i=0; i<dictData.length; i++){
	        if(dictData[i].id==val){
	            val=dictData[i].text
	            break;
	        }
	    }
	}
	
	return val;
}

/**
 * 获取字典翻译，异步
 * @param dictName 字典名
 * @param val 代码
 * @param showId 显示的ID
 * @returns
 */
function getDictTextAsync(dictName,val,showId){
	if($("#"+showId)){
		$("#"+showId).html(val);//先显示代码
	}
	
	if(!dictName){
		return;
	}
	
	var dictUrl = getDictUrl(dictName);//获取字典URL
	$.getJSON(dictUrl,function(dictData){
		
		if(dictData){
			for(i=0; i<dictData.length; i++){
		        if(dictData[i].id==val){
		        	$("#"+showId).html(dictData[i].text);//显示翻译
		            
		            return;
		        }
		    }
		}
		
    });	
}

/**
 * 递归遍历给定的字典树，返回代码对应的词条
 * @param dict 字典对象
 * @param dm 字典代码
 * @return string 词条
 */
function getTreeDictCt(dict, dm) {
    if (dict) {
        for (var i = 0; i < dict.length; i++) {
            if (dict[i].children instanceof Array){
                var ct = getTreeDictCt(dict[i].children, dm);
                if (ct) return ct;
			}
            if (dict[i].id == dm) {
                return dict[i].text;
            }
        }
    }
}

/**
 * 批量翻译树形字典，结果逗号隔开，异步
 * @param dictName
 * @param dmArray
 * @param showId
 */
function batchTranslateTreeDict(dictName, dmArray,showId) {
    $("#" + showId).html(dmArray.toString());//先显示代码

    if (!dictName) {
        return;
    }

    var dictUrl = getDictUrl(dictName);//获取字典URL
    var texts = "";
    $.getJSON(dictUrl, function (dictData) {
        for (var i = 0; i < dmArray.length; i++) {
        	var aText = getTreeDictCt(dictData, dmArray[i]);
        	if (!aText) {
        		continue;
			}
            texts += ",";
            texts += aText;
        }
        if (texts.charAt(0)==','){
        	texts = texts.substring(1,texts.length);
		}
        $("#" + showId).html(texts);
    });
}

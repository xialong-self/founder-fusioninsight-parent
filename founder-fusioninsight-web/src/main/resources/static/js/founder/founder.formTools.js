function getSubmitData(formObj){
	var formDataAry = formObj.serializeArray();
	var formData = {};
	$.each(formDataAry, function () {
		var index = this.name.indexOf('.');
		if(index>0){			
			var childObjName=this.name.substr(0,index);
			formData[childObjName] =setParam(formData[childObjName],this.name.substr(index+1),this.value)
		}else{
			setParam(formData,this.name,this.value)
		}
	});
	return formData;
}

function setParam(data,paramName,paramValue){
	if(!data)
		data = {};
	
	if (data[paramName]) {//已有相同名的参数
		data[paramName]+=','+paramValue;
	} else {
		data[paramName] = paramValue || '';
	}
	
	return data;
}

/**
 * 通用的简单表单提交方法
 * @param submit_url 提交的uRL
 * @param submit_data 提交的数据
 * @param call_back 回掉函数
 * @returns
 */
function normalSubmitFormData(submit_url,submit_data,call_back,err_back){
	//loading("open","信息提交中...")//显示遮罩
    $.ajax({
    	url : submit_url,
        type: 'post',
        dataType: 'json',
        data:submit_data,
        success : function(data){
        	if (call_back) {
        		var fn = eval(call_back);
        		fn(data);
        	}
        },
        error: function(data){
        	var msg = "数据提交失败！";
        	if(data){
        		if(data.message){
        			msg = data.message;
        		}else if(data.responseJSON){//有后台自定义的错误信息
        			var resMsg = data.responseJSON.message;
        			try{//返回可能不是json
        				var resJson = eval("("+resMsg+")");
            			if(resJson.message){
            				msg = resJson.message;
            			}else{
            				msg = resMsg;
            			}
        			}catch(ex){
        				msg = resMsg;
        			}
        			
        		}else{
        			msg = "数据提交失败:"+data;
        		}
        	}
        	msgAlert({msg:msg});
        	try{
        		loading("close");
        	}catch(e){}
        	if (err_back) {
        		var fn = eval(err_back);
        		fn(data);
        	}
		},
        complete : function(){
        	//loading("close")  //提交成功,关闭遮罩
        }
    });
}


/**
 * 加载表单数据
 * @param formObj 表单对象
 * @param data 数据
 */
function loadDetail(json){
	var detailObjAry = $(".detailText");
	
	var detailObj;//详情标签对象
	var ywid;//业务字段ID
	var dict;//需要翻译的字典
	for(var index = 0;index <detailObjAry.length;index++){
		detailObj = detailObjAry[index];
		ywid = $(detailObj).attr("ywid");
		dict = $(detailObj).attr("dict");
		if(ywid){
			if(dict){
				$(detailObj).text(getDictText(dict,json[ywid]));
			}else{
				$(detailObj).text(json[ywid]);
			}
			
			
		}
	}
}

/**
 * 加载表单数据
 * @param formObj 表单对象
 * @param data 数据
 */
function loadFormData(formObj,data){
	if(!formObj){
		alert("未获取到表单对象");
		return;
	}
	if(!data){
		alert('未获取到表单数据！')
		return;
	}
	
	var formData = getSubmitData(formObj);
	for(var index in formData){
		var htmlObj = $("#"+index);
		if(htmlObj.length==0){
			continue;
		}
		
		try{
			if(isCombobox(htmlObj)){
				setComboboxVal(index,data[index]);
			}else{
				$(htmlObj).val(data[index]);
			}
		}catch(ex){
			$(htmlObj).val(data[index]);
		}
		
		
	}
}

/**
 * 获取session信息，回调afterLoadInit
 */
function getSessionInfo(){
//	$.ajax({
//		url: basePath + '/session/getSessionBean',
//        type: 'get',
//        dataType: 'json',
//        success : function(sessionBean){
        	afterLoadInit({});
//        },
//        complete : function(){
//        	
//        }
//    });
}

/**
 * 获取url中的参数
 * @param paramName
 * @returns
 */
function getUrlParam(paramName){
	try{
		var value=window.location.search.substr(1).match(new RegExp("(^|&)"+ paramName +"=([^&]*)(&|$)"))[2];
		return decodeURI(value);
	}catch(ex){
		return null;
	}
}
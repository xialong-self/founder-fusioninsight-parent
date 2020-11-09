/*
 * founder
 * 基于bootstrap的模态框
 * 
 */

var current_win_id="";

/**
 * 打开模态框
 * @param config
 * @returns
 */
function openUrlWin(url,config){
	if(!url){
		console.error("未设置弹出框的URL");
		return;
	}
	
	if(!config){
		config = {};
	}
	
	if(!config.height){
		config.height = "300px";//默认高度
	}
	
	if(!config.width){
		config.width = "600px";//默认高度
	}
	
	if(!config.winName){
		config.winName = "";
	}
	
	 var win_id = guid();//模态框ID
	 var html = '<div class="modal fade" id="'+win_id+'">';
	 
	 html+='<div class="modal-dialog">';
	 html+='<div class="modal-content" style="width:'+config.width+'">';
	 
	 html+='<div class="modal-header">';
	 html+='<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>';
	 html+='<h4 class="modal-title">'+config.winName+'</h4>';
	 html+='</div>'
	 
	 html+='<div class="modal-body"><iframe id="'+win_id+'_iframe" src="'+url+'" width="99%" height="'+config.height+'" frameborder="0" name="_blank" id="_blank" ></iframe></div>';
	 
	 html+='<div class="modal-footer">';
     html+='<button type="button" class="btn btn-default" data-dismiss="modal">关闭</button>';
     if(!config.noSubmit){
    	 html+='<button type="button" class="btn btn-primary" onclick="doModalSubmit(\''+win_id+'\')">确定</button>';
     }
     
     html+='</div>';
     
     html+='</div>';
     html+='</div>';
	
	$("body").append(html);
	
	$('#'+win_id).modal();
	
	current_win_id = win_id;
	
	$('#'+win_id).on('hide.bs.modal', function() {
		$('#'+win_id).remove();
	});

}

function doModalSubmit(win_id){
	var iframe = document.getElementById(win_id+"_iframe");
	if(iframe.contentWindow.onSubmitClick()){
		//closeUrlWin();
	}
}

function closeUrlWin(){
	$('#'+current_win_id).modal('hide');
}

function msgAlert(config,calback){
	var win = window;
	for(var index = 0;index <10;index++){
		try{
			win.topMsgAlert(config,calback);
			break;
		}catch(e){
			win = win.parent;
		}
	}
		
}

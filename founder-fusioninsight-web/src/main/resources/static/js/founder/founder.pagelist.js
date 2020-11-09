function initPageListTable(page_config) {
    $("#"+page_config.tableId).bootstrapTable({ // 对应table标签的id
        url: page_config.url, // 获取表格数据的url
        method: 'post',
        contentType: 'application/x-www-form-urlencoded',
        dataType: 'json',
        cache: false, // 设置为 false 禁用 AJAX 数据缓存， 默认为true
        toolbar:'#toolbar',//工具栏
        showRefresh: true,//显示刷新按钮
        showColumns: true,//显示列选择
        striped: true,  //表格显示条纹，默认为false
        pagination: true, // 在表格底部显示分页组件，默认false
        pageList: [10, 20, 50,100], // 设置页面可以显示的数据条数
        pageSize: 10, // 页面数据条数
        pageNumber: 1, // 首页页码
        sidePagination: 'server', // 设置为服务器端分页
        dataField: 'rows',//后端分页时需返回含有total：总记录数,这个键值好像是固定的 rows： 记录集合 键值可以修改  dataField
        uniqueId: "id", //每一行的唯一标识，一般为主键列
        queryParams: _getPageListParam,
        columns: page_config.columns,
        onLoadSuccess: function () {  //加载成功时执行
            //console.info("加载成功");
        },
        onLoadError: function () {  //加载失败时执行
           // console.info("加载数据失败");
        }
    });
}

function initListTable(page_config) {
    $("#"+page_config.tableId).bootstrapTable({ // 对应table标签的id
        url: page_config.url, // 获取表格数据的url
        method: 'post',
        contentType: 'application/x-www-form-urlencoded',
        dataType: 'json',
        cache: false, // 设置为 false 禁用 AJAX 数据缓存， 默认为true
        toolbar:'#toolbar',//工具栏
        showRefresh: true,//显示刷新按钮
        showColumns: true,//显示列选择
        striped: true,  //表格显示条纹，默认为false
        pagination: false, // 在表格底部显示分页组件，默认false
        uniqueId: "id", //每一行的唯一标识，一般为主键列
        queryParams: _getPageListParam,
        columns: page_config.columns,
        onLoadSuccess: function () {  //加载成功时执行
            //console.info("加载成功");
        },
        onLoadError: function () {  //加载失败时执行
           // console.info("加载数据失败");
        }
    });
}

/**
 * 获取分页列表的请求参数
 * @param params
 * @returns
 */
function _getPageListParam(params){
	var paramObj;
	
	try{
		paramObj = getPageListParam();
	}catch(e){}
	
	if(!paramObj){
		paramObj = {};
	}
	
	paramObj.page = (params.offset / params.limit) + 1; // 页码
	paramObj.rows = params.limit; // 页码
	
	return paramObj;
}

function dictFormatter(val){
	var span_id=guid();
	getDictTextAsync(this.dictName,val,span_id);
	return '<span id="'+span_id+'"></span>';
}

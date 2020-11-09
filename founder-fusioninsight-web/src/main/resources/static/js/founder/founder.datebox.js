/*
 * founder
 * 基于bootstrap的日期选择
 * 
 */

$(function(){
	initDateBox();
});

/**
 * 初始化datebox
 * @returns
 */
function initDateBox(){
	$('.datebox').datetimepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayBtn: true,
        startView: 'month',
        minView:'month',
        maxView:'decade',
        language:  'zh-CN',
    });

}

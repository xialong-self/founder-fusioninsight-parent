/*
 * founder
 * 基于bootstrap模态框的加载遮罩
 * 
 */


/*
 * 加载遮罩提示信息
 */
function loading (actoin,msg) {
    if(actoin=="open"){
        $.blockUI({message:'<div class="spinner">\n' +
            '  <div class="rect1"></div>\n' +
            '  <div class="rect2"></div>\n' +
            '  <div class="rect3"></div>\n' +
            '  <div class="rect4"></div>\n' +
            '  <div class="rect5"></div>\n' +
            '</div><div style="padding-bottom: 10px;">'+msg+'</div>'})//显示遮罩
    }
    else {
        $.unblockUI()
    }
}

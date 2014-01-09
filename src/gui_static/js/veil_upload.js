NitrogenClass.prototype.$send_pending_files_veil = function(form,input) {
    var file=null;
    if(typeof(form.$nitrogen_pending_files)=="object")
    {
        // not a typo, doing an assignment here
        while(file=form.$nitrogen_pending_files.shift())
        {
            file.submit();
        }
    }
}

NitrogenClass.prototype.$attach_upload_handle_dragdrop_veil = function(form,input,settings) {
    var thisNitro = this;
    if(typeof(settings)=="undefined")
        settings={};
    if(typeof(form.$nitrogen_pending_files)=="undefined")
        form.$nitrogen_pending_files = [];

    jQuery.getScript("/nitrogen/jquery.fileupload.min.js",function(){
        var dropzone = jQuery(".upload_drop");
    
        jQuery(input).fileupload({
            dropZone:(settings.droppable ? dropzone : null),
            singleFileUploads:true,
            sequentialUploads:true,
            url:thisNitro.$url,
            paramName:"file",
            formData: function() {
                form.elements["pageContext"].value = thisNitro.$params["pageContext"];
                var d = jQuery(form).serializeArray();
                return d;
            },
            start: function(e) {
                form.pageContext.value = thisNitro.$params["pageContext"];
                jQuery(".upload_button").val("Uploading...");
                jQuery(".upload_button").prop('disabled', true);
                jQuery(".upload_drop").prop('disabled', true);
                jQuery(".select_files").prop('disabled', true);
                jQuery(".upload_button").removeClass("btn-inverse");
                jQuery(".upload_done_overall").css("width", "0%");
                jQuery(".upload_left_overall").css("width", "100%");
                jQuery(".upload_done").css("width", "0%");
                jQuery(".upload_left").css("width", "100%");
                jQuery(".progress_bar").show();
            },
            progressall: function(e,data) {
                var prog = parseInt(data.loaded / data.total * 100, 10);
                jQuery(".upload_button").val(sizeProgressString(data.loaded, data.total) + " (" + prog + ")%");
                jQuery(".upload_done_overall").css("width", prog + "%");
                jQuery(".upload_left_overall").css("width", (100 - prog) + "%");
            },
            progress: function(e,data) {
                var prog = parseInt(data.loaded / data.total * 100, 10);
                jQuery(".upload_done").css("width", prog + "%");
                jQuery(".upload_left").css("width", (100 - prog) + "%");
                
                jQuery(".pending_files").find("span[filename=\"" + data.files[0].name + "\"][status=added]")
                    .css("background-color", "rgb(241, 196, 15)");
                // Single file progress
            },
            send: function(e,data) {
            },
            stop: function(e,data) {
                
            },
            always: function(e,data) {
            },
            fail: function(e,data, options) {
                Nitrogen.$increment_pending_upload_counter_veil(form,-1);
            },
            add: function(e,data) {
                jQuery(".dummy_tag").remove();
                jQuery.each(data.files,function(i,f) {
                    // Let's add the visual list of pending files
                    jQuery(".pending_files")
                        .append(jQuery("<span></span>").attr("filename",f.name)
                        .attr("status", "added").css("background-color", "rgb(189, 195, 199)")
                        .addClass("tag").append(jQuery("<span></span>").text(f.name)));
                    jQuery(".upload_button").prop('disabled', false);
                    jQuery(".upload_button").addClass("btn-inverse");
                    Nitrogen.$increment_pending_upload_counter_veil(form,1);
                });
                if(settings.autoupload)
                    data.submit();
                else
                    form.$nitrogen_pending_files.push(data);
            },
            done: function(e,data) {
                if(typeof data.result == "string") {
                    // Good browsers will use XHR file transfers, and so this
                    // will return a string
                    var Postback = data.result;
                } else if(typeof data.result == "object") {
                    // Crappy browsers (IE9 and below) will do the transfer
                    // as with an iframe and return a document-type object
                    var Postback = data.result[0].body.innerHTML;
                } else {
                    // IE also has data.result as "undefined" on failure
                    // So let's just treat it as an empty string
                    var Postback = "";
                }

                jQuery.globalEval(Postback);
                Nitrogen.$increment_pending_upload_counter_veil(form,-1);
            }
        })
    })
}

NitrogenClass.prototype.$increment_pending_upload_counter_veil = function(form,incrementer) {
    var counter = $(form).data("pending_uploads");
    if(typeof(counter)=="undefined")
        counter=0;
    counter+=incrementer;
    $(form).data("pending_uploads",counter);
    if(counter==0)
    {
        jQuery(".upload_button").val("Upload successful!");
        jQuery(".upload_done_overall").css("width", "100%");
        jQuery(".upload_left_overall").css("width", "0%");
        jQuery(".upload_done").css("width", "100%");
        jQuery(".upload_left").css("width", "0%");
        jQuery(".progress_bar").fadeOut(1500);
        setTimeout(function f(){jQuery(".upload_button").val("Start upload");}, 1500); 
        jQuery(".upload_drop").prop('disabled', false);
        jQuery(".select_files").prop('disabled', false);  
        //setTimeout(function f(){jQuery(".pending_files").empty()
         //   .append(jQuery("<span></span>").css("background-color", "rgb(235, 237, 239)").addClass("tag dummy_tag")
          //      .append(jQuery("<span></span>").text("No files selected for upload...")));}, 3000);  
        Nitrogen.$alert_unfinished_files_veil(form);
    }
}


NitrogenClass.prototype.$upload_finished_veil = function(Name) {
    jQuery(".upload_done").css("width", "0%");
    jQuery(".upload_left").css("width", "100%");
    jQuery(".pending_files").find("span[filename=\"" + Name + "\"][status=added]")
        .css("background-color", "rgb(46, 204, 113)").attr("status", "done");
}

NitrogenClass.prototype.$alert_unfinished_files_veil = function(form) {
    var files = jQuery(".pending_files").find("span.tag[status='added']");
    if(files.length > 0)
    {
        jQuery(".pending_files").find("span.tag[status='added']")
            .css("background-color", "rgb(231, 76, 60)").attr("status", "error");

        var filenames = $(files).get().map(function(f) { return $(f).text() }).join("\r\n");
        alert("There was an error uploading the following file(s):\r\n" + filenames + "\r\n\r\nPlease try again.");
    }
} 

function sizeProgressString(currentSize, totalSize) 
{
    var i = -1;
    var byteUnits = ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    do {
        currentSize = currentSize / 1024;
        totalSize = totalSize / 1024;
        i++;
    } while (totalSize > 1024);

    return currentSize.toFixed(2) + "/" + totalSize.toFixed(2) + byteUnits[i];
};
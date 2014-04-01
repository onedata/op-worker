var FormID = '#upload_form';
var SubmitButtonID = '#upload_submit';
var DropID = '#drop_zone_container';
var DropListingID = '#drop_listing';
var FileInputID = '#file_input';
var FakeFileInputID = '#file_input_dummy';


veil_send_pending_files = function (form, input) {
    var file = null;
    if (typeof(form.$pending_files) == "object") {
        // not a typo, doing an assignment here
        while (file = form.$pending_files.shift()) {
            file.submit();
        }
    }
}

veil_attach_upload_handle_dragdrop = function (form, input) {
    if (typeof(form.$pending_files) == "undefined")
        form.$pending_files = [];

    $.getScript("js/jquery.fileupload.min.js", function () {
        var dropzone = $("#drop_zone_container");

        $(input).fileupload({
            dropZone: dropzone,
            singleFileUploads: true,
            sequentialUploads: true,
            paramName: "file",
            formData: function () {
                //form.elements["pageContext"].value = this.$params["pageContext"];
                var d = $(form).serializeArray();
                return d;
            },
            start: function (e) {
                //form.pageContext.value = this.$params["pageContext"];
                $(SubmitButtonID).html("Uploading...");
                $(SubmitButtonID).prop('disabled', true);
                $("#drop_zone_container").prop('disabled', true);
                $(".select_files").prop('disabled', true);
                $(SubmitButtonID).removeClass("btn-inverse");
                $(".upload_done_overall").css("width", "0%");
                $(".upload_left_overall").css("width", "100%");
                $(".upload_done").css("width", "0%");
                $(".upload_left").css("width", "100%");
                $(".progress_bar").show();
            },
            progressall: function (e, data) {
                var prog = parseInt(data.loaded / data.total * 100, 10);
                $(SubmitButtonID).html(sizeProgressString(data.loaded, data.total) + " (" + prog + ")%");
                $(".upload_done_overall").css("width", prog + "%");
                $(".upload_left_overall").css("width", (100 - prog) + "%");
            },
            progress: function (e, data) {
                var prog = parseInt(data.loaded / data.total * 100, 10);
                $(".upload_done").css("width", prog + "%");
                $(".upload_left").css("width", (100 - prog) + "%");

                $(".pending_files").find("span[filename=\"" + data.files[0].name + "\"][status=added]")
                    .css("background-color", "rgb(241, 196, 15)");
                // Single file progress
            },
            send: function (e, data) {
            },
            stop: function (e, data) {

            },
            always: function (e, data) {
            },
            fail: function (e, data, options) {
                veil_increment_pending_upload_counter(form, -1);
            },
            add: function (e, data) {
                $(".dummy_tag").remove();
                $.each(data.files, function (i, f) {
                    // Let's add the visual list of pending files
                    $(".pending_files")
                        .append($("<span></span>").attr("filename", f.name)
                            .attr("status", "added").css("background-color", "rgb(189, 195, 199)")
                            .addClass("tag").append($("<span></span>").text(f.name)));
                    $(SubmitButtonID).prop('disabled', false);
                    $(SubmitButtonID).addClass("btn-inverse");
                    veil_increment_pending_upload_counter(form, 1);
                });
                form.$pending_files.push(data);
            },
            done: function (e, data) {
                veil_upload_finished(data.result.files[0].name);
                veil_increment_pending_upload_counter(form, -1);
            }
        })
    })
}

veil_increment_pending_upload_counter = function (form, incrementer) {
    var counter = $(form).data("pending_uploads");
    if (typeof(counter) == "undefined")
        counter = 0;
    counter += incrementer;
    $(form).data("pending_uploads", counter);
    if (counter == 0) {
        $(SubmitButtonID).html("Upload successful!");
        $(".upload_done_overall").css("width", "100%");
        $(".upload_left_overall").css("width", "0%");
        $(".upload_done").css("width", "100%");
        $(".upload_left").css("width", "0%");
        $(".progress_bar").fadeOut(1500);
        setTimeout(function f() {
            $(SubmitButtonID).html("Start upload");
        }, 1500);
        $(".drop_zone_container").prop('disabled', false);
        $("#file_input").prop('disabled', false);
        //setTimeout(function f(){$(".pending_files").empty()
        //   .append($("<span></span>").css("background-color", "rgb(235, 237, 239)").addClass("tag dummy_tag")
        //      .append($("<span></span>").text("No files selected for upload...")));}, 3000);
        report_upload_finish('page_file_manager');
        veil_alert_unfinished_files(form);
    }
}


veil_upload_finished = function (Name) {
    $(".upload_done").css("width", "0%");
    $(".upload_left").css("width", "100%");
    $(".pending_files").find("span[filename=\"" + Name + "\"][status=added]")
        .css("background-color", "rgb(46, 204, 113)").attr("status", "done");
}

veil_alert_unfinished_files = function (form) {
    var files = $(".pending_files").find("span.tag[status='added']");
    if (files.length > 0) {
        files.css("background-color", "rgb(231, 76, 60)").attr("status", "error");

        var filenames = $(files).get().map(function (f) {
            return $(f).text()
        }).join("\r\n");
        alert("There was an error uploading the following file(s):\r\n" + filenames + "\r\n\r\nPlease try again.");
    }
}

function sizeProgressString(currentSize, totalSize) {
    var i = -1;
    var byteUnits = ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    do {
        currentSize = currentSize / 1024;
        totalSize = totalSize / 1024;
        i++;
    } while (totalSize > 1024);

    return currentSize.toFixed(2) + "/" + totalSize.toFixed(2) + byteUnits[i];
};
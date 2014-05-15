// Must be the same as macros in veil_upload.erl
var SUBMIT_BUTTON_ID = '#upload_submit';
var DROPZONE_ID = '#drop_zone_container';
var SELECT_BUTTON_ID = '#file_input';
var FAKE_SELECT_BUTTON_ID = '#file_input_dummy';
var PENDING_FILES_LIST_ID = '#pending_files';

var PROGRESS_BAR_CLASS = '.progress_bar';
var CURRENT_DONE_BAR_ID = '#upload_done';
var CURRENT_LEFT_BAR_ID = '#upload_left';
var OVERALL_DONE_BAR_ID = '#upload_done_overall';
var OVERALL_LEFT_BAR_ID = '#upload_left_overall';

var YELLOW_TAG_COLOUR = "rgb(241, 196, 15)";
var GREEN_TAG_COLOUR = "rgb(46, 204, 113)";
var RED_TAG_COLOUR = "rgb(231, 76, 60)";


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
        var dropzone = $(DROPZONE_ID);

        $(input).fileupload({
            dropZone: dropzone,
            singleFileUploads: true,
            sequentialUploads: true,
            paramName: "file",
            formData: function () {
                return  $(form).serializeArray();
            },
            start: function (e) {
                $(SUBMIT_BUTTON_ID).css("color", "#2C3E50");
                $(SUBMIT_BUTTON_ID).html("Uploading...");
                $(SUBMIT_BUTTON_ID).prop('disabled', true);
                $(DROPZONE_ID).prop('disabled', true);
                $(SELECT_BUTTON_ID).prop('disabled', true);
                $(FAKE_SELECT_BUTTON_ID).prop('disabled', true);
                $(SUBMIT_BUTTON_ID).removeClass("btn-inverse");
                $(OVERALL_DONE_BAR_ID).css("width", "0%");
                $(OVERALL_LEFT_BAR_ID).css("width", "100%");
                $(CURRENT_DONE_BAR_ID).css("width", "0%");
                $(CURRENT_LEFT_BAR_ID).css("width", "100%");
                $(PROGRESS_BAR_CLASS).show();
            },
            progressall: function (e, data) {
                var prog = parseInt(data.loaded / data.total * 100, 10);
                $(SUBMIT_BUTTON_ID).html(sizeProgressString(data.loaded, data.total) + " (" + prog + ")%");
                $(OVERALL_DONE_BAR_ID).css("width", prog + "%");
                $(OVERALL_LEFT_BAR_ID).css("width", (100 - prog) + "%");
            },
            progress: function (e, data) {
                var prog = parseInt(data.loaded / data.total * 100, 10);
                $(CURRENT_DONE_BAR_ID).css("width", prog + "%");
                $(CURRENT_LEFT_BAR_ID).css("width", (100 - prog) + "%");

                // Paint a tag yellow, but only the first one found
                if ($(PENDING_FILES_LIST_ID).find("span[filename=\"" + data.files[0].name + "\"][status=in_progress]").length == 0) {
                    $($(PENDING_FILES_LIST_ID).find("span[filename=\"" + data.files[0].name + "\"][status=added]")[0])
                        .attr("status", "in_progress")
                        .css("background-color", YELLOW_TAG_COLOUR);
                }
            },
            send: function (e, data) {
            },
            stop: function (e, data) {

            },
            always: function (e, data) {
            },
            fail: function (e, data, options) {
                $(findTagByFilename(data.files[0].name)).
                    css("background-color", RED_TAG_COLOUR).
                    attr("status", "error");
                veil_increment_pending_upload_counter(form, -1);
            },
            add: function (e, data) {
                $(".dummy_tag").remove();
                $.each(data.files, function (i, f) {
                    // Let's add the visual list of pending files
                    $(PENDING_FILES_LIST_ID)
                        .append($("<span></span>")
                            .attr("filename", f.name)
                            .attr("status", "added")
                            .css("background-color", "rgb(189, 195, 199)")
                            .addClass("tag")
                            .append($("<span></span>").text(f.name)));
                    $(SUBMIT_BUTTON_ID).prop('disabled', false);
                    $(SUBMIT_BUTTON_ID).addClass("btn-inverse");
                    veil_increment_pending_upload_counter(form, 1);
                });
                form.$pending_files.push(data);
            },
            done: function (e, data) {
                $(CURRENT_DONE_BAR_ID).css("width", "0%");
                $(CURRENT_LEFT_BAR_ID).css("width", "100%");
                $(findTagByFilename(data.result.files[0].name)).
                    css("background-color", GREEN_TAG_COLOUR).
                    attr("status", "success");
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
        $(SUBMIT_BUTTON_ID).html("Upload successful!");
        $(OVERALL_DONE_BAR_ID).css("width", "100%");
        $(OVERALL_LEFT_BAR_ID).css("width", "0%");
        $(CURRENT_DONE_BAR_ID).css("width", "100%");
        $(CURRENT_LEFT_BAR_ID).css("width", "0%");
        $(PROGRESS_BAR_CLASS).fadeOut(1500);
        setTimeout(function f() {
            $(SUBMIT_BUTTON_ID).html("Start upload");
            $(DROPZONE_ID).prop('disabled', false);
            $(SELECT_BUTTON_ID).prop('disabled', false);
            $(FAKE_SELECT_BUTTON_ID).prop('disabled', false);
            $(SUBMIT_BUTTON_ID).css("color", "#FFFFFF");
        }, 1500);
        //setTimeout(function f(){$(PENDING_FILES_LIST_ID).empty()
        //   .append($("<span></span>").css("background-color", "rgb(235, 237, 239)").addClass("tag dummy_tag")
        //      .append($("<span></span>").text("No files selected for upload...")));}, 3000);
        report_upload_finish($('#upload_form').find("input[name=\"pid\"]").val());
        veil_alert_unfinished_files(form);
    }
}

veil_alert_unfinished_files = function (form) {
    var files = $(PENDING_FILES_LIST_ID).find("span.tag[status='error']");
    if (files.length > 0) {
        var filenames = $(files).get().map(function (f) {
            return $(f).text()
        }).join("\r\n");
        alert("There was an error uploading the following file(s):\r\n" + filenames + "\r\n\r\nPlease try again.");
    }
}

function findTagByFilename(FileName) {
    var tags = $(PENDING_FILES_LIST_ID).find("span[filename=\"" + FileName + "\"][status=in_progress]");
    if (tags.length == 0) {
        tags = $(PENDING_FILES_LIST_ID).find("span[filename=\"" + FileName + "\"][status=added]");
    }
    return tags[0];    
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
}
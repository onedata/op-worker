%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains element definition for advanced file upload form.
%% Two fields are required when using veil_upload:
%% delegate - module containing upload_event/1 callback (preferably page module that
%% uses this element)
%% target_dir - target directory to save files in
%% @end
%% ===================================================================

-module(veil_upload).
-include("veil_modules/control_panel/vcn_common.hrl").
-include_lib("ctool/include/logging.hrl").

-export([render_element/1, api_event/3]).

-define(upload_start_callback, "report_upload_start").
-define(upload_finish_callback, "report_upload_finish").

% Must be the same as constants in veil_upload.js
-define(FORM_ID, "upload_form").

-define(SELECT_BUTTON_TEXT, "Select files").
-define(SELECT_BUTTON_ID, "file_input").
-define(FAKE_SELECT_BUTTON_ID, "file_input_dummy").

-define(DROPZONE_ID, "drop_zone_container").
-define(DROPZONE_TEXT, "Drop files").

-define(SUBMIT_BUTTON_ID, "upload_submit").
-define(SUBMIT_BUTTON_TEXT, "Start upload").

-define(PENDING_FILES_LIST_ID, "pending_files").

-define(PROGRESS_BAR_CLASS, "progress_bar").
-define(CURRENT_DONE_BAR_ID, "upload_done").
-define(CURRENT_LEFT_BAR_ID, "upload_left").
-define(OVERALL_DONE_BAR_ID, "upload_done_overall").
-define(OVERALL_LEFT_BAR_ID, "upload_left_overall").


render_element(Record) ->
    TargetDir = Record#veil_upload.target_dir,
    SubscriberPid = Record#veil_upload.subscriber_pid,
    PickledPid = wf:pickle(SubscriberPid),

    gui_jq:wire(#api{name = ?upload_start_callback, tag = ?upload_start_callback, delegate = ?MODULE}),
    gui_jq:wire(#api{name = ?upload_finish_callback, tag = ?upload_finish_callback, delegate = ?MODULE}),
    gui_jq:wire(<<"$('#", ?SUBMIT_BUTTON_ID, "').prop('disabled', true);">>),

    SubmitJS = <<"function (e){ veil_send_pending_files($('#", ?FORM_ID, "').get(0), $('#", ?SELECT_BUTTON_ID, "').get(0)); }">>,
    gui_jq:bind_element_click(<<?SUBMIT_BUTTON_ID>>, SubmitJS),

    ReportUploadStartJS = <<"function (e){ ", ?upload_start_callback, "('", PickledPid/binary, "'); }">>,
    gui_jq:bind_element_click(<<?SUBMIT_BUTTON_ID>>, ReportUploadStartJS),

    UploadJS = <<"veil_attach_upload_handle_dragdrop($('#", ?FORM_ID, "').get(0), $('#", ?SELECT_BUTTON_ID, "').get(0));">>,
    gui_jq:wire(UploadJS),


% Set the dimensions of the file input element the same as
% faked file input button has.

% Render the controls and hidden iframe...

    UploadDropStyle = <<"padding:20px;",
    "height:50px;",
    "text-align:center;",
    "width:250px;",
    "background-color: white;",
    "border:2px dashed rgb(26, 188, 156);",
    "font-size:18pt;",
    "border-radius:15px;",
    "-moz-border-radius:15px;">>,


    DropzoneStyle = <<"width:100%;",
    "height:50px;",
    "line-height: 50px;",
    "text-align: center;",
    "vertical-align: middle;">>,

    FormContent = [
%% IE9 does not support the droppable option, so let's just hide the drop field
        "<!--[if lte IE 9]>
            <style type='text/css'> .upload_drop {display: none} </style>
        <![endif]-->",

        #panel{style = <<"margin: 0 40px; overflow:hidden; position: relative; display:block;">>, body = [
            #panel{style = <<"float: left;">>, body = [
                #panel{
                    id = <<?DROPZONE_ID>>,
                    style = UploadDropStyle,
                    body = [
                        #panel{
                            class = <<"dropzone">>,
                            style = DropzoneStyle,
                            body = <<?DROPZONE_TEXT>>
                        }
                    ]
                },

                #panel{
                    style = <<"position: relative; margin-top: 15px;">>,
                    body = [
                        wf_tags:emit_tag(<<"input">>, [
                            {<<"type">>, <<"button">>},
                            {<<"class">>, <<"btn btn-inverse">>},
                            {<<"style">>, <<"margin: 0 auto; position: absolute; z-index: 1; width: 290px; height: 37px;">>},
                            {<<"value">>, <<?SELECT_BUTTON_TEXT>>},
                            {<<"id">>, <<?FAKE_SELECT_BUTTON_ID>>}
                        ]),

                        wf_tags:emit_tag(<<"input">>, [
                            {<<"name">>, <<"file">>},
                            {<<"multiple">>, <<"true">>},
                            {<<"id">>, <<?SELECT_BUTTON_ID>>},
                            {<<"type">>, <<"file">>},
                            {<<"style">>, <<"cursor: pointer; margin: 0 auto; opacity: 0; filter:alpha(opacity: 0); position: relative; z-index: 2; width: 290px; height: 37px;">>}
                        ])
                    ]
                }
            ]},
            #panel{style = <<"position:relative; margin-left: 330px; margin-right: 0px; overflow: hidden;">>, body = [
                #panel{id = <<?PENDING_FILES_LIST_ID>>, class = <<"tagsinput tagsinput-primary">>, style = <<"height: 100%; max-height: 125px; overflow-y: scroll;">>, body = [
                    #span{class = <<"tag dummy_tag">>, style = <<"background-color:rgb(189, 195, 199);">>, body = [
                        #span{body = <<"No files selected for upload...">>}
                    ]}
                ]},

                #panel{class = <<?PROGRESS_BAR_CLASS, " progress">>, style = <<"margin: 0 0 12px; display: none;">>, body = [
                    #panel{id = <<?CURRENT_DONE_BAR_ID>>, class = <<"bar bar-warning">>, style = <<"width: 0%; transition-property: none;",
                    "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>},
                    #panel{id = <<?CURRENT_LEFT_BAR_ID>>, class = <<"bar">>, style = <<"background-color: white; width: 100%; transition-property: none;",
                    "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>}
                ]},
                #panel{class = <<?PROGRESS_BAR_CLASS, " progress">>, style = <<"margin: 0 0 12px; display: none;">>, body = [
                    #panel{id = <<?OVERALL_DONE_BAR_ID>>, class = <<"bar bar-success">>, style = <<"width: 0%; transition-property: none;",
                    "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>},
                    #panel{id = <<?OVERALL_LEFT_BAR_ID>>, class = <<"bar">>, style = <<"background-color: white; width: 100%; transition-property: none;",
                    "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>}
                ]},
                #button{id = <<?SUBMIT_BUTTON_ID>>, class = <<"btn btn-block no-margin">>, body = <<?SUBMIT_BUTTON_TEXT>>},

                wf_tags:emit_tag(<<"input">>, [
                    {<<"name">>, <<"targetDir">>},
                    {<<"type">>, <<"hidden">>},
                    {<<"value">>, TargetDir}
                ]),

                wf_tags:emit_tag(<<"input">>, [
                    {<<"name">>, <<"pid">>},
                    {<<"type">>, <<"hidden">>},
                    {<<"value">>, PickledPid}
                ])
            ]}
        ]}
    ],

    [
        wf_tags:emit_tag(<<"form">>, wf:render(FormContent), [
            {<<"id">>, <<?FORM_ID>>},
            {<<"name">>, <<"files[]">>},
            {<<"method">>, <<"POST">>},
            {<<"enctype">>, <<"multipart/form-data">>},
            {<<"style">>, <<"width: 100%; position: relative; overflow: hidden; margin-bottom: 15px;">>},
            {<<"action">>, <<?file_upload_path>>}
        ])
    ].


% This is called once the user presses start. Sends a 'upload_started' message to subscriber_pid.
api_event(?upload_start_callback, Args, _) ->
    try
        PickledPid = mochijson2:decode(Args),
        wf:depickle(PickledPid) ! upload_started
    catch Type:Message ->
        ?error_stacktrace("Error, could not resolve subscriber pid during upload - ~p:~p", [Type, Message])
    end;


% This is called once upload completes (all files). Sends a 'upload_finished' message to subscriber_pid.
api_event(?upload_finish_callback, Args, _) ->
    try
        PickledPid = mochijson2:decode(Args),
        wf:depickle(PickledPid) ! upload_finished
    catch Type:Message ->
        ?error_stacktrace("Error, could not resolve subscriber pid during upload - ~p:~p", [Type, Message])
    end.
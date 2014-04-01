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
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

-export([render_element/1, api_event/3]).

-define(upload_start_callback, "report_upload_start").
-define(upload_finish_callback, "report_upload_finish").

render_element(Record) ->
    DroppableText = Record#veil_upload.droppable_text,
    FileInputText = Record#veil_upload.file_text,
    ButtonText = Record#veil_upload.button_text,
    TargetDir = Record#veil_upload.target_dir,
    FormID = "upload_form",
    SubmitButtonID = "upload_submit",
    DropID = "drop_zone_container",
    DropListingID = "drop_listing",
    FileInputID = "file_input",
    FakeFileInputID = "file_input_dummy",

    wf:wire(#api{name = ?upload_start_callback, tag = ?upload_start_callback, delegate = ?MODULE}),
    wf:wire(#api{name = ?upload_finish_callback, tag = ?upload_finish_callback, delegate = ?MODULE}),
    wf:wire(wf:f("$('#~s').prop('disabled', true);", [SubmitButtonID])),

    SubmitJS = wf:f("veil_send_pending_files($('#~s').get(0), $('#~s').get(0));", [FormID, FileInputID]),
    wf:wire(gui_utils:script_to_bind_element_click(SubmitButtonID, SubmitJS)),

    ReportUploadStartJS = wf:f("~s('~s');", [?upload_start_callback, gui_utils:to_list(Record#veil_upload.delegate)]),
    wf:wire(gui_utils:script_to_bind_element_click(SubmitButtonID, ReportUploadStartJS)),

    UploadJS = wf:f("veil_attach_upload_handle_dragdrop($('#~s').get(0), $('#~s').get(0));", [FormID, FileInputID]),
    wf:wire(UploadJS),


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
                    id = DropID,
                    style = UploadDropStyle,
                    body = [
                        #panel{
                            class = <<"dropzone">>,
                            style = DropzoneStyle,
                            body = DroppableText
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
                            {<<"value">>, FileInputText},
                            {<<"id">>, FakeFileInputID}
                        ]),

                        wf_tags:emit_tag(<<"input">>, [
                            {<<"name">>, <<"file">>},
                            {<<"multiple">>, <<"true">>},
                            {<<"class">>, <<"no_postback ", (list_to_binary(FileInputID))/binary>>},
                            {<<"id">>, list_to_binary(FileInputID)},
                            {<<"type">>, <<"file">>},
                            {<<"style">>, <<"cursor: pointer; margin: 0 auto; opacity: 0; filter:alpha(opacity: 0); position: relative; z-index: 2; width: 290px; height: 37px;">>}
                        ])
                    ]
                }
            ]},
            #panel{style = <<"position:relative; margin-left: 330px; margin-right: 0px; overflow: hidden;">>, body = [
                #panel{class = <<"pending_files tagsinput tagsinput-primary">>, style = <<"height: 100%; max-height: 125px; overflow-y: scroll;">>, body = [
                    #span{class = <<"tag dummy_tag">>, style = <<"background-color:rgb(189, 195, 199);">>, body = [
                        #span{body = <<"No files selected for upload...">>}
                    ]}
                ]},

                #panel{class = <<"progress_bar progress">>, style = <<"margin: 0 0 12px; display: none;">>, body = [
                    #panel{class = <<"bar bar-warning upload_done">>, style = <<"width: 0%; transition-property: none;",
                        "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>},
                    #panel{class = <<"bar upload_left">>, style = <<"background-color: white; width: 100%; transition-property: none;",
                        "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>}
                ]},
                #panel{class = <<"progress_bar progress">>, style = <<"margin: 0 0 12px; display: none;">>, body = [
                    #panel{class = <<"bar bar-success upload_done_overall">>, style = <<"width: 0%; transition-property: none;",
                        "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>},
                    #panel{class = <<"bar upload_left_overall">>, style = <<"background-color: white; width: 100%; transition-property: none;",
                        "-moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none;">>}
                ]},
                #button{id = SubmitButtonID, class = <<"btn btn-block no-margin">>, body = ButtonText},

                %% TODO?
                #list{
                    style = <<"margin-top: 12px;">>,
                    show_if = false,
                    id = DropListingID,
                    class = upload_droplist
                },

                %% TODO?
                wf_tags:emit_tag(<<"input">>, [
                    {<<"name">>, <<"pageContext">>},
                    {<<"type">>, <<"hidden">>},
                    {<<"class">>, <<"no_postback">>},
                    {<<"value">>, <<"">>}
                ]),

                wf_tags:emit_tag(<<"input">>, [
                    {<<"name">>, <<"targetDir">>},
                    {<<"type">>, <<"hidden">>},
                    {<<"class">>, <<"no_postback">>},
                    {<<"value">>, TargetDir}
                ]),

                %% TODO?
                wf_tags:emit_tag(<<"input">>, [
                    {<<"type">>, <<"hidden">>},
                    {<<"class">>, <<"no_postback">>},
                    {<<"value">>, <<"">>}
                ])
            ]}
        ]}
    ],

    [
        wf_tags:emit_tag(<<"form">>, wf:render(FormContent), [
            {<<"id">>, FormID},
            {<<"name">>, <<"files[]">>},
            {<<"method">>, <<"POST">>},
            {<<"enctype">>, <<"multipart/form-data">>},
            {<<"class">>, <<"no_postback">>},
            {<<"style">>, <<"width: 100%; position: relative; overflow: hidden; margin-bottom: 15px;">>},
            {<<"action">>, <<?file_upload_path>>}
        ])
    ].


api_event(?upload_start_callback, Args, _) ->
    Delegate = binary_to_atom(mochijson2:decode(Args), latin1),
    Delegate:upload_event(start);


api_event(?upload_finish_callback, Args, _) ->
    Delegate = binary_to_atom(mochijson2:decode(Args), latin1),
    Delegate:upload_event(finish).
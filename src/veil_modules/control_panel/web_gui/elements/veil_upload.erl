%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains element definition for advanced file upload form
%% @end
%% ===================================================================

-module(veil_upload).
-include("veil_modules/control_panel/common.hrl").
-export([
    reflect/0,
    render_element/1,
    event/1
]).

%% #veil_upload allows a user to upload a file.
%% 
%% How it works:
%% - This element creates an <input type=file ...> HTML element on the page, wrapped
%%   in a <form>, with all of the required parameters necessary to fake the system
%%   into believing it is a real postback call. 
%%
%% - When the user clicks the upload button, first the 'upload_started' event
%%   gets fired, calling start_upload_event(Tag) on the Module or Page.
%%
%% - Then, the browser begins uploading the file to the server. The multipart file
%%   is parsed in SimpleBridge.
%%
%% - Finally, once the upload is complete, control is passed on to Nitrogen, which reads 
%%   the parameters sent over in the first step and calls the 'upload_finished' event in
%%   this module.
%%
%% - The 'upload_finished' emits Javascript that causes *another* postback, this time
%%   to the 'upload_event' event in this module, which then calls 
%%   Module:finish_upload_event(Tag, OriginalName, TempFile, Node).
%%   The reason we do this extra postback is because the upload itself happens in a form
%%   separate from the main Nitrogen page (otherwise the main Nitrogen page would need to 
%%   refresh) so this is our way of getting the main page to see the event.


reflect() -> record_info(fields, veil_upload).

render_element(Record) ->
    Anchor = Record#veil_upload.anchor,
	Multiple = Record#veil_upload.multiple,
    Droppable = Record#veil_upload.droppable,
    DroppableText = Record#veil_upload.droppable_text,
    FileInputText = Record#veil_upload.file_text,
    ShowButton = Record#veil_upload.show_button,
    ButtonText = Record#veil_upload.button_text,
    TargetDir = Record#veil_upload.target_dir,
    StartedTag = {upload_started, Record},
    FinishedTag = {upload_finished, Record}, 
    FormID = wf:temp_id(),
    IFrameID = wf:temp_id(),
    ButtonID = wf:temp_id(),
    DropID = wf:temp_id(),
    DropListingID = wf:temp_id(),
    FileInputID = wf:temp_id(),
    FakeFileInputID = wf:temp_id(),

	Param = [
		{droppable,Droppable},
		{autoupload,not(ShowButton)}
	],

	JSONParam = nitro_mochijson2:encode({struct,Param}),
	SubmitJS = wf:f("Nitrogen.$send_pending_files_veil(jQuery('#~s').get(0),jQuery('#~s').get(0));",[FormID,FileInputID]),
    UploadJS = wf:f("Nitrogen.$attach_upload_handle_dragdrop_veil(jQuery('#~s').get(0),jQuery('#~s').get(0),~s);", [FormID,FileInputID,JSONParam]),

    PostbackInfo = wf_event:serialize_event_context(FinishedTag, Record#veil_upload.id, undefined, false, ?MODULE),

    % Create a postback that is called when the user first starts the upload...
    wf:wire(Anchor, #event { show_if=(not ShowButton), type=change, delegate=?MODULE, postback=StartedTag }),
    wf:wire(ButtonID, #event { show_if=ShowButton, type=click, delegate=?MODULE, postback=StartedTag }),

    % If the button is invisible, then start uploading when the user selects a file.
    %wf:wire(Anchor, #event { show_if=(not ShowButton), type=change, actions=SubmitJS }),
    wf:wire(ButtonID, #event { show_if=ShowButton, type=click, actions=SubmitJS }),
    wf:wire(#script { script="jQuery(\".upload_button\").prop('disabled', true);"}),

    wf:wire(UploadJS),

    % Set the dimensions of the file input element the same as
    % faked file input button has.

    % Render the controls and hidden iframe...

    UploadDropS = "padding:20px;
    height:50px;
    text-align:center;
    width:250px;
    background-color: white;
    border:2px dashed rgb(26, 188, 156);
    font-size:18pt;
    border-radius:15px;
    -moz-border-radius:15px;",


    DropzoneS = "
    width:100%;
    height:50px;
    line-height: 50px;
    text-align: center;
    vertical-align: middle;",

    FormContent = [
        %% IE9 does not support the droppable option, so let's just hide the drop field
        "<!--[if lte IE 9]>
            <style type='text/css'> .upload_drop {display: none} </style>
        <![endif]-->",

        #panel {style="margin: 0 40px; overflow:hidden; position: relative; display:block;", body=[
            #panel { style="float: left;", body=[
                #panel{
                    id=DropID,
                    class=[upload_drop],
                    style=UploadDropS,
                    body=[
                        #panel{
                            class=[dropzone],
                            style=DropzoneS,
                            text=DroppableText
                        }
                    ]
                },

                #panel{
                    style="position: relative; margin-top: 15px;",
                    body=[
                        wf_tags:emit_tag(input, [
                            {type, button},
                            {class, "btn btn-inverse"},
                            {style, "margin: 0 auto; position: absolute; z-index: 1; width: 290px; height: 37px;"},
                            {value, FileInputText},
                            {id, FakeFileInputID}
                        ]),

                        wf_tags:emit_tag(input, [
                            {name, file},
                            {multiple, Multiple},
                            {class, [select_files, no_postback, FileInputID|Anchor]},
                            {id, FileInputID},
                            {type, file},
                            {style, "cursor: pointer; margin: 0 auto; opacity: 0; filter:alpha(opacity: 0); position: relative; z-index: 2; width: 290px; height: 37px;"}
                        ])
                    ]
                }
            ]},
            #panel { style="position:relative; margin-left: 330px; margin-right: 0px; overflow: hidden;", body=[
                #panel { class="pending_files tagsinput tagsinput-primary", style="height: 100%; max-height: 125px; overflow-y: scroll;", body=[
                    #span { class="tag dummy_tag", style="background-color:rgb(189, 195, 199);", body=#span { text="No files selected for upload..."}}
                ]},
                
                #panel { class="progress_bar progress", style="margin: 0 0 12px; display: none;", body=[
                    #panel { class="bar bar-warning upload_done", style="width: 0%; transition-property: none; 
                        -moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none; " },
                    #panel { class="bar upload_left", style="background-color: white; width: 100%; transition-property: none; 
                        -moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none; " }
                ]},
                #panel { class="progress_bar progress", style="margin: 0 0 12px; display: none;", body=[
                    #panel { class="bar bar-success upload_done_overall", style="width: 0%; transition-property: none; 
                        -moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none; " },
                    #panel { class="bar upload_left_overall", style="background-color: white; width: 100%; transition-property: none; 
                        -moz-transition-property: none; -webkit-transition-property: none;-o-transition-property: none; " }
                ]},
                #button { id=ButtonID, class="upload_button btn btn-block no-margin",  
                    show_if=ShowButton, text=ButtonText },
                #list{
                    style="margin-top: 12px;",
                    show_if=Droppable,
                    id=DropListingID,
                    class=upload_droplist
                },


                wf_tags:emit_tag(input, [
                    {name, eventContext},
                    {type, hidden},
                    {class, no_postback},
                    {value, PostbackInfo}
                ]),

                wf_tags:emit_tag(input, [
                    {name, pageContext},
                    {type, hidden},
                    {class, no_postback},
                    {value, ""}
                ]),

                wf_tags:emit_tag(input, [
                    {name, targetDir},
                    {type, hidden},
                    {class, no_postback},
                    {value, TargetDir}
                ]),

                wf_tags:emit_tag(input, [
                    {type, hidden},
                    {class, no_postback},
                    {value, ""}
                ])
            ]}
        ]}
    ],

    [
        wf_tags:emit_tag(form, FormContent, [
            {id, FormID},
            {name, upload}, 
            {method, 'POST'},
            {enctype, "multipart/form-data"},
            {class, no_postback},
            {style, "width: 100%; position: relative; overflow: hidden; margin-bottom: 15px;"},
            {target, IFrameID}
        ])
    ].


-spec event(any()) -> any().
% This event is fired when the user first clicks the upload button.
event({upload_started, Record}) ->
    Module = wf:coalesce([Record#veil_upload.delegate, wf:page_module()]),
    Module:start_upload_event(Record#veil_upload.tag);


% This event is called once the upload post happens behind the scenes.
% It happens somewhat outside of Nitrogen, so the next thing we do
% is trigger a postback that happens inside of Nitrogen. 
event({upload_finished, Record}) ->
    wf_context:type(first_request),
    Req = wf_context:request_bridge(),

    % % Create the postback...
    {Filename, NewTag} = case Req:post_files() of
        [] -> 
            {undefined,{upload_event, Record, undefined, undefined, undefined}};
        [#uploaded_file { name=Name, path=Path }|_] ->
            {Name,{upload_event, Record, Name, Path, node()}}
    end,

    % Make the tag...
    Anchor = wf_context:anchor(),
    ValidationGroup = wf_context:event_validation_group(),
    HandleInvalid = wf_context:event_handle_invalid(),
    Postback = wf_event:generate_postback_script(NewTag, Anchor, ValidationGroup, HandleInvalid, undefined, ?MODULE, undefined),

    % Set the response...
    wf_context:data([
        "Nitrogen.$upload_finished_veil(\"",wf:js_escape(Filename),"\");",
        Postback
    ]);

% This event is fired by the upload_finished event, it calls
% back to the page or control that contained the upload element.
event({upload_event, Record, Name, Path, Node}) ->
    Module = wf:coalesce([Record#veil_upload.delegate, wf:page_module()]),
    Module:finish_upload_event(Record#veil_upload.tag, Name, Path, Node).

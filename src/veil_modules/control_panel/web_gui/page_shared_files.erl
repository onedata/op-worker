%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains Nitrogen website code
%% @end
%% ===================================================================

-module(page_shared_files).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("logging.hrl").

%% Template points to the template file, which will be filled with content
main() -> #template{file = "./gui_static/templates/bare.html"}.

%% Page title
title() -> "Shared files".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
    gui_utils:apply_or_redirect(?MODULE, render_body, true).


% Page content
render_body() ->
    [
        gui_utils:top_menu(shared_files_tab),
        #panel{style = "margin-top: 59px;", body = main_panel()},
        footer_popup()
    ].


% Main table   
main_panel() ->
    put(user_id, gui_utils:get_user_dn()),
    ShareEntries = lists:foldl(
        fun(#veil_document{uuid = UUID, record = #share_desc{file = FileID}}, Acc) ->
            case logical_files_manager:get_file_user_dependent_name_by_uuid(FileID) of
                {ok, FilePath} ->
                    Filename = filename:basename(FilePath),
                    AddressPrefix = "https://" ++ gui_utils:get_requested_hostname() ++
                        ?shared_files_download_path,
                    Acc ++ [{"~/" ++ FilePath, Filename, AddressPrefix, UUID}];
                _ ->
                    Acc
            end
        end, [], get_shared_files()),

    TableRows = lists:map(
        fun({LinkText, Filename, AddressPrefix, UUID}) ->
            _TableRow = #tablerow{cells = [
                #tablecell{body = #span{class = "table-cell", body = [
                    #panel{style = "display: inline-block; vertical-align: middle;", body = [
                        #image{class = "list-icon", image = "/images/file32.png"}
                    ]},
                    #panel{class = "filename_row", style = "word-wrap: break-word; display: inline-block;vertical-align: middle;", body = [
                        #link{text = LinkText, new = true, url = AddressPrefix ++ UUID}
                    ]}
                ]}},
                #tablecell{style = "width: 80px;", body = #span{class = "table-cell", body = [
                    #panel{style = "margin: 5px 0; display: inline-block; vertical-align: middle;", body = [
                        #link{class = "glyph-link", style = "margin-right: 25px;",
                        postback = {action, show_link, [UUID]}, body = #span{class = "fui-link",
                        style = "font-size: 24px; margin: -4px 0px 0px; position: relative; top: 4px;"}},
                        #link{class = "glyph-link", postback = {action, remove_link_prompt, [UUID, Filename]},
                        body = #span{class = "fui-cross", style = "font-size: 24px;
                                        margin: -4px 0px 0px; position: relative; top: 4px;"}}
                    ]}
                ]}}
            ]}
        end, lists:usort(ShareEntries)),  % Sort link names alphabetically

    PanelBody = case TableRows of
                    [] ->
                        #p{style = "padding: 15px;", text = "No shared files"};
                    _ ->
                        #table{id = main_table, class = "table table-stripped", style = "border-radius: 0; margin-bottom: 0;", rows = TableRows}
                end,
    wf:wire(#script{script = wf:f("window.onresize = function(e) { $('.filename_row').css('max-width', '' +
        ($(window).width() - ~B) + 'px'); }; $(window).resize();", [250])}), % 240 is size of button cell + paddings.
    PanelBody.


% Get list of user's shared files from database
get_shared_files() ->
    #veil_document{uuid = UID} = wf:session(user_doc),
    _ShareList = case logical_files_manager:get_share({user, UID}) of
                     {ok, List} when is_list(List) -> List;
                     {ok, Doc} -> [Doc];
                     _ -> []
                 end.


% Footer popup panel
footer_popup() ->
    #panel{class = "dialog success-dialog wide hidden", style = "position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0; width: 100%;", id = footer_popup, body = []}.


% Handle postback event
event({action, Fun}) ->
    event({action, Fun, []});

event({action, Fun, Args}) ->
    gui_utils:apply_or_redirect(?MODULE, Fun, Args, true).


% Display link to file in popup panel
show_link(ShareID) ->
    AddressPrefix = "https://" ++ gui_utils:get_requested_hostname() ++ ?shared_files_download_path,
    Body = [
        #link{postback = {action, hide_popup}, title = "Hide", class = "glyph-link",
        style = "position: absolute; top: 8px; right: 8px; z-index: 3;",
        body = #span{class = "fui-cross", style = "font-size: 20px;"}},
        #form{class = "control-group", body = [
            %% todo add hostname dynamically
            #textbox{id = shared_link_textbox, class = "flat", style = "width: 700px;", postback = {action, hide_popup},
            text = AddressPrefix ++ ShareID, placeholder = "Download link"}
        ]}
    ],
    wf:update(footer_popup, Body),
    wf:wire(footer_popup, #remove_class{class = "hidden", speed = 0}),
    wf:wire(footer_popup, #slide_down{speed = 200}),
    wf:wire(#script{script =
    "obj('shared_link_textbox').focus(); obj('shared_link_textbox').select();"}).


% Display removal prompt in popup panel
remove_link_prompt(ShareID, Filename) ->
    Body =
        [
            #link{postback = {action, hide_popup}, title = "Hide", class = "glyph-link",
            style = "position: absolute; top: 8px; right: 8px; z-index: 3;",
            body = #span{class = "fui-cross", style = "font-size: 20px;"}},
            #form{class = "control-group", body = [
                #p{body = wf:f("Remove share for <b>~s</b>?", [Filename])},
                #bootstrap_button{id = ok_button, class = "btn btn-success btn-wide", body = "Ok", postback = {action, remove_link, [ShareID]}},
                #bootstrap_button{class = "btn btn-danger btn-wide", body = "Cancel", postback = {action, hide_popup}}
            ]}
        ],
    wf:update(footer_popup, Body),
    wf:wire(footer_popup, #remove_class{class = "hidden", speed = 0}),
    wf:wire(footer_popup, #slide_down{speed = 200}),
    wf:wire(#script{script = "obj('ok_button').focus();"}).


% Actually remove a link
remove_link(ShareID) ->
    ok = logical_files_manager:remove_share({uuid, ShareID}),
    wf:replace(main_table, main_panel()),
    wf:update(footer_popup, []),
    wf:wire(footer_popup, #add_class{class = "hidden", speed = 0}),
    wf:wire(footer_popup, #slide_up{speed = 200}).


% Hide popup panel 
hide_popup() ->
    wf:update(footer_popup, []),
    wf:wire(footer_popup, #add_class{class = "hidden", speed = 0}),
    wf:wire(footer_popup, #slide_up{speed = 200}).
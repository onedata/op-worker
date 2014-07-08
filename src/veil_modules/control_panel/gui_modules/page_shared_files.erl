%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page contains a list of currently shared files to view or delete.
%% @end
%% ===================================================================

-module(page_shared_files).
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("logging.hrl").

% n2o API
-export([main/0, event/1, api_event/3]).
% Postback functions
-export([show_link/1, remove_link_prompt/2, remove_link/1, hide_popup/0]).

%% Template points to the template file, which will be filled with content
main() ->
    case vcn_gui_utils:maybe_redirect(true, true, true, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
    end.

%% Page title
title() -> <<"Shared files">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    gui_jq:register_escape_event("escape_pressed"),
    [
        vcn_gui_utils:top_menu(shared_files_tab),
        #panel{style = <<"margin-top: 59px;">>, body = main_panel()},
        footer_popup()
    ].


% Main table   
main_panel() ->
    fslogic_context:set_user_dn(vcn_gui_utils:get_user_dn()),
    ShareEntries = lists:foldl(
        fun(#veil_document{uuid = UUID, record = #share_desc{file = FileID}}, Acc) ->
            case logical_files_manager:get_file_user_dependent_name_by_uuid(FileID) of
                {ok, FilePath} ->
                    Filename = filename:basename(FilePath),
                    AddressPrefix = <<"https://", (gui_ctx:get_requested_hostname())/binary, ?shared_files_download_path>>,
                    Acc ++ [{
                        <<"~/", (gui_str:unicode_list_to_binary(FilePath))/binary>>,
                        gui_str:unicode_list_to_binary(Filename),
                        AddressPrefix,
                        gui_str:unicode_list_to_binary(UUID)
                    }];
                _ ->
                    Acc
            end
        end, [], get_shared_files()),

    TableRows = lists:map(
        fun({LinkText, Filename, AddressPrefix, UUID}) ->
            _TableRow = #tr{cells = [
                #td{body = #span{class = <<"table-cell">>, body = [
                    #panel{style = <<"display: inline-block; vertical-align: middle;">>, body = [
                        #image{class = <<"list-icon">>, image = <<"/images/file32.png">>}
                    ]},
                    #panel{style = <<"word-wrap: break-word; display: inline-block;vertical-align: middle;">>,
                        class = <<"filename_row">>, body = [
                            #link{body = gui_str:html_encode(LinkText), target = <<"_blank">>, url = <<AddressPrefix/binary, UUID/binary>>}
                        ]}
                ]}},
                #td{style = <<"width: 80px;">>, body = #span{class = <<"table-cell">>, body = [
                    #panel{style = <<"margin: 5px 0; display: inline-block; vertical-align: middle;">>, body = [
                        #link{class = <<"glyph-link">>, style = <<"margin-right: 25px;">>,
                            postback = {action, show_link, [UUID]}, body = #span{class = <<"fui-link">>,
                                style = <<"font-size: 24px; margin: -4px 0px 0px; position: relative; top: 4px;">>}},
                        #link{class = <<"glyph-link">>, postback = {action, remove_link_prompt, [UUID, Filename]},
                            body = #span{class = <<"fui-cross">>, style = <<"font-size: 24px;",
                            "margin: -4px 0px 0px; position: relative; top: 4px;">>}}
                    ]}
                ]}}
            ]}
        end, lists:usort(ShareEntries)),  % Sort link names alphabetically

    PanelBody = case TableRows of
                    [] ->
                        #p{style = <<"padding: 15px;">>, body = <<"No shared files">>};
                    _ ->
                        #table{id = <<"main_table">>, class = <<"table table-stripped">>, style = <<"border-radius: 0; margin-bottom: 0;">>,
                            body = TableRows}
                end,
    gui_jq:wire(<<"window.onresize = function(e) { $('.filename_row').css('max-width', '' +",
    "($(window).width() - 250) + 'px'); }; $(window).resize();">>), % 240 is size of button cell + paddings.
    PanelBody.


% Get list of user's shared files from database
get_shared_files() ->
    {ok, #veil_document{uuid = UUID}} = user_logic:get_user({login, gui_ctx:get_user_id()}),
    _ShareList = case logical_files_manager:get_share({user, UUID}) of
                     {ok, List} when is_list(List) -> List;
                     {ok, Doc} -> [Doc];
                     _ -> []
                 end.


% Footer popup panel
footer_popup() ->
    #panel{class = <<"dialog success-dialog wide hidden">>, id = <<"footer_popup">>,
        style = <<"position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0; width: 100%;">>, body = []}.


% Handle postback event
api_event("escape_pressed", _, _) ->
    event({action, hide_popup}).


event(init) ->
    ok;

event({action, Fun}) ->
    event({action, Fun, []});

event({action, Fun, Args}) ->
    vcn_gui_utils:apply_or_redirect(?MODULE, Fun, Args, true);

event(terminate) ->
    ok.


% Display link to file in popup panel
show_link(ShareID) ->
    AddressPrefix = <<"https://", (gui_ctx:get_requested_hostname())/binary, ?shared_files_download_path>>,
    Body = [
        #link{postback = {action, hide_popup}, title = <<"Hide">>, class = <<"glyph-link">>,
            style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
            body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
        #form{class = <<"control-group">>, body = [
            #textbox{id = <<"shared_link_textbox">>, class = <<"flat">>, style = <<"width: 700px;">>,
                value = <<AddressPrefix/binary, ShareID/binary>>, placeholder = <<"Download link">>},
            #button{id = <<"shared_link_submit">>, postback = {action, hide_popup},
                class = <<"btn btn-success btn-wide">>, body = <<"Ok">>}
        ]}
    ],
    gui_jq:update(<<"footer_popup">>, Body),
    gui_jq:remove_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:slide_down(<<"footer_popup">>, 200),
    gui_jq:select_text(<<"shared_link_textbox">>),
    gui_jq:bind_enter_to_submit_button(<<"shared_link_textbox">>, <<"shared_link_submit">>).


% Display removal prompt in popup panel
remove_link_prompt(ShareID, Filename) ->
    Body =
        [
            #link{postback = {action, hide_popup}, title = <<"Hide">>, class = <<"glyph-link">>,
                style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
                body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
            #form{class = <<"control-group">>, body = [
                #p{body = <<"Remove share for <b>", (gui_str:html_encode(Filename))/binary, "</b>?">>},
                #button{id = <<"ok_button">>, class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, postback = {action, remove_link, [ShareID]}},
                #button{class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>, postback = {action, hide_popup}}
            ]}
        ],
    gui_jq:update(<<"footer_popup">>, Body),
    gui_jq:remove_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:slide_down(<<"footer_popup">>, 200),
    gui_jq:focus(<<"ok_button">>).


% Actually remove a link
remove_link(ShareID) ->
    ok = logical_files_manager:remove_share({uuid, gui_str:binary_to_unicode_list(ShareID)}),
    gui_jq:replace(<<"main_table">>, main_panel()),
    hide_popup().


% Hide popup panel 
hide_popup() ->
    gui_jq:update(<<"footer_popup">>, []),
    gui_jq:add_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:slide_up(<<"footer_popup">>, 200).

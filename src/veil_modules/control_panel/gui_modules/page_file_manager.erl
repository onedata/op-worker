%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page is a file manager providing basic functionalities.
%% @end
%% ===================================================================

-module(page_file_manager).
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

% n2o API
-export([main/0, event/1, api_event/3]).
% Postback functions and other
-export([get_requested_hostname/0, comet_loop/1]).
-export([clear_manager/0, clear_workspace/0, sort_toggle/1, sort_reverse/0, navigate/1, up_one_level/0]).
-export([toggle_view/1, select_item/1, select_all/0, deselect_all/0, clear_clipboard/0, put_to_clipboard/1, paste_from_clipboard/0]).
-export([rename_item/2, create_directory/1, remove_selected/0, search/1, show_popup/1, hide_popup/0, path_navigator_body/1]).
-export([fs_mkdir/1, fs_remove/1, fs_remove_dir/1, fs_mv/2, fs_mv/3, fs_copy/2, fs_unique_filename/2, fs_create_share/1]).


% How often should comet process check for changes in current dir
-define(AUTOREFRESH_PERIOD, 1000).


% Item is either a file or a dir represented in manager
-record(item, {
    id = <<"">>,
    path = "/",
    is_shared = false,
    attr = #fileattributes{}}).


%% Check if user is logged in and has dn defined.
main() ->
    case vcn_gui_utils:maybe_redirect(true, true, true, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, custom()}]}
    end.


%% Page title
title() -> <<"File manager">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    gui_jq:register_escape_event("escape_pressed"),
    Body = [
        #panel{id = <<"spinner">>, style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>, body = [
            #image{image = <<"/images/spinner.gif">>}
        ]},
        vcn_gui_utils:top_menu(file_manager_tab, manager_submenu()),
        manager_workspace(),
        footer_popup()
    ],
    Body.

%% This will be placed in the template instead of {{custom}} tag
custom() ->
    <<"<script src='/js/veil_upload.js' type='text/javascript' charset='utf-8'></script>">>.

% Submenu that will be glued below the top menu
manager_submenu() ->
    [
        #panel{class = <<"navbar-inner">>, style = <<"padding-top: 10px;">>, body = [
            #panel{class = <<"container">>, style = <<"position: relative; overflow: hidden;">>, body = [
                #list{class = <<"nav">>, body =
                tool_button_and_dummy(<<"tb_up_one_level">>, <<"Up one level">>, <<"padding: 10px 7px 10px 15px;">>,
                    <<"fui-arrow-left">>, {action, up_one_level})},
                #panel{class = <<"breadcrumb-text breadcrumb-background">>, style = <<"overflow: hidden; margin-left: 15px;">>, body = [
                    #p{id = <<"path_navigator">>, class = <<"breadcrumb-content">>, body = <<"~/">>}
                ]},
                #panel{class = <<"control-group">>, style = <<"position: absolute; right: 15px; top: 0;">>, body = [
                    #panel{class = <<"input-append">>, style = <<"; margin-bottom: 0px;">>, body = [
                        #textbox{id = wire_enter(<<"search_textbox">>, <<"search_button">>), class = <<"span2">>,
                            style = <<"width: 220px;">>, placeholder = <<"Search">>},
                        #panel{class = <<"btn-group">>, body = [
                            #button{id = wire_click(<<"search_button">>, {action, search, [{query_value, <<"search_textbox">>}]}, <<"search_textbox">>),
                                class = <<"btn">>, type = <<"button">>, body = #span{class = <<"fui-search">>}}
                        ]}
                    ]}
                ]}
            ]}
        ]},
        #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>, body = [
            #panel{class = <<"container">>, body = [
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button(<<"tb_create_dir">>, <<"Create directory">>, <<"padding: 18px 14px;">>,
                    <<"fui-folder">>, {action, show_popup, [create_directory]}) ++
                    tool_button(<<"tb_upload_files">>, <<"Upload file(s)">>, <<"padding: 18px 14px;">>,
                        <<"fui-plus-inverted">>, {action, show_popup, [file_upload]}) ++
                    tool_button_and_dummy(<<"tb_share_file">>, <<"Share">>, <<"padding: 18px 14px;">>,
                        <<"fui-link">>, {action, show_popup, [share_file]})

                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy(<<"tb_rename">>, <<"Rename">>, <<"padding: 18px 14px;">>,
                    <<"fui-new">>, {action, show_popup, [rename_item]}) ++
                tool_button_and_dummy(<<"tb_remove">>, <<"Remove">>, <<"padding: 18px 14px;">>,
                    <<"fui-trash">>, {action, show_popup, [remove_selected]})
                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy(<<"tb_cut">>, <<"Cut">>, <<"padding: 18px 14px;">>,
                    <<"fui-window">>, {action, put_to_clipboard, [cut]}) ++
                %tool_button_and_dummy(<<"tb_copy">>, <<"Copy">>, <<"padding: 18px 14px;">>,
                %    <<"fui-windows">>, {action, put_to_clipboard, [copy]}) ++

                [#li{id = wire_click(<<"tb_paste">>, {action, paste_from_clipboard}), body = #link{title = <<"Paste">>, style = <<"padding: 18px 14px;">>,
                    body = #span{class = <<"fui-upload">>, body = #span{id = <<"clipboard_size_label">>, class = <<"iconbar-unread">>,
                        style = <<"right: -12px; top: -10px; background-color: rgb(26, 188, 156);">>,
                        body = <<"0">>}}}},
                    #li{id = <<"tb_paste_dummy">>, class = <<"disabled hidden">>, body = #link{title = <<"Paste">>, style = <<"padding: 18px 14px;">>,
                        body = #span{style = <<"color: rgb(200, 200, 200);">>, class = <<"fui-upload">>}}}]
                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy(<<"tb_select_all">>, <<"Select all">>, <<"padding: 18px 14px;">>,
                    <<"fui-checkbox-checked">>, {action, select_all}) ++
                tool_button_and_dummy(<<"tb_deselect_all">>, <<"Deselect all">>, <<"padding: 18px 14px;">>,
                    <<"fui-checkbox-unchecked">>, {action, deselect_all})
                },

                #panel{class = <<"btn-toolbar pull-right no-margin">>, style = <<"padding: 12px 15px; overflow: hidden;">>, body = [
                    #panel{class = <<"btn-group no-margin">>, body = [
                        #link{id = wire_click(<<"list_view_button">>, {action, toggle_view, [list]}),
                            title = <<"List view">>, class = <<"btn btn-small btn-inverse">>,
                            body = #span{class = <<"fui-list-columned">>}},
                        #link{id = wire_click(<<"grid_view_button">>, {action, toggle_view, [grid]}),
                            title = <<"Grid view">>, class = <<"btn btn-small btn-inverse">>,
                            body = #span{class = <<"fui-list-small-thumbnails">>}}
                    ]}
                ]},

                #panel{class = <<"btn-group pull-right">>, style = <<"margin: 12px 15px">>, body = [
                    <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
                    #button{id = wire_click(<<"button_sort_reverse">>, {action, sort_reverse}), title = <<"Reverse sorting">>,
                        class = <<"btn btn-inverse btn-small">>, body = <<"Sort">>},
                    #button{title = <<"Sort by">>, class = <<"btn btn-inverse btn-small dropdown-toggle">>,
                        data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = #span{class = <<"caret">>}},
                    #list{id = <<"sort_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, body = []}
                ]}
            ]}
        ]}
    ].


% Working space of the explorer.
manager_workspace() ->
    #panel{id = <<"manager_workspace">>, style = <<"z-index: -1; margin: 170px 0 20px 0; overflow: hidden">>, body = []}.


% Footer popup to display prompts and forms.
footer_popup() ->
    #panel{id = <<"footer_popup">>, class = <<"dialog success-dialog wide hidden">>,
        style = <<"z-index: 2; position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0px; width: 100%;">>, body = []
    }.


% Emits a button, properly wired (postback)
tool_button(ID, Title, Style, Icon, Postback) ->
    [
        #li{id = wire_click(ID, Postback), body = #link{title = Title, style = Style,
            body = #span{class = Icon}}}
    ].


% Emits a button, properly wired (postback) + its disabled clone
tool_button_and_dummy(ID, Title, Style, Icon, Postback) ->
    tool_button(ID, Title, Style, Icon, Postback) ++
    [
        #li{id = <<ID/binary, "_dummy">>, class = <<"disabled hidden">>, body = #link{title = Title, style = Style,
            body = #span{style = <<"color: rgb(200, 200, 200);">>, class = Icon}}}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Wiring postbacks. Thanks to this wrapper every time a postback is initiated,
%% there will be spinner showing up in 150ms. It gets hidden when reply is received.
wire_click(ID, Tag) ->
    gui_jq:wire(gui_jq:postback_action(ID, Tag)),
    gui_jq:bind_element_click(ID, <<"function(e) { $('#spinner').delay(150).show(); }">>),
    ID.

wire_click(ID, Tag, Source) ->
    gui_jq:wire(gui_jq:form_submit_action(ID, Tag, Source)),
    gui_jq:bind_element_click(ID, <<"function(e) { $('#spinner').delay(150).show(); }">>),
    ID.

wire_enter(ID, ButtonToClickID) ->
    % No need to show the spinner, as this only performs a click on a submit button
    gui_jq:bind_enter_to_submit_button(ID, ButtonToClickID),
    ID.


%% Handling events
api_event("escape_pressed", _, _) ->
    event({action, hide_popup}).


event(init) ->
    case gui_ctx:user_logged_in() and vcn_gui_utils:dn_and_storage_defined() of
        false ->
            skip;
        true ->
            UserID = vcn_gui_utils:get_user_dn(),
            Hostname = gui_ctx:get_requested_hostname(),
            {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(UserID, Hostname) end),
            put(comet_pid, Pid)
    end;


event(terminate) ->
    ok;


event({action, Fun}) ->
    event({action, Fun, []});


event({action, Fun, Args}) ->
    NewArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {query_value, FieldName} ->
                    % This tuple means, that element with id=FieldName has to be queried
                    % and the result be put in function args
                    gui_str:to_list(gui_ctx:form_param(FieldName));
                Other ->
                    Other
            end
        end, Args),
    vcn_gui_utils:apply_or_redirect(erlang, send, [get(comet_pid), {action, Fun, NewArgs}], true).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Comet loop and functions evaluated by comet
comet_loop_init(UserId, RequestedHostname) ->
    % Initialize page state
    fslogic_context:set_user_dn(UserId),
    set_requested_hostname(RequestedHostname),
    set_working_directory("/"),
    set_selected_items([]),
    set_display_style(list),
    set_displayed_file_attributes([size, atime, mtime]),
    set_sort_by(name),
    set_sort_ascending(true),
    set_item_counter(1),
    set_item_list(fs_list_dir(get_working_directory())),
    set_item_list_rev(item_list_md5(get_item_list())),
    set_clipboard_items([]),
    set_clipboard_type(none),
    refresh_workspace(),
    gui_jq:hide(<<"spinner">>),
    gui_comet:flush(),

    % Enter comet loop for event processing and autorefreshing
    comet_loop(false).


comet_loop(IsUploadInProgress) ->
    try
        receive
            {action, Fun, Args} ->
                case IsUploadInProgress of
                    true ->
                        gui_jq:wire(#alert{text = <<"Please wait for the upload to finish.">>}), gui_comet:flush();
                    false ->
                        erlang:apply(?MODULE, Fun, Args)
                end,
                gui_jq:hide(<<"spinner">>),
                gui_comet:flush(),
                ?MODULE:comet_loop(IsUploadInProgress);
            upload_started ->
                ?MODULE:comet_loop(true);
            upload_finished ->
                ?MODULE:comet_loop(false);
            Other ->
                ?debug("Unrecognized comet message in page_file_manager: ~p", [Other]),
                ?MODULE:comet_loop(IsUploadInProgress)

        after ?AUTOREFRESH_PERIOD ->
            % Refresh file list if it has changed
            CurrentItemList = fs_list_dir(get_working_directory()),
            CurrentMD5 = item_list_md5(CurrentItemList),
            case get_item_list_rev() of
                CurrentMD5 ->
                    skip;
                _ ->
                    set_item_list(CurrentItemList),
                    set_item_list_rev(CurrentMD5),
                    refresh_workspace(),
                    gui_comet:flush()
            end,
            ?MODULE:comet_loop(IsUploadInProgress)
        end

    catch Type:Message ->
        ?error_stacktrace("Error in file_manager comet_loop - ~p:~p", [Type, Message]),
        page_error:redirect_with_error(?error_internal_server_error),
        gui_comet:flush()
    end.


%%%%%%%%%%%%%%%
%% Event handling
clear_manager() ->
    hide_popup(),
    gui_jq:update(<<"path_navigator">>, path_navigator_body(get_working_directory())),
    clear_workspace().


clear_workspace() ->
    set_item_list(fs_list_dir(get_working_directory())),
    set_item_list_rev(item_list_md5(get_item_list())),
    refresh_workspace().


refresh_workspace() ->
    set_selected_items([]),
    refresh_tool_buttons(),
    sort_item_list(),
    NewBody = case get_display_style() of
                  list -> list_view_body();
                  grid -> grid_view_body()
              end,
    gui_jq:update(<<"manager_workspace">>, NewBody).


sort_item_list() ->
    AllItems = get_item_list(),
    {ItemList, GroupsDir} = lists:partition(
        fun(I) ->
            item_path(I) /= "/groups"
        end, AllItems),
    Attr = get_sort_by(),
    SortAscending = get_sort_ascending(),
    SortedItems = lists:sort(
        fun(Item1, Item2) ->
            {V1, V2} = case item_attr(Attr, Item1) of
                           L when is_list(L) ->
                               {string:to_lower(item_attr(Attr, Item1)),
                                   string:to_lower(item_attr(Attr, Item2))};
                           _ ->
                               {item_attr(Attr, Item1), item_attr(Attr, Item2)}
                       end,
            if
                V1 < V2 -> true;
                V1 > V2 -> false;
                V1 =:= V2 -> item_attr(name, Item1) =< item_attr(name, Item2)
            end
        end, ItemList),
    Result = case SortAscending of
                 true -> SortedItems;
                 false -> lists:reverse(SortedItems)
             end,
    FinalResult = case Attr of
                      name ->
                          {Dirs, Files} = lists:partition(fun(I) -> item_is_dir(I) end, Result),
                          Dirs ++ Files;
                      _ -> Result
                  end,
    set_item_list(GroupsDir ++ FinalResult).


sort_toggle(Type) ->
    case get_sort_by() of
        Type -> sort_reverse();
        _ ->
            set_sort_by(Type),
            set_sort_ascending(true),
            refresh_workspace()
    end.


sort_reverse() ->
    set_sort_ascending(not get_sort_ascending()),
    refresh_workspace().


refresh_tool_buttons() ->
    % View toggling buttons
    {EnableID, DisableID} = case get_display_style() of
                                list -> {<<"list_view_button">>, <<"grid_view_button">>};
                                grid -> {<<"grid_view_button">>, <<"list_view_button">>}
                            end,
    gui_jq:add_class(EnableID, <<"active">>),
    gui_jq:remove_class(DisableID, <<"active">>),

    % Sort dropdown
    DropdownBody = case get_display_style() of
                       grid ->
                           #li{id = wire_click(<<"grid_sort_by_name">>, {action, sort_toggle, [name]}),
                               class = <<"active">>, body = #link{body = <<"Name">>}};
                       list ->
                           lists:foldl(
                               fun(Attr, Acc) ->
                                   Class = case get_sort_by() of
                                               Attr -> <<"active">>;
                                               _ -> <<"">>
                                           end,
                                   Acc ++ [#li{id = wire_click(<<"list_sort_by_", (atom_to_binary(Attr, latin1))/binary>>, {action, sort_toggle, [Attr]}),
                                       class = Class, body = #link{body = attr_to_name(Attr)}}]
                               end, [], [name | get_displayed_file_attributes()])
                   end,
    gui_jq:update(<<"sort_dropdown">>, DropdownBody),

    Count = length(get_selected_items()),
    NFiles = length(get_item_list()),
    IsDir = try item_is_dir(item_find(lists:nth(1, get_selected_items()))) catch _:_ -> false end,
    enable_tool_button(<<"tb_up_one_level">>, get_working_directory() /= "/"),
    enable_tool_button(<<"tb_share_file">>, (Count =:= 1) andalso (not IsDir)),
    enable_tool_button(<<"tb_rename">>, Count =:= 1),
    enable_tool_button(<<"tb_remove">>, Count > 0),
    enable_tool_button(<<"tb_cut">>, Count > 0),
    enable_tool_button(<<"tb_copy">>, false),
    enable_tool_button(<<"tb_paste">>, length(get_clipboard_items()) > 0),
    gui_jq:update(<<"clipboard_size_label">>, integer_to_binary(length(get_clipboard_items()))),
    enable_tool_button(<<"tb_select_all">>, Count < NFiles),
    enable_tool_button(<<"tb_deselect_all">>, Count > 0).


enable_tool_button(ID, Flag) ->
    case Flag of
        true ->
            gui_jq:remove_class(ID, <<"hidden">>),
            gui_jq:add_class(<<ID/binary, "_dummy">>, <<"hidden">>),
            gui_jq:show(ID),
            gui_jq:hide(<<ID/binary, "_dummy">>);
        false ->
            gui_jq:add_class(ID, <<"hidden">>),
            gui_jq:remove_class(<<ID/binary, "_dummy">>, <<"hidden">>),
            gui_jq:hide(ID),
            gui_jq:show(<<ID/binary, "_dummy">>)
    end.


navigate(Path) ->
    set_working_directory(Path),
    clear_manager().


up_one_level() ->
    navigate(filename:dirname(filename:absname(get_working_directory()))).


toggle_view(Type) ->
    set_display_style(Type),
    set_sort_by(name),
    set_sort_ascending(true),
    clear_workspace().


select_item(Path) ->
    case item_find(Path) of
        undefined ->
            skip;
        Item ->
            SelectedItems = get_selected_items(),
            case lists:member(Path, SelectedItems) of
                false ->
                    set_selected_items(SelectedItems ++ [Path]),
                    gui_jq:add_class(item_id(Item), <<"selected-item">>);
                true ->
                    set_selected_items(SelectedItems -- [Path]),
                    gui_jq:remove_class(item_id(Item), <<"selected-item">>)
            end
    end,
    refresh_tool_buttons().


select_all() ->
    set_selected_items([]),
    lists:foreach(
        fun(Item) ->
            set_selected_items(get_selected_items() ++ [item_path(Item)]),
            gui_jq:add_class(item_id(Item), <<"selected-item">>)
        end, get_item_list()),
    refresh_tool_buttons().


deselect_all() ->
    lists:foreach(
        fun(Item) ->
            gui_jq:remove_class(item_id(Item), <<"selected-item">>)
        end, get_item_list()),
    set_selected_items([]),
    refresh_tool_buttons().


clear_clipboard() ->
    set_clipboard_items([]),
    set_clipboard_type(none).


put_to_clipboard(Type) ->
    SelectedItems = get_selected_items(),
    set_clipboard_type(Type),
    set_clipboard_items(SelectedItems),
    clear_workspace().


paste_from_clipboard() ->
    ErrorMessage = lists:foldl(
        fun(Path, Acc) ->
            case get_clipboard_type() of
                cut ->
                    case fs_mv(Path, get_working_directory()) of
                        ok ->
                            Acc;
                        {logical_file_system_error, "eperm"} ->
                            <<Acc/binary, "Unable to move ", (gui_str:to_binary(filename:basename(Path)))/binary, " - insufficient permissions.\r\n">>;
                        {logical_file_system_error, "eexist"} ->
                            <<Acc/binary, "Unable to move ", (gui_str:to_binary(filename:basename(Path)))/binary, " - file exists.\r\n">>;
                        _ ->
                            <<Acc/binary, "Unable to move ", (gui_str:to_binary(filename:basename(Path)))/binary, " - unknown error.\r\n">>
                    end;
                copy ->
                    % Not yet implemented
                    fs_copy(Path, get_working_directory()),
                    Acc
            end
        end, <<"">>, get_clipboard_items()),
    case ErrorMessage of
        <<"">> ->
            ok;
        _ ->
            gui_jq:wire(#alert{text = ErrorMessage})
    end,
    clear_clipboard(),
    clear_workspace().


rename_item(OldPath, NewName) ->
    OldName = filename:basename(OldPath),
    case NewName of
        [] -> hide_popup();
        undefined -> hide_popup();
        OldName -> hide_popup();
        _ ->
            NewPath = filename:absname(NewName, get_working_directory()),
            case fs_mv(OldPath, get_working_directory(), NewName) of
                ok ->
                    clear_clipboard(),
                    clear_manager(),
                    select_item(NewPath);
                {logical_file_system_error, "eperm"} ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - insufficient permissions.">>});
                {logical_file_system_error, "eexist"} ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - file exists.">>});
                _ ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - unknown error.">>})
            end
    end.


create_directory(Name) ->
    case Name of
        [] -> hide_popup();
        undefined -> hide_popup();
        _ ->
            FullPath = filename:absname(Name, get_working_directory()),
            case fs_mkdir(FullPath) of
                ok ->
                    clear_manager(),
                    select_item(FullPath);
                _ ->
                    case item_find(FullPath) of
                        undefined ->
                            gui_jq:wire(#alert{text = <<"Cannot create directory - disallowed name.">>});
                        _ ->
                            gui_jq:wire(#alert{text = <<"Cannot create directory - file exists.">>})
                    end,
                    hide_popup()
            end
    end.


remove_selected() ->
    SelectedItems = get_selected_items(),
    lists:foreach(
        fun(Path) ->
            fs_remove(Path)
        end, SelectedItems),
    clear_clipboard(),
    clear_manager().


search(SearchString) ->
    case SearchString of
        [] -> skip;
        undefined -> skip;
        _ ->
            deselect_all(),
            lists:foreach(
                fun(Item) ->
                    case string:str(item_basename(Item), SearchString) of
                        0 -> skip;
                        _ -> select_item(item_path(Item))
                    end
                end, get_item_list())
    end.


% Shows popup with a prompt, form, etc.
show_popup(Type) ->
    {FooterBody, Script, CloseButtonAction} =
        case Type of
            create_directory ->
                Body = [
                    #p{body = <<"Create directory">>},
                    #form{class = <<"control-group">>, body = [
                        #textbox{id = wire_enter(<<"create_dir_textbox">>, <<"create_dir_submit">>), class = <<"flat">>,
                            style = <<"width: 350px;">>, placeholder = <<"New directory name">>},
                        #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>,
                            id = wire_click(<<"create_dir_submit">>,
                                {action, create_directory, [{query_value, <<"create_dir_textbox">>}]},
                                <<"create_dir_textbox">>)}
                    ]}
                ],
                {Body, <<"$('#create_dir_textbox').focus();">>, {action, hide_popup}};

            rename_item ->
                Filename = filename:basename(lists:nth(1, get_selected_items())),
                SelectionLength = case string:rchr(Filename, $.) - 1 of
                                      -1 -> length(Filename);
                                      Val -> Val
                                  end,
                OldLocation = lists:nth(1, get_selected_items()),
                Body = [
                    #p{body = <<"Rename <b>", (gui_str:html_encode(Filename))/binary, "</b>">>},
                    #form{class = <<"control-group">>, body = [
                        #textbox{id = wire_enter(<<"new_name_textbox">>, <<"new_name_submit">>), class = <<"flat">>,
                            style = <<"width: 350px;">>, placeholder = <<"New name">>, value = gui_str:html_encode(Filename)},

                        #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>,
                            id = wire_click(<<"new_name_submit">>,
                                {action, rename_item, [OldLocation, {query_value, <<"new_name_textbox">>}]},
                                <<"new_name_textbox">>)}
                    ]}
                ],

                FocusScript = <<"setTimeout(function() { ",
                "document.getElementById('new_name_textbox').focus(); ",
                "if( $('#new_name_textbox').createTextRange ) { ",
                "var selRange = $('#new_name_textbox').createTextRange(); ",
                "selRange.collapse(true); ",
                "selRange.moveStart('character', 0); ",
                "selRange.moveEnd('character', ", (integer_to_binary(SelectionLength))/binary, "); ",
                "selRange.select(); ",
                "} else if( document.getElementById('new_name_textbox').setSelectionRange ) { ",
                "document.getElementById('new_name_textbox').setSelectionRange(0, ", (integer_to_binary(SelectionLength))/binary, "); ",
                "} else if( $('#new_name_textbox').selectionStart ) { ",
                "$('#new_name_textbox').selectionStart = 0; ",
                "$('#new_name_textbox').selectionEnd = ", (integer_to_binary(SelectionLength))/binary, "; ",
                "} }, 1); ">>,

                {Body, FocusScript, {action, hide_popup}};

            share_file ->
                Path = lists:nth(1, get_selected_items()),
                Filename = filename:basename(lists:nth(1, get_selected_items())),
                {Status, ShareID} = case fs_get_share_by_filepath(Path) of
                                        {ok, #veil_document{uuid = UUID}} ->
                                            {exists, gui_str:to_binary(UUID)};
                                        _ ->
                                            {ok, ID} = fs_create_share(Path),
                                            {new, gui_str:to_binary(ID)}
                                    end,
                clear_workspace(),
                select_item(Path),
                AddressPrefix = <<"https://", (get_requested_hostname())/binary, ?shared_files_download_path>>,
                Body = [
                    case Status of
                        exists ->
                            #p{body = <<"<b>", (gui_str:html_encode(Filename))/binary,
                            "</b> is already shared. Visit <b>Shared files</b> tab for more.">>};
                        new ->
                            #p{body = <<"<b>", (gui_str:html_encode(Filename))/binary,
                            "</b> successfully shared. Visit <b>Shared files</b> tab for more.">>}
                    end,
                    #form{class = <<"control-group">>, body = [
                        #textbox{id = wire_enter(<<"shared_link_textbox">>, <<"shared_link_submit">>), class = "flat", style = "width: 700px;",
                            value = gui_str:html_encode(<<AddressPrefix/binary, ShareID/binary>>), placeholder = "Download link"},
                        #button{id = wire_click(<<"shared_link_submit">>, {action, hide_popup}),
                            class = <<"btn btn-success btn-wide">>, body = <<"Ok">>}
                    ]}
                ],
                {Body, <<"$('#shared_link_textbox').focus(); $('#shared_link_textbox').select();">>, {action, hide_popup}};

            file_upload ->
                Body = [
                    #veil_upload{subscriber_pid = self(), target_dir = get_working_directory()}
                ],
                {Body, undefined, {action, clear_manager}};

            remove_selected ->
                {_FB, _S, _A} =
                    case get_selected_items() of
                        [] -> {[], undefined, undefined};
                        Paths ->
                            {NumFiles, NumDirs} = lists:foldl(
                                fun(Path, {NFiles, NDirs}) ->
                                    case item_is_dir(item_find(Path)) of
                                        true -> {NFiles, NDirs + 1};
                                        false -> {NFiles + 1, NDirs}
                                    end
                                end, {0, 0}, Paths),
                            FilesString = if
                                              (NumFiles =:= 1) ->
                                                  <<"<b>", (integer_to_binary(NumFiles))/binary, " file</b>">>;
                                              (NumFiles > 1) ->
                                                  <<"<b>", (integer_to_binary(NumFiles))/binary, " files</b>">>;
                                              true -> <<"">>
                                          end,
                            DirsString = if
                                             (NumDirs =:= 1) ->
                                                 <<"<b>", (integer_to_binary(NumDirs))/binary, " directory</b> and all its content">>;
                                             (NumDirs > 1) ->
                                                 <<"<b>", (integer_to_binary(NumDirs))/binary, " directories</b> and all their content">>;
                                             true -> <<"">>
                                         end,
                            Punctuation = if
                                              (FilesString /= <<"">>) and (DirsString /= <<"">>) ->
                                                  <<", ">>;
                                              true -> <<"">>
                                          end,
                            Body = [
                                #p{body = <<"Remove ", FilesString/binary, Punctuation/binary, DirsString/binary, "?">>},
                                #form{class = <<"control-group">>, body = [
                                    #button{id = wire_click(<<"ok_button">>, {action, remove_selected}),
                                        class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                                    #button{id = wire_click(<<"cancel_button">>, {action, hide_popup}),
                                        class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>}
                                ]}
                            ],
                            {Body, <<"$('#ok_button').focus();">>, {action, hide_popup}}
                    end;

            _ ->
                {[], undefined, undefined}
        end,
    case FooterBody of
        [] -> skip;
        _ ->
            CloseButton = #link{id = wire_click(<<"close_button">>, CloseButtonAction), title = <<"Hide">>, class = <<"glyph-link">>,
                style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
                body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
            gui_jq:update(<<"footer_popup">>, [CloseButton | FooterBody]),
            gui_jq:remove_class(<<"footer_popup">>, <<"hidden">>),
            gui_jq:slide_down(<<"footer_popup">>, 200)
    end,
    case Script of
        undefined ->
            ok;
        _ ->
            gui_jq:wire(Script, false)
    end.


% Hides the footer popup
hide_popup() ->
    gui_jq:update(<<"footer_popup">>, []),
    gui_jq:add_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:slide_up(<<"footer_popup">>, 200).


% Render path navigator
path_navigator_body(WorkingDirectory) ->
    case WorkingDirectory of
        "/" -> gui_str:format("~~", []);
        _ ->
            FirstLink = #link{id = wire_click(<<"nav_top">>, {action, navigate, ["/"]}), body = <<"~">>},
            PathElements = string:tokens(WorkingDirectory, "/"),
            {LinkList, _} = lists:mapfoldl(
                fun(Element, {CurrentPath, Counter}) ->
                    PathToElement = CurrentPath ++ "/" ++ Element,
                    Link = #link{id = wire_click(<<"nav_", (integer_to_binary(Counter))/binary>>, {action, navigate, [PathToElement]}),
                        body = gui_str:html_encode(Element)},
                    {Link, {PathToElement, Counter + 1}}
                end, {"/", 1}, lists:sublist(PathElements, length(PathElements) - 1)),
            [FirstLink | LinkList] ++ [lists:last(PathElements)]
    end.


% Render grid view workspace
grid_view_body() ->
    {Tiles, _} = lists:mapfoldl(
        fun(Item, Counter) ->
            FullPath = item_path(Item),
            Filename = gui_str:to_binary(item_basename(Item)),
            ImageStyle = case get_clipboard_type() of
                             cut ->
                                 case lists:member(FullPath, get_clipboard_items()) of
                                     true -> <<"opacity:0.3; filter:alpha(opacity=30);">>;
                                     _ -> <<"">>
                                 end;
                             _ -> <<"">>
                         end,

            ImageUrl = case item_is_dir(Item) of
                           true ->
                               case is_group_dir(FullPath) of
                                   true -> <<"/images/folder_groups64.png">>;
                                   false -> <<"/images/folder64.png">>
                               end;
                           false ->
                               <<"/images/file64.png">>
                       end,

            LinkID = <<"grid_item_", (integer_to_binary(Counter))/binary>>,
            % Item won't hightlight if the link is clicked.
            gui_jq:bind_element_click(LinkID, <<"function(e) { e.stopPropagation(); }">>),
            Tile = #panel{
                id = wire_click(item_id(Item), {action, select_item, [FullPath]}),
                style = <<"width: 100px; height: 116px; overflow:hidden; position: relative; margin: 0; padding: 5px 10px; display: inline-block;">>,
                body = case item_is_dir(Item) of
                           true ->
                               [
                                   #panel{style = <<"margin: 0 auto; text-align: center;">>, body = [
                                       #image{style = ImageStyle, image = ImageUrl}
                                   ]},
                                   #panel{style = <<"margin: 5px auto 0; text-align: center; word-wrap: break-word;">>, body = [
                                       #link{title = gui_str:html_encode(Filename), id = wire_click(LinkID, {action, navigate, [FullPath]}),
                                           body = gui_str:html_encode(Filename)}
                                   ]}
                               ];
                           false ->
                               ShareIcon = case item_is_shared(Item) of
                                               true ->
                                                   #span{style = <<"font-size: 36px; position: absolute; top: 0px; left: 0; z-index: 1;">>,
                                                       class = <<"fui-link">>};
                                               false -> []
                                           end,
                               [
                                   #panel{style = <<"margin: 0 auto; text-align: center;">>, body = [
                                       #panel{style = <<"display: inline-block; position: relative;">>, body = [
                                           ShareIcon,
                                           #image{style = ImageStyle, image = ImageUrl}
                                       ]}
                                   ]},
                                   #panel{style = <<"margin: 5px auto 0; text-align: center; word-wrap: break-word;">>, body = [
                                       #link{title = gui_str:html_encode(Filename), id = LinkID, body = gui_str:html_encode(Filename), target = <<"_blank">>,
                                           url = <<?user_content_download_path, "/", (gui_str:to_binary(gui_str:url_encode(FullPath)))/binary>>}
                                   ]}
                               ]
                       end
            },
            {Tile, Counter + 1}
        end, 1, get_item_list()),
    Body = case Tiles of
               [] -> #p{style = <<"margin: 15px;">>, body = <<"This directory is empty">>};
               Other -> Other
           end,
    #panel{style = <<"margin-top: 15px;">>, body = Body}.


% Render list view workspace
list_view_body() ->
    NumAttr = erlang:max(1, length(get_displayed_file_attributes())),
    CellWidth = <<"width: ", (integer_to_binary(round(90 * (2 + NumAttr) / NumAttr)))/binary, "px;">>,
    HeaderTable = [
        #table{class = <<"no-margin table">>, style = <<"position: fixed; top: 173px; z-index: 10;",
        "background: white; border: 2px solid #bbbdc0; border-collapse: collapse;">>, body = [
            #tr{cells =
            [
                #th{style = <<"border: 2px solid #aaacae;color: rgb(64, 89, 116);">>, body = <<"Name">>}
            ] ++
            lists:map(
                fun(Attr) ->
                    #th{style = <<"border: 2px solid #aaacae; color: rgb(64, 89, 116); ", CellWidth/binary>>,
                        body = attr_to_name(Attr)}
                end, get_displayed_file_attributes())
            }
        ]}
    ],
    DirUpRow = case get_working_directory() of
                   "/" -> [];
                   Path ->
                       PrevDir = filename:dirname(filename:absname(Path)),
                       Item = item_new(PrevDir),
                       [
                           #tr{cells = [
                               #td{style = <<"vertical-align: middle;">>, body = #span{style = <<"word-wrap: break-word;">>,
                                   class = <<"table-cell">>, body = [
                                       #panel{style = <<"display: inline-block; vertical-align: middle;">>, body = [
                                           #link{id = wire_click(<<"prev_dir_link_image">>, {action, navigate, [PrevDir]}), body = [
                                               #image{class = <<"list-icon">>, image = <<"/images/folder32.png">>}
                                           ]}
                                       ]},
                                       #panel{style = <<"max-width: 230px; word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                           #link{id = wire_click(<<"prev_dir_link_text">>, {action, navigate, [PrevDir]}), body = <<"..&nbsp;&nbsp;&nbsp;">>}
                                       ]}
                                   ]}}] ++
                           lists:map(
                               fun(Attr) ->
                                   #td{style = CellWidth, class = <<"table-cell">>, body = item_attr_value(Attr, Item)}
                               end, get_displayed_file_attributes())
                           }
                       ]
               end,
    {TableRows, _} = lists:mapfoldl(
        fun(Item, Counter) ->
            FullPath = item_path(Item),
            Filename = gui_str:to_binary(item_basename(Item)),
            ImageStyle = case get_clipboard_type() of
                             cut ->
                                 case lists:member(FullPath, get_clipboard_items()) of
                                     true -> <<"opacity:0.3; filter:alpha(opacity=30);">>;
                                     _ -> <<"">>
                                 end;
                             _ -> <<"">>
                         end,

            ImageUrl = case item_is_dir(Item) of
                           true ->
                               case is_group_dir(FullPath) of
                                   true -> <<"/images/folder_groups32.png">>;
                                   false -> <<"/images/folder32.png">>
                               end;
                           false -> <<"/images/file32.png">>
                       end,

            LinkID = <<"list_item_", (integer_to_binary(Counter))/binary>>,
            % Item won't hightlight if the link is clicked.
            gui_jq:bind_element_click(LinkID, <<"function(e) { e.stopPropagation(); }">>),
            ImageID = <<"image_", (integer_to_binary(Counter))/binary>>,
            % Image won't hightlight if the image is clicked.
            gui_jq:bind_element_click(ImageID, <<"function(e) { e.stopPropagation(); }">>),
            TableRow = #tr{
                id = wire_click(item_id(Item), {action, select_item, [FullPath]}),
                cells = [
                    case item_is_dir(Item) of
                        true ->
                            #td{style = <<"vertical-align: middle;">>, body = #span{style = <<"word-wrap: break-word;">>,
                                class = <<"table-cell">>, body = [
                                    #panel{style = <<"display: inline-block; vertical-align: middle;">>, body = [
                                        #link{id = wire_click(ImageID, {action, navigate, [FullPath]}), body =
                                        #image{class = <<"list-icon">>, style = ImageStyle, image = ImageUrl}}
                                    ]},
                                    #panel{class = <<"filename_row">>,
                                        style = <<"max-width: 400px; word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                            #link{id = wire_click(LinkID, {action, navigate, [FullPath]}), body = gui_str:html_encode(Filename)}
                                        ]}
                                ]}};
                        false ->
                            ShareIcon = case item_is_shared(Item) of
                                            true -> #span{class = <<"fui-link">>,
                                                style = <<"font-size: 18px; position: absolute; top: 0px; left: 0; z-index: 1; color: rgb(82, 100, 118);">>};
                                            false -> <<"">>
                                        end,
                            #td{body = #span{class = <<"table-cell">>, body = [
                                #panel{style = <<"display: inline-block; vertical-align: middle; position: relative;">>, body = [
                                    #link{id = ImageID, target = <<"_blank">>,
                                        url = <<?user_content_download_path, "/", (gui_str:url_encode(FullPath))/binary>>, body = [
                                            ShareIcon,
                                            #image{class = <<"list-icon">>, style = ImageStyle, image = ImageUrl}
                                        ]}
                                ]},
                                #panel{class = <<"filename_row">>, style = <<"word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                    #link{id = LinkID, body = gui_str:html_encode(Filename), target = <<"_blank">>,
                                        url = <<?user_content_download_path, "/", (gui_str:to_binary(gui_str:url_encode(FullPath)))/binary>>}
                                ]}
                            ]}}
                    end
                ] ++
                lists:map(
                    fun(Attr) ->
                        #td{style = CellWidth, class = <<"table-cell">>, body = item_attr_value(Attr, Item)}
                    end, get_displayed_file_attributes())
            },
            {TableRow, Counter + 1}
        end, 1, get_item_list()),
    % Set filename containers width
    ContentWithoutFilename = 100 + (51 + round(90 * (2 + NumAttr) / NumAttr)) * NumAttr, % 51 is padding + border
    gui_jq:wire(<<"window.onresize = function(e) { $('.filename_row').css('max-width', ",
    "'' +($(window).width() - ", (integer_to_binary(ContentWithoutFilename))/binary, ") + 'px'); }; $(window).resize();">>),
    [
        HeaderTable,
        #table{id = <<"main_table">>, class = <<"table table-bordered">>,
            style = <<"border-radius: 0; margin-top: 49px; margin-bottom: 0; width: 100%; ">>, body = #tbody{body = DirUpRow ++ TableRows}}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Item manipulation functions
item_new(Dir, File) ->
    FullPath = filename:absname(File, Dir),
    item_new(FullPath).

item_new(FullPath) ->
    FA = fs_get_attributes(FullPath),
    FileAttr = case FA#fileattributes.type of
                   "DIR" -> FA#fileattributes{size = -1};
                   _ -> FA
               end,
    IsShared = case fs_get_share_by_filepath(FullPath) of
                   {ok, _} -> true;
                   _ -> false
               end,
    #item{id = <<"item_", (get_item_counter())/binary>>, path = FullPath, is_shared = IsShared, attr = FileAttr}.

item_find(Path) ->
    case lists:keyfind(Path, 3, get_item_list()) of
        false -> undefined;
        Item -> Item
    end.

item_is_dir(#item{attr = #fileattributes{type = Type}}) ->
    "DIR" =:= Type.

item_is_shared(#item{is_shared = IsShared}) ->
    IsShared.

item_id(#item{id = ID}) ->
    ID.

item_path(#item{path = Path}) ->
    Path.

item_basename(#item{path = Path}) ->
    filename:basename(Path).

item_attr(name, Item) -> item_basename(Item);
item_attr(mode, #item{attr = #fileattributes{mode = Value}}) -> Value;
item_attr(uid, #item{attr = #fileattributes{uid = Value}}) -> Value;
item_attr(gid, #item{attr = #fileattributes{gid = Value}}) -> Value;
item_attr(atime, #item{attr = #fileattributes{atime = Value}}) -> Value;
item_attr(mtime, #item{attr = #fileattributes{mtime = Value}}) -> Value;
item_attr(ctime, #item{attr = #fileattributes{ctime = Value}}) -> Value;
item_attr(type, #item{attr = #fileattributes{type = Value}}) -> Value;
item_attr(size, #item{attr = #fileattributes{size = Value}}) -> Value;
item_attr(uname, #item{attr = #fileattributes{uname = Value}}) -> Value;
item_attr(gname, #item{attr = #fileattributes{gname = Value}}) -> Value.

item_attr_value(name, Item) -> gui_str:to_binary(item_basename(Item));
item_attr_value(mode, Item) -> gui_str:to_binary(lists:flatten(io_lib:format("~.8B", [item_attr(mode, Item)])));
item_attr_value(uname, Item) -> gui_str:to_binary(item_attr(uname, Item));
item_attr_value(atime, Item) -> gui_str:to_binary(time_to_string(item_attr(atime, Item)));
item_attr_value(mtime, Item) -> gui_str:to_binary(time_to_string(item_attr(mtime, Item)));
item_attr_value(ctime, Item) -> gui_str:to_binary(time_to_string(item_attr(ctime, Item)));
item_attr_value(size, Item) ->
    case item_is_dir(Item) of
        true -> <<"">>;
        false -> gui_str:to_binary(size_to_printable(item_attr(size, Item)))
    end.

attr_to_name(name) -> <<"Name">>;
attr_to_name(size) -> <<"Size">>;
attr_to_name(mode) -> <<"Mode">>;
attr_to_name(uname) -> <<"Owner">>;
attr_to_name(atime) -> <<"Access">>;
attr_to_name(mtime) -> <<"Modification">>;
attr_to_name(ctime) -> <<"State change">>.

time_to_string(Time) ->
    Timestamp = {Time div 1000000, Time rem 1000000, 0},
    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Timestamp),
    io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
        [YY, MM, DD, Hour, Min, Sec]).

size_to_printable(Size) ->
    size_to_printable(Size, ["B", "KB", "MB", "GB", "TB"]).

size_to_printable(Size, [Current | Bigger]) ->
    case Size > 1024 of
        true -> size_to_printable(Size / 1024, Bigger);
        false ->
            case is_float(Size) of
                true -> lists:flatten(io_lib:format("~.2f ~s", [Size, Current]));
                false -> lists:flatten(io_lib:format("~B ~s", [Size, Current]))
            end
    end.

item_list_md5(ItemList) ->
    _Hash = lists:foldl(
        fun(#item{path = Path, is_shared = Shared, attr = Attrs}, Acc) ->
            TTB = term_to_binary({Path, Shared, Attrs}),
            erlang:md5(<<TTB/binary, Acc/binary>>)
        end, <<"">>, ItemList).


is_group_dir(Path) ->
    case Path of
        "/groups" -> true;
        "/groups" ++ Rest -> case string:rstr(Rest, "/") of
                                 1 -> true;
                                 _ -> false
                             end;
        _ -> false
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% logical_files_manager interfacing
fs_get_attributes(Path) ->
    {ok, FileAttr} = logical_files_manager:getfileattr(Path),
    FileAttr.


fs_mkdir(Path) ->
    logical_files_manager:mkdir(Path).


fs_remove(Path) ->
    Item = item_new(Path),
    case item_is_dir(Item) of
        true -> fs_remove_dir(Path);
        false -> logical_files_manager:delete(Path)
    end.


fs_remove_dir(DirPath) ->
    case is_group_dir(DirPath) of
        true ->
            skip;
        false ->
            ItemList = fs_list_dir(DirPath),
            lists:foreach(
                fun(Item) ->
                    fs_remove(item_path(Item))
                end, ItemList),
            logical_files_manager:rmdir(DirPath)
    end.


fs_list_dir(Dir) ->
    DirContent = fs_list_dir(Dir, 0, 10, []),
    _ItemList = lists:foldl(
        fun(File, Acc) ->
            try
                Acc ++ [item_new(Dir, File)]
            catch _:_ ->
                Acc
            end
        end, [], DirContent).


fs_list_dir(Path, Offset, Count, Result) ->
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                Count -> fs_list_dir(Path, Offset + Count, Count * 10, Result ++ FileList);
                _ -> Result ++ FileList
            end;
        _ ->
            {error, not_a_dir}
    end.

fs_mv(Path, TargetDir) ->
    fs_mv(Path, TargetDir, filename:basename(Path)).

fs_mv(Path, TargetDir, TargetName) ->
    TargetPath = filename:absname(TargetName, TargetDir),
    case Path of
        TargetPath ->
            ok;
        _ ->
            logical_files_manager:mv(Path, TargetPath)
    end.


fs_copy(_Path, _TargetPath) ->
    throw(not_yet_implemented).

fs_unique_filename(RequestedPath, 0) ->
    case item_find(RequestedPath) of
        undefined -> RequestedPath;
        _ -> fs_unique_filename(RequestedPath, 1)
    end;

fs_unique_filename(RequestedPath, Counter) ->
    Ext = filename:extension(RequestedPath),
    Rootname = filename:rootname(RequestedPath),
    NewName = lists:flatten(io_lib:format("~s(~B)~s", [Rootname, Counter, Ext])),
    case item_find(NewName) of
        undefined -> NewName;
        _ -> fs_unique_filename(RequestedPath, Counter + 1)
    end.

fs_create_share(Filepath) ->
    logical_files_manager:create_standard_share(Filepath).

fs_get_share_by_filepath(Filepath) ->
    logical_files_manager:get_share({file, Filepath}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions to save and retrieve page state
set_requested_hostname(Host) -> put(rh, Host).
get_requested_hostname() -> get(rh).

set_working_directory(Dir) -> put(wd, Dir).
get_working_directory() -> get(wd).

set_selected_items(List) -> put(sel_items, List).
get_selected_items() -> get(sel_items).

set_display_style(Style) -> put(display_style, Style).
get_display_style() -> get(display_style).

set_displayed_file_attributes(Attrs) -> put(dfa, Attrs).
get_displayed_file_attributes() -> get(dfa).

set_sort_by(Type) -> put(sort_by, Type).
get_sort_by() -> get(sort_by).

set_sort_ascending(Flag) -> put(sort_ascending, Flag).
get_sort_ascending() -> get(sort_ascending).

set_item_list(List) -> put(item_list, List).
get_item_list() -> get(item_list).

set_item_list_rev(MD5) -> put(item_list_rev, MD5).
get_item_list_rev() -> get(item_list_rev).

set_clipboard_items(List) -> put(clipboard_items, List).
get_clipboard_items() -> get(clipboard_items).

set_clipboard_type(Type) -> put(clipboard_type, Type).
get_clipboard_type() -> get(clipboard_type).

set_item_counter(Counter) -> put(item_counter, Counter).
get_item_counter() ->
    Val = get(item_counter),
    put(item_counter, Val + 1),
    integer_to_binary(Val).  % Return binary as this is used for making element IDs

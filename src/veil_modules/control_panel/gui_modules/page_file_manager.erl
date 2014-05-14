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
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").


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
    case gui_utils:maybe_redirect(true, true, true, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}]}
    end.


%% Page title
title() -> <<"File manager">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    reset_wire_accumulator(),
    gui_utils:register_escape_event("escape_pressed"),
    Body = [
        #panel{id = <<"spinner">>, class = <<"spinner">>, style = <<"position: absolute; top: 15px; left: 17px; z-index: 1234;">>, body = [
            #image{image = <<"/images/spinner.gif">>}
        ]},
        gui_utils:top_menu(file_manager_tab, manager_submenu()),
        manager_workspace(),
        footer_popup()
    ],
    do_wiring(),
    Body.


% Submenu that will be glued below the top menu
manager_submenu() ->
    [
        #panel{class = <<"navbar-inner">>, style = <<"padding-top: 10px;">>, body = [
            #panel{class = <<"container">>, style = <<"position: relative; overflow: hidden;">>, body = [
                #list{class = <<"nav">>, body =
                tool_button_and_dummy("tb_up_one_level", <<"Up one level">>, <<"padding: 10px 7px 10px 15px;">>,
                    <<"fui-arrow-left">>, {action, up_one_level})},
                #panel{class = <<"breadcrumb-text breadcrumb-background">>, style = <<"overflow: hidden; margin-left: 15px;">>, body = [
                    #p{id = <<"path_navigator">>, class = <<"breadcrumb-content">>, body = <<"~/">>}%path_navigator_body("/")}
                ]},
                #panel{class = <<"control-group">>, style = <<"position: absolute; right: 15px; top: 0;">>, body = [
                    #panel{class = <<"input-append">>, style = <<"; margin-bottom: 0px;">>, body = [
                        #textbox{id = wire_enter("search_textbox", "search_button"), class = <<"span2">>,
                            style = <<"width: 220px;">>, placeholder = <<"Search">>},
                        #panel{class = <<"btn-group">>, body = [
                            #button{id = wire_click("search_button", ["search_textbox"], {action, search, [{q, "search_textbox"}]}),
                                class = <<"btn">>, type = <<"button">>, body = #span{class = <<"fui-search">>}}
                        ]}
                    ]}
                ]}
            ]}
        ]},
        #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>, body = [
            #panel{class = <<"container">>, body = [
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button("tb_create_dir", <<"Create directory">>, <<"padding: 18px 14px;">>,
                    <<"fui-folder">>, {action, show_popup, [create_directory]}) ++
                    tool_button("tb_upload_files", <<"Upload file(s)">>, <<"padding: 18px 14px;">>,
                        <<"fui-plus-inverted">>, {action, show_popup, [file_upload]}) ++
                    tool_button_and_dummy("tb_share_file", <<"Share">>, <<"padding: 18px 14px;">>,
                        <<"fui-link">>, {action, show_popup, [share_file]})

                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy("tb_rename", <<"Rename">>, <<"padding: 18px 14px;">>,
                    <<"fui-new">>, {action, show_popup, [rename_item]}) ++
                tool_button_and_dummy("tb_remove", <<"Remove">>, <<"padding: 18px 14px;">>,
                    <<"fui-trash">>, {action, show_popup, [remove_selected]})
                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy("tb_cut", <<"Cut">>, <<"padding: 18px 14px;">>,
                    <<"fui-window">>, {action, put_to_clipboard, [cut]}) ++
                %tool_button_and_dummy("tb_copy", "Copy", "padding: 18px 14px;",
                %    "fui-windows", {action, put_to_clipboard, [copy]}) ++

                [#li{id = wire_click("tb_paste", {action, paste_from_clipboard}), body = #link{title = <<"Paste">>, style = <<"padding: 18px 14px;">>,
                    body = #span{class = <<"fui-upload">>, body = #span{id = <<"clipboard_size_label">>, class = <<"iconbar-unread">>,
                        style = <<"right: -12px; top: -10px; background-color: rgb(26, 188, 156);">>,
                        body = <<"0">>}}}},
                    #li{id = <<"tb_paste_dummy">>, class = <<"disabled hidden">>, body = #link{title = <<"Paste">>, style = <<"padding: 18px 14px;">>,
                        body = #span{style = <<"color: rgb(200, 200, 200);">>, class = <<"fui-upload">>}}}]
                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy("tb_select_all", <<"Select all">>, <<"padding: 18px 14px;">>,
                    <<"fui-checkbox-checked">>, {action, select_all}) ++
                tool_button_and_dummy("tb_deselect_all", <<"Deselect all">>, <<"padding: 18px 14px;">>,
                    <<"fui-checkbox-unchecked">>, {action, deselect_all})
                },

                #panel{class = <<"btn-toolbar pull-right no-margin">>, style = <<"padding: 12px 15px; overflow: hidden;">>, body = [
                    #panel{class = <<"btn-group no-margin">>, body = [
                        #link{id = wire_click("list_view_button", {action, toggle_view, [list]}),
                            title = "List view", class = <<"btn btn-small btn-inverse">>,
                            body = #span{class = <<"fui-list-columned">>}},
                        #link{id = wire_click("grid_view_button", {action, toggle_view, [grid]}),
                            title = "Grid view", class = <<"btn btn-small btn-inverse">>,
                            body = #span{class = <<"fui-list-small-thumbnails">>}}
                    ]}
                ]},

                #panel{class = <<"btn-group pull-right">>, style = <<"margin: 12px 15px">>, body = [
                    "<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>",
                    #button{id = wire_click("button_sort_reverse", {action, sort_reverse}), title = "Reverse sorting",
                        class = <<"btn btn-inverse btn-small">>, body = "Sort"},
                    #button{title = "Sort by", class = <<"btn btn-inverse btn-small dropdown-toggle">>,
                        data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = #span{class = <<"caret">>}},
                    #list{id = sort_dropdown, class = <<"dropdown-menu dropdown-inverse">>, body = []}
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
        #li{id = ID ++ "_dummy", class = <<"disabled hidden">>, body = #link{title = Title, style = Style,
            body = #span{style = <<"color: rgb(200, 200, 200);">>, class = Icon}}}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Wiring postbacks. Thanks to this wrapper every time a postback is initiated,
%% there will be spinner showing up in 150ms. It gets hidden when reply is received.
wire_click(ID, Tag) ->
    put(to_wire, get(to_wire) ++ [{ID, ID, #event{type = click, target = ID, postback = Tag}, true}]),
    ID.

wire_click(ID, Source, Tag) ->
    put(to_wire, get(to_wire) ++ [{ID, ID, #event{type = click, target = ID, source = Source, postback = Tag}, true}]),
    ID.

wire_enter(ID, ButtonToClickID) ->
    % No need to show the spinner, as this only performs a click on a submit button
    put(to_wire, get(to_wire) ++ [gui_utils:script_for_enter_submission(ID, ButtonToClickID)]),
    ID.

wire_event(TriggerID, TargetID, Event) ->
    put(to_wire, get(to_wire) ++ [{TriggerID, TargetID, Event, false}]),
    TriggerID.

wire_script(Script) ->
    put(to_wire, get(to_wire) ++ [Script]).


% Wiring should be done after emiting elements that are getting wired.
% wire_xxx() will accumulate wiring clauses, and do_wiring will flush it.
do_wiring() ->
    lists:foreach(
        fun({TriggerID, TargetID, Event, ShowSpinner}) ->
            case ShowSpinner of
                false ->
                    skip;
                true ->
                    wf:wire(gui_utils:script_to_bind_element_click(TriggerID, "$('#spinner').show(150);"))
            end,
            wf:wire(TriggerID, TargetID, Event);
            (Script) ->
                wf:wire(Script)
        end, get(to_wire)),
    reset_wire_accumulator().

% This should be called to init accumulation of wire clauses
reset_wire_accumulator() ->
    put(to_wire, []).


%% Handling events
api_event("escape_pressed", _, _) ->
    event({action, hide_popup}).


event(init) ->
    case gui_utils:user_logged_in() and gui_utils:dn_and_storage_defined() of
        false ->
            skip;
        true ->
            UserID = gui_utils:get_user_dn(),
            Hostname = gui_utils:get_requested_hostname(),
            {ok, Pid} = gui_utils:comet(fun() -> comet_loop_init(UserID, Hostname) end),
            put(comet_pid, Pid)
    end;


event({action, Fun}) ->
    event({action, Fun, []});


event({action, Fun, Args}) ->
    NewArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {q, FieldName} -> gui_utils:to_list(wf:q(FieldName));
                Other -> Other
            end
        end, Args),
    gui_utils:apply_or_redirect(erlang, send, [get(comet_pid), {action, Fun, NewArgs}], true).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Comet loop and functions evaluated by comet
comet_loop_init(UserId, RequestedHostname) ->
    % Initialize page state
    put(user_id, UserId),
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
    reset_wire_accumulator(),
    refresh_workspace(),
    do_wiring(),
    wf:wire(#jquery{target = "spinner", method = ["hide"]}),
    gui_utils:flush(),

    % Enter comet loop for event processing and autorefreshing
    comet_loop(false).


comet_loop(IsUploadInProgress) ->
    try
        receive
            {action, Fun, Args} ->
                case IsUploadInProgress of
                    true ->
                        wf:wire(#alert{text = "Please wait for the upload to finish."}), gui_utils:flush();
                    false ->
                        reset_wire_accumulator(),
                        erlang:apply(?MODULE, Fun, Args),
                        do_wiring()
                end,
                wf:wire(#jquery{target = "spinner", method = ["hide"]}),
                gui_utils:flush(),
                comet_loop(IsUploadInProgress);
            upload_started ->
                comet_loop(true);
            upload_finished ->
                comet_loop(false);
            Other ->
                ?debug("Unrecognized comet message in page_file_manager: ~p", [Other]),
                comet_loop(IsUploadInProgress)

        after ?AUTOREFRESH_PERIOD ->
            % Refresh file list if it has changed
            CurrentItemList = fs_list_dir(get_working_directory()),
            CurrentMD5 = item_list_md5(CurrentItemList),
            case get_item_list_rev() of
                CurrentMD5 ->
                    skip;
                _ ->
                    reset_wire_accumulator(),
                    set_item_list(CurrentItemList),
                    set_item_list_rev(CurrentMD5),
                    refresh_workspace(),
                    do_wiring(),
                    gui_utils:flush()
            end,
            comet_loop(IsUploadInProgress)
        end

    catch Type:Message ->
        ?error_stacktrace("Error in file_manager comet_loop - ~p:~p", [Type, Message]),
        page_error:redirect_with_error(<<"Internal server error">>,
            <<"Server encountered an unexpected error. Please contact the site administrator if the problem persists.">>),
        gui_utils:flush()
    end.


%%%%%%%%%%%%%%%
%% Event handling
clear_manager() ->
    hide_popup(),
    gui_utils:update("path_navigator", path_navigator_body(get_working_directory())),
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
    gui_utils:update("manager_workspace", NewBody).


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
                                list -> {"list_view_button", "grid_view_button"};
                                grid -> {"grid_view_button", "list_view_button"}
                            end,
    wf:wire(#jquery{target = EnableID, method = ["addClass"], args = ["\"active\""]}),
    wf:wire(#jquery{target = DisableID, method = ["removeClass"], args = ["\"active\""]}),

    % Sort dropdown
    DropdownBody = case get_display_style() of
                       grid ->
                           #li{id = wire_click("grid_sort_by_name", {action, sort_toggle, [name]}),
                               class = <<"active">>, body = #link{body = <<"Name">>}};
                       list ->
                           lists:foldl(
                               fun(Attr, Acc) ->
                                   Class = case get_sort_by() of
                                               Attr -> <<"active">>;
                                               _ -> <<"">>
                                           end,
                                   Acc ++ [#li{id = wire_click("list_sort_by_" ++ gui_utils:to_list(Attr), {action, sort_toggle, [Attr]}),
                                       class = Class, body = #link{body = attr_to_name(Attr)}}]
                               end, [], [name | get_displayed_file_attributes()])
                   end,
    gui_utils:update(sort_dropdown, DropdownBody),

    Count = length(get_selected_items()),
    NFiles = length(get_item_list()),
    IsDir = try item_is_dir(item_find(lists:nth(1, get_selected_items()))) catch _:_ -> false end,
    enable_tool_button("tb_up_one_level", get_working_directory() /= "/"),
    enable_tool_button("tb_share_file", (Count =:= 1) andalso (not IsDir)),
    enable_tool_button("tb_rename", Count =:= 1),
    enable_tool_button("tb_remove", Count > 0),
    enable_tool_button("tb_cut", Count > 0),
    enable_tool_button("tb_copy", false),
    enable_tool_button("tb_paste", length(get_clipboard_items()) > 0),
    gui_utils:update("clipboard_size_label", integer_to_list(length(get_clipboard_items()))),
    enable_tool_button("tb_select_all", Count < NFiles),
    enable_tool_button("tb_deselect_all", Count > 0).


enable_tool_button(ID, Flag) ->
    case Flag of
        true ->
            wf:wire(#jquery{target = ID, method = ["removeClass"], args = ["\"hidden\""]}),
            wf:wire(#jquery{target = ID ++ "_dummy", method = ["addClass"], args = ["\"hidden\""]}),
            wf:wire(#jquery{target = ID, method = ["show"], args = ["0"]}),
            wf:wire(#jquery{target = ID ++ "_dummy", method = ["hide"], args = ["0"]});
        false ->
            wf:wire(#jquery{target = ID, method = ["addClass"], args = ["\"hidden\""]}),
            wf:wire(#jquery{target = ID ++ "_dummy", method = ["removeClass"], args = ["\"hidden\""]}),
            wf:wire(#jquery{target = ID, method = ["hide"], args = ["0"]}),
            wf:wire(#jquery{target = ID ++ "_dummy", method = ["show"], args = ["0"]})
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
                    wf:wire(#jquery{target = item_id(Item), method = ["addClass"], args = ["\"selected-item\""]});
                true ->
                    set_selected_items(SelectedItems -- [Path]),
                    wf:wire(#jquery{target = item_id(Item), method = ["removeClass"], args = ["\"selected-item\""]})
            end
    end,
    refresh_tool_buttons().


select_all() ->
    set_selected_items([]),
    lists:foreach(
        fun(Item) ->
            set_selected_items(get_selected_items() ++ [item_path(Item)]),
            wf:wire(#jquery{target = item_id(Item), method = ["addClass"], args = ["\"selected-item\""]})
        end, get_item_list()),
    refresh_tool_buttons().


deselect_all() ->
    lists:foreach(
        fun(Item) ->
            wf:wire(#jquery{target = item_id(Item), method = ["removeClass"], args = ["\"selected-item\""]})
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
    lists:foreach(
        fun(Path) ->
            case get_clipboard_type() of
                cut -> fs_mv(Path, get_working_directory());
                copy -> fs_copy(Path, get_working_directory())
            end
        end, get_clipboard_items()),
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
            case item_find(NewPath) of
                undefined ->
                    fs_mv(OldPath, get_working_directory(), NewName),
                    clear_clipboard(),
                    clear_manager(),
                    select_item(NewPath);
                _ ->
                    wf:wire(#alert{text = NewName ++ " already exists."})
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
                            wf:wire(#alert{text = "Cannot create directory - disallowed name."});
                        _ ->
                            wf:wire(#alert{text = "Cannot create directory - file exists."})
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
                        #textbox{id = wire_enter("create_dir_textbox", "create_dir_submit"), class = <<"flat">>,
                            style = <<"width: 350px;">>, placeholder = <<"New directory name">>},
                        #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>,
                            id = wire_click("create_dir_submit", ["create_dir_textbox"], {action, create_directory, [{q, "create_dir_textbox"}]})}
                    ]}
                ],
                {Body, "$('#create_dir_textbox').focus();", {action, hide_popup}};

            rename_item ->
                Filename = filename:basename(lists:nth(1, get_selected_items())),
                SelectionLength = case string:rchr(Filename, $.) - 1 of
                                      -1 -> length(Filename);
                                      Val -> Val
                                  end,
                OldLocation = lists:nth(1, get_selected_items()),
                Body = [
                    #p{body = <<"Rename <b>", (gui_utils:to_binary(Filename))/binary, "</b>">>},
                    #form{class = <<"control-group">>, body = [
                        #textbox{id = wire_enter("new_name_textbox", "new_name_submit"), class = <<"flat">>,
                            style = <<"width: 350px;">>, placeholder = <<"New name">>, value = gui_utils:to_binary(Filename),
                            data_fields = [{<<"onfocus">>, <<"this.select(); this.selAll=1;">>}]},
                        #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>,
                            id = wire_click("new_name_submit", ["new_name_textbox"], {action, rename_item, [OldLocation, {q, "new_name_textbox"}]})}
                    ]}
                ],
                FocusScript = wf:f("$('#new_name_textbox').focus();
                if (typeof $('#new_name_textbox').selectionStart != \"undefined\")
                {
                    $('#new_name_textbox').selectionStart = ~B;
                    $('#new_name_textbox').selectionEnd = ~B;
                }
                else if (document.selection && document.selection.createRange)
                {
                    // IE branch
                    $('#new_name_textbox').select();
                    var range = document.selection.createRange();
                    range.collapse(true);
                    range.moveEnd(\"character\", ~B);
                    range.moveStart(\"character\", ~B);
                    range.select();
                }", [0, SelectionLength, 0, SelectionLength]),
                {Body, FocusScript, {action, hide_popup}};

            share_file ->
                Path = lists:nth(1, get_selected_items()),
                Filename = filename:basename(lists:nth(1, get_selected_items())),
                {Status, ShareID} = case fs_get_share_by_filepath(Path) of
                                        {ok, #veil_document{uuid = UUID}} ->
                                            {exists, gui_utils:to_binary(UUID)};
                                        _ ->
                                            {ok, ID} = fs_create_share(Path),
                                            {new, gui_utils:to_binary(ID)}
                                    end,
                clear_workspace(),
                select_item(Path),
                AddressPrefix = <<"https://", (get_requested_hostname())/binary, ?shared_files_download_path>>,
                Body = [
                    case Status of
                        exists ->
                            #p{body = <<"<b>", (gui_utils:to_binary(Filename))/binary,
                            "</b> is already shared. Visit <b>Shared files</b> tab for more.">>};
                        new ->
                            #p{body = <<"<b>", (gui_utils:to_binary(Filename))/binary,
                            "</b> successfully shared. Visit <b>Shared files</b> tab for more.">>}
                    end,
                    #form{class = <<"control-group">>, body = [
                        #textbox{id = wire_enter("shared_link_textbox", "shared_link_submit"), class = "flat", style = "width: 700px;",
                            value = <<AddressPrefix/binary, ShareID/binary>>, placeholder = "Download link"},
                        #button{id = wire_click("shared_link_submit", {action, hide_popup}),
                            class = <<"btn btn-success btn-wide">>, body = <<"Ok">>}
                    ]}
                ],
                {Body, "$('#shared_link_textbox').focus(); $('#shared_link_textbox').select();", {action, hide_popup}};

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
                                    #button{id = wire_click("ok_button", {action, remove_selected}),
                                        class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                                    #button{id = wire_click("cancel_button", {action, hide_popup}),
                                        class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>}
                                ]}
                            ],
                            {Body, "$('#ok_button').focus();", {action, hide_popup}}
                    end;

            _ ->
                {[], undefined, undefined}
        end,
    case FooterBody of
        [] -> skip;
        _ ->
            CloseButton = #link{id = wire_click("close_button", CloseButtonAction), title = <<"Hide">>, class = <<"glyph-link">>,
                style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
                body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
            gui_utils:update("footer_popup", [CloseButton | FooterBody]),
            wf:wire(#jquery{target = "footer_popup", method = ["removeClass"], args = ["\"hidden\""]}),
            wf:wire(#jquery{target = "footer_popup", method = ["slideDown"], args = ["200"]})
    end,
    case Script of
        undefined -> skip;
        Script -> wf:wire(Script)
    end.


% Hides the footer popup
hide_popup() ->
    gui_utils:update("footer_popup", []),
    wf:wire(#jquery{target = "footer_popup", method = ["addClass"], args = ["\"hidden\""]}),
    wf:wire(#jquery{target = "footer_popup", method = ["slideUp"], args = ["200"]}).


% Render path navigator
path_navigator_body(WorkingDirectory) ->
    case WorkingDirectory of
        "/" -> wf:f("~~", []);
        _ ->
            FirstLink = #link{id = wire_click("nav_top", {action, navigate, ["/"]}), body = <<"~">>},
            PathElements = string:tokens(WorkingDirectory, "/"),
            {LinkList, _} = lists:mapfoldl(
                fun(Element, {CurrentPath, Counter}) ->
                    PathToElement = CurrentPath ++ "/" ++ Element,
                    Link = #link{id = wire_click("nav_" ++ integer_to_list(Counter), {action, navigate, [PathToElement]}),
                        body = gui_utils:to_binary(Element)},
                    {Link, {PathToElement, Counter + 1}}
                end, {"/", 1}, lists:sublist(PathElements, length(PathElements) - 1)),
            [FirstLink | LinkList] ++ [lists:last(PathElements)]
    end.


% Render grid view workspace
grid_view_body() ->
    {Tiles, _} = lists:mapfoldl(
        fun(Item, Counter) ->
            FullPath = item_path(Item),
            Filename = gui_utils:to_binary(item_basename(Item)),
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
            wire_script(wf:f("$('.wfid_~s').click(function(e) { e.stopPropagation();});", [LinkID])),
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
                                       #link{title = Filename, id = wire_click(LinkID, {action, navigate, [FullPath]}), body = Filename}
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
                                       #link{title = Filename, id = LinkID, body = Filename, target = <<"_blank">>,
                                           url = <<?user_content_download_path, "/", (gui_utils:to_binary(wf:url_encode(FullPath)))/binary>>}
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
                                           #link{id = wire_click("prev_dir_link_image", {action, navigate, [PrevDir]}), body = [
                                               #image{class = <<"list-icon">>, image = <<"/images/folder32.png">>}
                                           ]}
                                       ]},
                                       #panel{style = <<"max-width: 230px; word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                           #link{id = wire_click("prev_dir_link_text", {action, navigate, [PrevDir]}), body = <<"..&nbsp;&nbsp;&nbsp;">>}
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
            Filename = gui_utils:to_binary(item_basename(Item)),
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
            wire_script(wf:f("$('#~s').click(function(e) { e.stopPropagation(); });", [LinkID])),
            ImageID = <<"image_", (integer_to_binary(Counter))/binary>>,
            % Image won't hightlight if the image is clicked.
            wire_script(wf:f("$('#~s').click(function(e) { e.stopPropagation();});", [ImageID])),
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
                                            #link{id = wire_click(LinkID, {action, navigate, [FullPath]}), body = Filename}
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
                                        url = <<?user_content_download_path, "/", (gui_utils:to_binary(wf:url_encode(FullPath)))/binary>>, body = [
                                            ShareIcon,
                                            #image{class = <<"list-icon">>, style = ImageStyle, image = ImageUrl}
                                        ]}
                                ]},
                                #panel{class = <<"filename_row">>, style = <<"word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                    #link{id = LinkID, body = Filename, target = <<"_blank">>,
                                        url = <<?user_content_download_path, "/", (gui_utils:to_binary(wf:url_encode(FullPath)))/binary>>}
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
    wire_script(wf:f("window.onresize = function(e) { $('.filename_row').css('max-width', '' +
            ($(window).width() - ~B) + 'px'); }; $(window).resize();", [ContentWithoutFilename])),
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

item_dirname(#item{path = Path}) ->
    filename:dirname(Path).

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

item_attr_value(name, Item) -> gui_utils:to_binary(item_basename(Item));
item_attr_value(mode, Item) -> gui_utils:to_binary(lists:flatten(io_lib:format("~.8B", [item_attr(mode, Item)])));
item_attr_value(uname, Item) -> gui_utils:to_binary(item_attr(uname, Item));
item_attr_value(atime, Item) -> gui_utils:to_binary(time_to_string(item_attr(atime, Item)));
item_attr_value(mtime, Item) -> gui_utils:to_binary(time_to_string(item_attr(mtime, Item)));
item_attr_value(ctime, Item) -> gui_utils:to_binary(time_to_string(item_attr(ctime, Item)));
item_attr_value(size, Item) ->
    case item_is_dir(Item) of
        true -> <<"">>;
        false -> gui_utils:to_binary(size_to_printable(item_attr(size, Item)))
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
        TargetPath -> ok;
        _ ->
            case logical_files_manager:mv(Path, TargetPath) of
                ok -> ok;
                _ -> wf:wire(#alert{text = "Unable to move " ++ TargetName ++
                    ". File exists."})
            end
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

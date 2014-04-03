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

-module(page_file_manager).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/fslogic/fslogic.hrl").


% How often shoul comet process check for changes in current dir
-define(AUTOREFRESH_PERIOD, 500).

% Item is either a file or a dir represented in manager
-record(item, {id="", path="/", is_shared=false, attr=#fileattributes { } }).

%%%%% List of keys stored in session memory
% user_doc -> user's document obtained during login
% working_directory -> directory user is currently in
% selected_items -> list of selected paths
% display_style -> list or grid
% displayed_file_attributes -> list of attr names as in item_attr
% sort_by -> one of attrs from above
% sort_ascending -> true / false
% item_list -> current dir content, list of #item records
% item_list_rev -> md5 of current dir state to comparison
% clipboard_items -> list of paths
% clipboard_type -> cut or paste

%% Template points to the template file, which will be filled with content
main() -> #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "File manager".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() -> 
    gui_utils:apply_or_redirect(?MODULE, render_body, true).


% Page content
render_body() ->
    reset_wire_accumulator(),
    init_session_state(),
    {ok, SyncPid} = wf:comet(fun() -> sync_loop_init() end),
    put_key(sync_pid, SyncPid),
    {ok, _CometPid} = wf:comet(fun() -> comet_loop_init() end),
    wf:wire(#api { name="escape_pressed", tag=escape_pressed }),
    wf:wire(#script { script=
        "jQuery(document).bind('keydown', function (e){if (e.which == 27) document.escape_pressed();});"
    }),
    Body = [
        #panel { id=spinner, class="spinner", style="display: none; position: absolute; top: 15px; left: 17px; z-index: 1234;", body=[
            #image { image="/images/spinner.gif" }
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
        #panel { class="navbar-inner", style="padding-top: 10px;", body=[
            #panel { class="container", style="position: relative; overflow: hidden;", body=[
                #list {class="nav", body=
                    tool_button_and_dummy("tb_up_one_level", "Up one level", "padding: 10px 7px 10px 15px;",
                        "fui-arrow-left", {action, up_one_level})},
                #panel { class="breadcrumb-text breadcrumb-background", style="overflow: hidden; margin-left: 15px;", body=[
                    #p{ id=path_navigator, class="breadcrumb-content", body=path_navigator_body(get_key(working_directory)) }
                ]},
                #panel { class="control-group", style="position: absolute; right: 15px; top: 0;", body=[
                    #panel { class="input-append", style="; margin-bottom: 0px;", body=[
                        #textbox { id=wire_enter(search_textbox, {action, search, [{q, search_textbox}]}), class="span2", 
                            style="width: 220px;", placeholder="Search" },
                        #panel { class="btn-group", body=[
                            #bootstrap_button { id=wire_click({action, search, [{q, search_textbox}]}), class="btn", type="button", 
                            body=#span { class="fui-search" } }
                        ]}
                    ]}
                ]}
            ]}
        ]},
        #panel { class="navbar-inner", style="border-bottom: 1px solid gray; padding-bottom: 5px;", body=[
            #panel { class="container", body=[
                #list { class="nav", style="margin-right: 30px;", body=
                    tool_button("tb_create_dir", "Create directory", "padding: 18px 14px;", 
                        "fui-folder", {action, show_popup, [create_directory]}) ++
                    tool_button("tb_upload_files", "Upload file(s)", "padding: 18px 14px;", 
                        "fui-plus-inverted", {action, show_popup, [file_upload]}) ++
                    tool_button_and_dummy("tb_share", "Share", "padding: 18px 14px;", 
                        "fui-link", {action, show_popup, [share_file]})

                },
                #list { class="nav", style="margin-right: 30px;", body=
                    tool_button_and_dummy("tb_rename", "Rename", "padding: 18px 14px;", 
                        "fui-new", {action, show_popup, [rename_item]}) ++
                    tool_button_and_dummy("tb_remove", "Remove", "padding: 18px 14px;", 
                        "fui-trash", {action, show_popup, [remove_selected]})
                },
                #list { class="nav", style="margin-right: 30px;", body=
                    tool_button_and_dummy("tb_cut", "Cut", "padding: 18px 14px;", 
                        "fui-window", {action, put_to_clipboard, [cut]}) ++
                    %tool_button_and_dummy("tb_copy", "Copy", "padding: 18px 14px;", 
                    %    "fui-windows", {action, put_to_clipboard, [copy]}) ++

                    [#listitem { id=wire_click("tb_paste", {action, paste_from_clipboard}), body=#link{ title="Paste", style="padding: 18px 14px;",
                        body=#span{ class="fui-upload", body=#span { id=clipboard_size_label, class="iconbar-unread", 
                            style="right: -12px; top: -10px; background-color: rgb(26, 188, 156);", 
                            body="0" } } } },
                    #listitem { id="tb_paste_dummy", class="disabled hidden", body=#link{ title="Paste", style="padding: 18px 14px;",
                    body=#span{ style="color: rgb(200, 200, 200);", class="fui-upload" } } }]            
                },
                #list { class="nav", style="margin-right: 30px;", body=
                    tool_button_and_dummy("tb_select_all", "Select all", "padding: 18px 14px;", 
                        "fui-checkbox-checked", {action, select_all}) ++
                    tool_button_and_dummy("tb_deselect_all", "Deselect all", "padding: 18px 14px;", 
                        "fui-checkbox-unchecked", {action, deselect_all})
                },
                %#link { id=wire_click({action, show_popup, [advanced]}), class="btn btn-small btn-inverse pull-right", 
                %    style="margin: 12px 15px;", body="Advanced" },

                #panel { class="btn-toolbar pull-right no-margin", style="padding: 12px 15px; overflow: hidden;", body=[
                    #panel { class="btn-group no-margin", body=[
                        #link { id=wire_click(list_view_button, {action, toggle_view, [list]}), 
                            title="List view", class="btn btn-small btn-inverse", 
                            body=#span { class="fui-list-columned" }  },
                        #link { id=wire_click(grid_view_button, {action, toggle_view, [grid]}), 
                            title="Grid view", class="btn btn-small btn-inverse", 
                            body=#span { class="fui-list-small-thumbnails" } }
                    ]}
                ]},

                #panel { class="btn-group pull-right", style="margin: 12px 15px", body=[
                    "<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>",
                    #bootstrap_button { id=wire_click({action, sort_reverse}), title="Reverse sorting", 
                        class="btn btn-inverse btn-small", body="Sort" },
                    #bootstrap_button { title="Sort by", class="btn btn-inverse btn-small dropdown-toggle", 
                        data_toggle="dropdown", body=#span { class="caret" } },
                    #list { id=sort_dropdown, class="dropdown-menu dropdown-inverse", body=[]}
                ]}                
            ]}
        ]}            
    ].


% Working space of the explorer.
manager_workspace() ->
    #panel { id=manager_workspace, 
        style="z-index: -1; margin: 170px 0 20px 0; overflow: hidden", body=
        case get_key(display_style) of
            list -> list_view_body();
            grid -> grid_view_body()
        end }.


% Footer popup to display prompts and forms.
footer_popup() ->
    #panel { class="dialog success-dialog wide hidden", 
        style="z-index: 2; position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0px; width: 100%;", 
        id=footer_popup,  body=[] }.


% Emits a button, properly wired (postback)
tool_button(ID, Title, Style, Icon, Postback) ->
    [
        #listitem { id=wire_click(ID, Postback), body=#link{ title=Title, style=Style,
            body=#span{ class=Icon } } }
    ].


% Emits a button, properly wired (postback) + its disabled clone
tool_button_and_dummy(ID, Title, Style, Icon, Postback) ->
    tool_button(ID, Title, Style, Icon, Postback) ++
    [
        #listitem { id=ID ++ "_dummy", class="disabled hidden", body=#link{ title=Title, style=Style,
            body=#span{ style="color: rgb(200, 200, 200);", class=Icon } } }
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Initializing session state

init_session_state() ->
    put_key(requested_hostname, gui_utils:get_requested_hostname()),
    put(user_id, gui_utils:get_user_dn()),
    put_key_if_undefined(working_directory, "/"),
    put_key(selected_items, []),
    put_key_if_undefined(display_style, list),
    put_key_if_undefined(displayed_file_attributes, [size, atime, mtime]),
    put_key_if_undefined(sort_by, name),
    put_key_if_undefined(sort_ascending, true),
    put_key(item_list, fs_list_dir(get_key(working_directory))),
    put_key(item_list_rev, item_list_md5(get_key(item_list))),
    put_key_if_undefined(clipboard_items, []),
    put_key_if_undefined(clipboard_type, none),
    refresh_tool_buttons(),
    sort_item_list().



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Wiring postbacks. Thanks to this wrapper every time a postback is initiated,
%% there will be spinner showing up in 150ms. It gets hidden when reply is received.

wire_click(Tag) ->
    wire_click(wf:temp_id(), Tag).

wire_click(ID, Tag) ->
    put(to_wire, get(to_wire) ++ [{ID, ID, #event { type=click, postback=Tag}, true}]),
    ID.

wire_enter(ID, Tag) ->
    put(to_wire, get(to_wire) ++ [{ID, ID, #event { type=enterkey, postback=Tag}, true}]),
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
        fun ({TriggerID, TargetID, Event=#event { type=Type }, ShowSpinner}) -> 
            wf:wire(TriggerID, TargetID, Event),
            case ShowSpinner of
                false -> skip;
                true ->
                    wf:wire(TriggerID, #event { type=Type, actions=
                        #script { script="$('.spinner').show(150);" } })
            end;
            (Script) ->
                wf:wire(#script { script=Script })
        end, get(to_wire)),
    reset_wire_accumulator().

% This should be called to init accumulation of wire clauses
reset_wire_accumulator() ->
    put(to_wire, []).





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Synchronisation of postback and comet processes

sync_loop_init() ->
    sync_loop(none, none, erlang:now()).

sync_loop(Holder, Waiting, LastUpdate) ->
    receive 
        {Pid, lock, MinTimePassed} ->
            case Holder of
                none -> 
                    case MinTimePassed of
                        0 -> 
                            Pid ! {self(), ok},
                            sync_loop(Pid, none, erlang:now());
                        _ ->
                            TimePassed = timer:now_diff(erlang:now(), LastUpdate) div 1000,
                            if
                                MinTimePassed > TimePassed -> 
                                    Pid ! {self(), {wait, MinTimePassed - TimePassed}},
                                    sync_loop(none, none, LastUpdate);
                                true ->
                                    Pid ! {self(), ok},
                                    sync_loop(Pid, none, erlang:now())
                            end
                    end;
                _ ->
                    case MinTimePassed of
                        0 -> 
                            sync_loop(Holder, Pid, erlang:now());
                        _ ->
                            Pid ! {self(), {wait, MinTimePassed}},
                            sync_loop(Holder, Waiting, LastUpdate)
                    end
            end;
        {Holder, release} ->
            if 
                Waiting /= none -> Waiting ! {self(), ok};
                true -> skip
            end,
            Holder ! {self(), ok},
            sync_loop(Waiting, none, erlang:now());

        _ -> 
            sync_loop(Holder, Waiting, LastUpdate)

    % Make sure no deadlock occurs
    after 10000 ->
            if 
                Waiting /= none -> Waiting ! {self(), ok};
                true -> skip
            end,
            if 
                Holder /= none -> Holder ! {self(), ok};
                true -> skip
            end,
            sync_loop(Waiting, none, erlang:now())
    end.


sync_lock(MinTimePassed) ->
    Pid = get_key(sync_pid),
    Pid ! {self(), lock, MinTimePassed},
    receive {Pid, Ans} -> Ans end.

sync_release() ->
    Pid = get_key(sync_pid),
    Pid ! {self(), release},
    receive {Pid, Ans} -> Ans after 500 -> throw(cannot_release_lock) end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Handling events

api_event("escape_pressed", escape_pressed, []) ->
    event({action, hide_popup}).


event({action, Fun}) ->
    event({action, Fun, []});

event({action, Fun, Args}) ->    
    NewArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {q, FieldName} -> wf:q(FieldName);
                Other -> Other
            end
        end, Args),    
    gui_utils:apply_or_redirect(?MODULE, process_event, [{action, Fun, NewArgs}], true).


process_event({action, Fun, Args}) ->
    sync_lock(0),
    reset_wire_accumulator(),
    put(user_id, gui_utils:get_user_dn()),
    erlang:apply(?MODULE, Fun, Args),
    wf:wire(spinner, #hide { }),
    do_wiring(),
    sync_release().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Actions performed by comet (workspace refreshing)

comet_loop_init() ->
    timer:sleep(?AUTOREFRESH_PERIOD),
    comet_loop().

comet_loop() ->
    case sync_lock(?AUTOREFRESH_PERIOD) of
        {wait, Time} ->
            timer:sleep(Time);
        ok ->
            try
                case gui_utils:get_user_dn() of
                    undefined -> 
                        sync_release(),
                        wf:redirect("/manage_account");
                    DN ->    
                        put(user_id, DN),
                        comet_maybe_refresh(),
                        sync_release(),
                        timer:sleep(?AUTOREFRESH_PERIOD)
                end
            catch Type:Message ->
                catch sync_release(),
                lager:error("Error in file_manager comet_loop - ~p - ~p~n~p", 
                    [Type, Message, erlang:get_stacktrace()])
            end
    end,
    comet_loop().


comet_maybe_refresh() ->        
    CurrentItemList = fs_list_dir(get_key(working_directory)),
    CurrentMD5 = item_list_md5(CurrentItemList),
    case get_key(item_list_rev) of
        CurrentMD5 -> skip;
        _ -> 
            reset_wire_accumulator(),
            put_key(item_list, CurrentItemList),
            put_key(item_list_rev, CurrentMD5),
            refresh_workspace(),
            do_wiring(),
            wf:flush()
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Actions made as result of postbacks to the page

clear_manager() ->
    hide_popup(),
    wf:update(path_navigator, path_navigator_body(get_key(working_directory))),
    clear_workspace().


clear_workspace() ->
    put_key(item_list, fs_list_dir(get_key(working_directory))),
    put_key(item_list_rev, item_list_md5(get_key(item_list))),
    refresh_workspace().


refresh_workspace() ->
    put_key(selected_items, []),
    refresh_tool_buttons(),
    sort_item_list(),
    NewBody = case get_key(display_style) of
        list -> list_view_body();
        grid -> grid_view_body()
    end,
    % Let know the comet process that workspace was refreshed.
    % It will make it wait at least 0.5 sec.
    wf:update(manager_workspace, NewBody).


sort_item_list() ->
    ItemList = get_key(item_list),
    Attr = get_key(sort_by),
    SortAscending = get_key(sort_ascending),
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
                V1 < V2   -> true;
                V1 > V2   -> false;
                V1 =:= V2 -> item_attr(name, Item1) =< item_attr(name, Item2)
            end
        end, ItemList), 
    Result = case SortAscending of 
        true -> SortedItems;
        false -> lists:reverse(SortedItems)
    end,
    FinalResult = case Attr of
        name ->    
            {Dirs, Files} = lists:partition(fun(I) ->item_is_dir(I) end, Result),
            Dirs ++ Files;
        _ -> Result
    end,
    put_key(item_list, FinalResult).        


sort_toggle(Type) ->
    case get_key(sort_by) of
        Type -> sort_reverse();
        _ -> 
            put_key(sort_by, Type),
            put_key(sort_ascending, true),
            refresh_workspace()
    end.


sort_reverse() ->
    put_key(sort_ascending, not get_key(sort_ascending)),
    refresh_workspace().


refresh_tool_buttons() ->
    % View toggles
    {EnableID, DisableID} = case get_key(display_style) of
        list -> {list_view_button, grid_view_button};
        grid -> {grid_view_button, list_view_button}
    end,
    wf:wire(EnableID, #add_class { class="active", speed=0 }),
    wf:wire(DisableID, #remove_class { class="active", speed=0 }),


    % Sort dropdown
    DropdownBody = case get_key(display_style) of
        grid -> 
            #listitem { id=wire_click({action, sort_toggle, [name]}), 
                class="active", body=#link { text="Name" } }; 
        list -> 
            lists:foldl(
                fun(Attr, Acc) ->
                    Class = case get_key(sort_by) of
                        Attr -> "active";
                        _ -> ""
                    end,                    
                    Acc ++ [#listitem { id=wire_click({action, sort_toggle, [Attr]}), 
                        class=Class, body=#link { text=attr_to_name(Attr) } }]
                end, [], [name|get_key(displayed_file_attributes)])
    end,
    wf:update(sort_dropdown, DropdownBody),

    Count = length(get_key(selected_items)),
    NFiles = length(get_key(item_list)),
    IsDir = try item_is_dir(item_find(lists:nth(1, get_key(selected_items)))) catch _:_ -> false end,
    enable_tool_button("tb_up_one_level", get_key(working_directory) /= "/"),
    enable_tool_button("tb_share", (Count =:= 1) andalso (not IsDir)),
    enable_tool_button("tb_rename", Count =:= 1),
    enable_tool_button("tb_remove", Count > 0),
    enable_tool_button("tb_cut", Count > 0),
    enable_tool_button("tb_copy", false),
    enable_tool_button("tb_paste", length(get_key(clipboard_items)) > 0),
    wf:update(clipboard_size_label, integer_to_list(length(get_key(clipboard_items)))),
    enable_tool_button("tb_select_all", Count < NFiles),
    enable_tool_button("tb_deselect_all", Count > 0).
    

enable_tool_button(ID, Flag) ->
    case Flag of 
        true ->
            wf:wire(ID, #remove_class { class="hidden", speed=0 }),
            wf:wire(ID ++ "_dummy", #add_class { class="hidden", speed=0 }),
            wf:wire(ID, #show { speed=0 }),
            wf:wire(ID ++ "_dummy", #hide { speed=0 });
        false ->
            wf:wire(ID, #add_class { class="hidden", speed=0 }),
            wf:wire(ID ++ "_dummy", #remove_class { class="hidden", speed=0 }),
            wf:wire(ID, #hide { speed=0 }),
            wf:wire(ID ++ "_dummy", #show { speed=0 })
    end.


navigate(Path) ->
    put_key(working_directory, Path),
    clear_manager().


up_one_level() ->
    navigate(filename:dirname(filename:absname(get_key(working_directory)))).


toggle_view(Type) ->
    put_key(display_style, Type),
    put_key(sort_by, name),
    put_key(sort_ascending, true),
    clear_workspace().


select_item(Path) ->
    case item_find(Path) of
        undefined -> skip;
        Item ->
            SelectedItems = get_key(selected_items),
            case lists:member(Path, SelectedItems) of 
                false ->
                    put_key(selected_items, SelectedItems ++ [Path]),
                    wf:wire(item_id(Item), #add_class { class="selected-item", speed=0 });
                true ->
                    put_key(selected_items, SelectedItems -- [Path]),
                    wf:wire(item_id(Item), #remove_class { class="selected-item", speed=0 })
            end
    end,
    refresh_tool_buttons().


select_all() ->
    put_key(selected_items, []),
    lists:foreach(
        fun(Item) ->
            put_key(selected_items, get_key(selected_items) ++ [item_path(Item)]),
            wf:wire(item_id(Item), #add_class { class="selected-item", speed=0 })
        end, get_key(item_list)),
    refresh_tool_buttons(). 


deselect_all() ->
    lists:foreach(
        fun(Item) ->
            wf:wire(item_id(Item), #remove_class { class="selected-item", speed=0 })
        end, get_key(item_list)),
    put_key(selected_items, []),
    refresh_tool_buttons().


clear_clipboard() ->
    put_key(clipboard_items, []),
    put_key(clipboard_type, none).


put_to_clipboard(Type) ->
    SelectedItems = get_key(selected_items),
    put_key(clipboard_type, Type),
    put_key(clipboard_items, SelectedItems),
    clear_workspace().


paste_from_clipboard() ->
    lists:foreach(
        fun(Path) ->
            case get_key(clipboard_type) of
                cut -> fs_mv(Path, get_key(working_directory));
                copy -> fs_copy(Path, get_key(working_directory))
            end
        end, get_key(clipboard_items)),
    clear_clipboard(),
    clear_workspace().


rename_item(OldPath, NewName) ->
    OldName = filename:basename(OldPath),
    case NewName of
        [] -> hide_popup();
        undefined -> hide_popup();
        OldName -> hide_popup(); 
        _ ->
            NewPath = filename:absname(NewName, get_key(working_directory)),
            case item_find(NewPath) of
                undefined -> 
                    logical_files_manager:mv(OldPath, NewPath),
                    clear_clipboard(),
                    clear_manager(),
                    select_item(NewPath);
                _ ->   
                    wf:wire(#alert { text=NewName ++ " already exists." })
            end
    end.


create_directory(Name) ->
    case Name of
        [] -> hide_popup();
        undefined -> hide_popup();
        _ -> 
            FullPath = filename:absname(Name, get_key(working_directory)),
            case fs_mkdir(FullPath) of
                ok ->                
                    clear_manager(),             
                    select_item(FullPath);
                _ ->
                    case item_find(FullPath) of
                        undefined -> 
                            wf:wire(#alert { text="Cannot create directory - disallowed name." });
                        _ -> 
                            wf:wire(#alert { text="Cannot create directory - file exists." })
                    end,
                    hide_popup()
            end
    end.


remove_selected() ->
    SelectedItems = get_key(selected_items),
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
                end, get_key(item_list))
    end. 


% Shows popup with a prompt, form, etc.
show_popup(Type) ->
    {FooterBody, Script, CloseButtonAction} = case Type of
        create_directory -> 
            Body = [
                #p { text="Create directory" },
                #form { class="control-group", body=[
                    #textbox { id=wire_enter(create_dir_textbox, {action, create_directory, [{q, create_dir_textbox}]}), class="flat", style="width: 350px;", placeholder="New directory name" },
                    #bootstrap_button { class="btn btn-success btn-wide", body="Ok", id=wire_click({action, create_directory, [{q, create_dir_textbox}]})}
                ]}
            ],
            {Body, "obj('create_dir_textbox').focus();", {action, hide_popup}};

        rename_item ->
            Filename = filename:basename(lists:nth(1, get_key(selected_items))),
            SelectionLength = case string:rchr(Filename, $.) - 1 of
                -1 -> length(Filename);
                Val -> Val
            end, 
            OldLocation = lists:nth(1, get_key(selected_items)),
            Body = [
                #p { body="Rename <b>" ++ Filename ++ "</b>" },
                #form { class="control-group", body=[
                    #textbox { id=wire_enter(new_name_textbox, {action, rename_item, [OldLocation, {q, new_name_textbox}]}), class="flat", 
                        style="width: 350px;", placeholder="New name", text=Filename},
                    #bootstrap_button { id=wire_click({action, rename_item, [OldLocation, {q, new_name_textbox}]}), 
                        class="btn btn-success btn-wide", body="Ok" }
                ]}
            ],
            {Body, 
                wf:f("obj('new_name_textbox').focus();
                if (typeof obj('new_name_textbox').selectionStart != \"undefined\") 
                {
                    obj('new_name_textbox').selectionStart = ~B;
                    obj('new_name_textbox').selectionEnd = ~B;
                } 
                else if (document.selection && document.selection.createRange) 
                {
                    // IE branch
                    obj('new_name_textbox').select();
                    var range = document.selection.createRange();
                    range.collapse(true);
                    range.moveEnd(\"character\", ~B);
                    range.moveStart(\"character\", ~B);
                    range.select();
                }", [0, SelectionLength, 0, SelectionLength]), 
                {action, hide_popup}};

        share_file ->
            Path = lists:nth(1, get_key(selected_items)),
            Filename = filename:basename(lists:nth(1, get_key(selected_items))),
            {Status, ShareID} = case logical_files_manager:get_share({file, Path}) of
                {ok, #veil_document { uuid=UUID } } -> 
                    {exists, UUID};
                _ ->
                    {ok, ID} = logical_files_manager:create_standard_share(Path),
                    {new, ID}
            end,
            clear_workspace(),
            select_item(Path),
            AddressPrefix = "https://" ++ get_key(requested_hostname) ++ ?shared_files_download_path,
            Body = [
                case Status of
                    exists -> #p { body="<b>" ++ Filename ++ "</b> is already shared. Visit <b>Shared files</b> tab for more." };
                    new -> #p { body="<b>" ++ Filename ++ "</b> successfully shared. Visit <b>Shared files</b> tab for more." }            
                end,
                #form { class="control-group", body=[
                    %% todo add hostname dynamically
                    #textbox { id=wire_enter(shared_link_textbox, {action, hide_popup}), class="flat", style="width: 700px;", 
                        text=AddressPrefix ++ ShareID, placeholder="Download link" }
                ]}
            ],
            {Body, "obj('shared_link_textbox').focus(); obj('shared_link_textbox').select();",
                {action, hide_popup}};

        file_upload ->
            Body = [
                #veil_upload{ tag=file_upload, show_button=true, multiple=true, droppable=true, target_dir=get_key(working_directory) }
            ],
            {Body, undefined, {action, clear_manager}};
            
        remove_selected ->
            {_FB, _S, _A} = case get_key(selected_items) of
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
                        (NumFiles =:= 1) -> wf:f("~B file", [NumFiles]);
                        (NumFiles > 1) -> wf:f("~B files", [NumFiles]);
                        true -> ""
                    end,
                    DirsString = if
                        (NumDirs =:= 1) -> wf:f("~B directory and all its content", [NumDirs]);
                        (NumDirs > 1) -> wf:f("~B directories and all their content", [NumDirs]);
                        true -> ""
                    end,
                    Punctuation = if
                        (FilesString /= "") and (DirsString /= "") -> ", ";
                        true -> ""
                    end,
                    Body = [
                        #p { text=wf:f("Remove ~s~s~s?", [FilesString, Punctuation, DirsString])},
                        #form { class="control-group", body=[
                            #bootstrap_button { id=wire_click(ok_button, {action, remove_selected}), 
                                class="btn btn-success btn-wide", body="Ok" },
                            #bootstrap_button { id=wire_click({action, hide_popup}),
                                class="btn btn-danger btn-wide", body="Cancel" }
                        ]}
                    ],
                    {Body, "obj('ok_button').focus();", {action, hide_popup}}
            end;

        % todo own checkbox element (collision with nitrogen's)
        advanced2 ->
            Body = [
                #form { class="control-group", body=[
                    #p { text="Displayed attributes" },
                    #panel { style="text-aalign: left;", body=[
                        #label { class="checkbox primary", for="checkbox5", body=[
                            #span { class="icons", body=[
                                #span { class="first-icon fui-checkbox-unchecked" },
                                #span { class="second-icon fui-checkbox-checked" }
                            ]},
                            #checkbox { value="", id="checkbox5" },
                            "Unchecked"
                        ]}
                    ]}
                ]}
            ],
            {Body, undefined, {action, hide_popup}};


        _ ->
            {[], undefined, undefined}
        end,
    case FooterBody of
        [] -> skip;
        _ -> 
            CloseButton = #link{ id=wire_click(CloseButtonAction), title="Hide", class="glyph-link", 
                style="position: absolute; top: 8px; right: 8px; z-index: 3;", 
                body=#span{ class="fui-cross", style="font-size: 20px;" } },
            wf:update(footer_popup, [CloseButton|FooterBody]),
            wf:wire(footer_popup, #remove_class { class="hidden", speed=0 }),
            wf:wire(footer_popup, #slide_down { speed=200 })
    end,

    case Script of
        undefined -> skip;
        Script -> wf:wire(#script { script=Script })
    end.


% Hides the footer popup
hide_popup() ->
    wf:update(footer_popup, []),
    wf:wire(footer_popup, #add_class { class="hidden", speed=0 }),
    wf:wire(footer_popup, #slide_up { speed=200 }).


% Render path navigator
path_navigator_body(WorkingDirectory) ->
    case WorkingDirectory of
        "/" -> wf:f("~~", []);
         _ -> 
             FirstLink = #link { id=wire_click({action, navigate, ["/"]}), text=wf:f("~~", [])},
            PathElements = string:tokens(WorkingDirectory, "/"),
            {LinkList, _} = lists:mapfoldl(
                fun(Element, CurrentPath) ->
                    PathToElement = CurrentPath ++ "/" ++ Element,
                    Link = #link { id=wire_click({action, navigate, [PathToElement]}), text=Element },
                    {Link, PathToElement}
                end, "/", lists:sublist(PathElements, length(PathElements) - 1)),
            [FirstLink|LinkList] ++ [lists:last(PathElements)]
    end.


% Render grid view workspace
grid_view_body() ->
    Tiles = lists:map(
        fun(Item) ->
            FullPath = item_path(Item),
            Filename = item_basename(Item),
            ImageStyle = case get_key(clipboard_type) of
                cut ->
                    case lists:member(FullPath, get_key(clipboard_items)) of
                        true -> "opacity:0.3; filter:alpha(opacity=30);";
                        _ -> ""
                    end;
                _ -> ""
            end,
            LinkID = wf:temp_id(),
            % Item won't hightlight if the link is clicked.
            wire_script(wf:f("$('.wfid_~s').click(function(e) { e.stopPropagation();});", [LinkID])),
            _Tile = #panel { 
                id=wire_click(item_id(Item), {action, select_item, [FullPath]}),
                style="width: 100px; height: 116px; overflow:hidden; position: relative; margin: 0; padding: 5px 10px; display: inline-block;", 
                body=case item_is_dir(Item) of
                    true ->
                        [
                            #panel { style="margin: 0 auto; text-align: center;", body=[
                                #image { style=ImageStyle, image="/images/folder64.png" }
                            ]},
                            #panel { style="margin: 5px auto 0; text-align: center; word-wrap: break-word;", body=[
                                #link { title=Filename, id=wire_click(LinkID, {action, navigate, [FullPath]}), text=Filename }
                            ]}
                        ];
                    false ->
                        ShareIcon = case item_is_shared(Item) of
                            true -> #span { style="font-size: 36px; position: absolute; top: 0px; left: 0; z-index: 1;", 
                                class="fui-link" };
                            false -> []
                        end,
                        [
                            #panel { style="margin: 0 auto; text-align: center;", body=
                                #panel { style="display: inline-block; position: relative;", body=
                                    [ShareIcon] ++
                                    [#image { style=ImageStyle, image="/images/file64.png" }]
                                }
                            },
                            #panel { style="margin: 5px auto 0; text-align: center; word-wrap: break-word;", body=[
                                #link { title=Filename, id=LinkID, text=Filename, new=true, 
                                    url="/user_content?f=" ++ wf:url_encode(FullPath)}
                            ]}
                        ]
                end 
            }
        end, get_key(item_list)),
    Body = case Tiles of
        [] -> #p { style="margin: 15px;", text="This directory is empty" };
        Other -> Other
    end,
    #panel { style="margin-top: 15px;", body=Body}.


% Render list view workspace
list_view_body() ->
    NumAttr = erlang:max(1, length(get_key(displayed_file_attributes))),
    CellWidth = "width: " ++ integer_to_list(round(90 * (2 + NumAttr) / NumAttr)) ++ "px;",
    HeaderTable = [
        #table { class="no-margin", style="position: fixed; top: 173px; z-index: 10;
        background: white; border: 2px solid #bbbdc0; border-collapse: collapse;", rows=[
            #tablerow { cells=
                [
                    #tableheader { style="border: 2px solid #aaacae;color: rgb(64, 89, 116);", text="Name" }
                ] ++ 
                lists:map(
                    fun(Attr) ->
                        #tableheader { style="border: 2px solid #aaacae;color: rgb(64, 89, 116);" ++ CellWidth, 
                            text=attr_to_name(Attr) }                
                    end, get_key(displayed_file_attributes))
            }
        ]}
    ],
    DirUpRow = case get_key(working_directory) of
        "/" -> [];
        Path -> 
            PrevDir = filename:dirname(filename:absname(Path)),
            Item = item_new(PrevDir),
            [
                #tablerow { cells=[
                    #tablecell { style="vertical-align: middle;", body=#span { style="word-wrap: break-word;", class="table-cell", body=[
                        #panel { style="display: inline-block; vertical-align: middle;", body=[
                            #link { id=wire_click({action, navigate, [PrevDir]}), body=
                                #image { class="list-icon", image="/images/folder32.png" } }
                        ]}, 
                        #panel { style="max-width: 230px; word-wrap: break-word; display: inline-block;vertical-align: middle;", body=[
                            #link { id=wire_click({action, navigate, [PrevDir]}), body="..&nbsp;&nbsp;&nbsp;" }
                        ]} 
                    ]}}] ++
                    lists:map(
                        fun(Attr) ->
                            #tablecell { style=CellWidth, class="table-cell", text=item_attr_string(Attr, Item) }            
                        end, get_key(displayed_file_attributes))
                }
            ]
    end,
    TableRows = DirUpRow ++ lists:map(
        fun(Item) ->
            FullPath = item_path(Item),
            Filename = item_basename(Item),
            ImageStyle = case get_key(clipboard_type) of
                cut ->
                    case lists:member(FullPath, get_key(clipboard_items)) of
                        true -> "opacity:0.3; filter:alpha(opacity=30);";
                        _ -> ""
                    end;
                _ -> ""
            end,
            LinkID = wf:temp_id(),
            % Item won't hightlight if the link is clicked.
            wire_script(wf:f("$('.wfid_~s').click(function(e) { e.stopPropagation();});", [LinkID])),
            ImageID = wf:temp_id(),
            % Item won't hightlight if the image is clicked.
            wire_script(wf:f("$('.wfid_~s').click(function(e) { e.stopPropagation();});", [ImageID])),
            _TableRow = #tablerow {
                id=wire_click(item_id(Item), {action, select_item, [FullPath]}),
                cells=[
                    case item_is_dir(Item) of
                        true -> 
                            #tablecell { style="vertical-align: middle;", body=#span { style="word-wrap: break-word;", class="table-cell", body=[
                                #panel { style="display: inline-block; vertical-align: middle;", body=[
                                    #link { id=wire_click(ImageID, {action, navigate, [FullPath]}), body=
                                        #image { class="list-icon", style=ImageStyle, image="/images/folder32.png" } }
                                ]}, 
                                #panel { class="filename_row", style="max-width: 400px; word-wrap: break-word; display: inline-block;vertical-align: middle;", body=[
                                    #link {id=wire_click(LinkID, {action, navigate, [FullPath]}), text=Filename }
                                ]} 
                            ]}};
                        false ->
                            ShareIcon = case item_is_shared(Item) of
                                true -> #span { style="font-size: 18px; position: absolute; top: 0px; left: 0; z-index: 1;
                                    color: rgb(82, 100, 118);", 
                                    class="fui-link" };
                                false -> []
                            end,
                            #tablecell { body=#span {  class="table-cell", body=[
                                #panel { style="display: inline-block; vertical-align: middle; position: relative;", body=
                                    #link { id=ImageID, new=true, url="/user_content?f=" ++ wf:url_encode(FullPath), body=[
                                        ShareIcon,
                                        #image { class="list-icon", style=ImageStyle, image="/images/file32.png" } 
                                    ]}
                                }, 
                                #panel { class="filename_row", style="word-wrap: break-word; display: inline-block;vertical-align: middle;", body=[
                                    #link { id=LinkID, text=Filename, new=true, 
                                        url="/user_content?f=" ++ wf:url_encode(FullPath)}
                                ]}
                            ]}}
                    end
                ] ++
                lists:map(
                    fun(Attr) ->
                        #tablecell { style=CellWidth, class="table-cell", text=item_attr_string(Attr, Item) }                
                    end, get_key(displayed_file_attributes))
            }
        end, get_key(item_list)),
        % Set filename containers width
        ContentWithoutFilename = 100 + (51 + round(90 * (2 + NumAttr) / NumAttr)) * NumAttr, % 51 is padding + border
        wire_script(wf:f("window.onresize = function(e) { $('.filename_row').css('max-width', '' + 
            ($(window).width() - ~B) + 'px'); }; $(window).resize();", [ContentWithoutFilename])),
    [
        HeaderTable,
        #table { id=main_table, class="table table-bordered", style="border-radius: 0; margin-top: 49px; margin-bottom: 0;", rows=TableRows }
    ].


start_upload_event(file_upload) ->
    %wf:flash("Upload started."),
    ok.


finish_upload_event(_Tag, undefined, _, _) ->
    %wf:flash("Please select a file."),
    ok;


finish_upload_event(_Tag, _FileName, _LocalFileData, _Node) ->
    %put(user_id, gui_utils:get_user_dn()),
    %reset_wire_accumulator(),
    %clear_workspace(),
    %do_wiring(),
    %wf:flash(wf:f("Uploaded file: ~s (~p bytes) on node ~s.", [FileName, FileSize, Node])),
    ok.



%% Item manipulation functions
item_new(Dir, File) ->
    FullPath = filename:absname(File, Dir),
    item_new(FullPath).

item_new(FullPath) ->
    FileAttr = fs_get_attributes(FullPath),
    IsShared = case logical_files_manager:get_share({file, FullPath}) of
        {ok, _} -> true;
        _ -> false
    end,
    #item { id=wf:temp_id(), path=FullPath, is_shared=IsShared, attr=FileAttr}.

item_find(Path) ->
    case lists:keyfind(Path, 3, get_key(item_list)) of
        false -> undefined;
        Item -> Item
    end.

item_is_dir(#item { attr=#fileattributes { type=Type } }) ->
    "DIR" =:= Type.

item_is_shared(#item { is_shared=IsShared }) ->
    IsShared.

item_id(#item { id=ID }) ->
    ID.

item_path(#item { path=Path }) ->
    Path.

item_basename(#item { path=Path }) ->
    filename:basename(Path).

item_dirname(#item { path=Path }) ->
    filename:dirname(Path).

item_attr(name, Item) -> item_basename(Item);
item_attr(mode, #item { attr=#fileattributes { mode=Value } }) -> Value;
item_attr(uid, #item { attr=#fileattributes { uid=Value } }) -> Value;
item_attr(gid, #item { attr=#fileattributes { gid=Value } }) -> Value;
item_attr(atime, #item { attr=#fileattributes { atime=Value } }) -> Value;
item_attr(mtime, #item { attr=#fileattributes { mtime=Value } }) -> Value;
item_attr(ctime, #item { attr=#fileattributes { ctime=Value } }) -> Value;
item_attr(type, #item { attr=#fileattributes { type=Value } }) -> Value;
item_attr(size, #item { attr=#fileattributes { size=Value } }) -> Value;
item_attr(uname, #item { attr=#fileattributes { uname=Value } }) -> Value;
item_attr(gname, #item { attr=#fileattributes { gname=Value } }) -> Value.

item_attr_string(name, Item) -> item_basename(Item);
item_attr_string(mode, Item) -> lists:flatten(io_lib:format("~.8B", [item_attr(mode, Item)]));
item_attr_string(uname, Item) -> item_attr(uname, Item);
item_attr_string(atime, Item) -> time_to_string(item_attr(atime, Item));
item_attr_string(mtime, Item) -> time_to_string(item_attr(mtime, Item));
item_attr_string(ctime, Item) -> time_to_string(item_attr(ctime, Item));
item_attr_string(size, Item) -> 
    case item_is_dir(Item) of 
        true -> "";
        false -> size_to_printable(item_attr(size, Item))
    end.

attr_to_name(name) -> "Name";
attr_to_name(size) -> "Size";
attr_to_name(mode) -> "Mode";
attr_to_name(uname) -> "Owner";
attr_to_name(atime) -> "Access";
attr_to_name(mtime) -> "Modification";
attr_to_name(ctime) -> "State change".

time_to_string(Time) ->
    Timestamp = { Time div 1000000, Time rem 1000000, 0},
    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Timestamp),
    io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
                  [YY, MM, DD, Hour, Min, Sec]).

size_to_printable(Size) ->
    size_to_printable(Size, ["B", "KB", "MB", "GB", "TB"]).

size_to_printable(Size, [Current|Bigger]) ->
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
        fun(#item{ path=Path, is_shared=Shared, attr=Attrs }, Acc) ->
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
            logical_files_manager:rmdir(DirPath).
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

fs_mv(Path, TargetPath) ->
    case filename:dirname(Path) of
        TargetPath -> ok;
        _ -> 
            case logical_files_manager:mv(Path, filename:absname(filename:basename(Path), TargetPath)) of
                ok -> ok;
                _ -> wf:wire(#alert { text="Unable to move " ++ filename:basename(Path) ++
                    ". File exists." })
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Convienience functions to get values from session memory.

put_key(Key, Value) ->
    wf:session(Key, Value).

put_key_if_undefined(Key, Value) ->
    case wf:session(Key) of
        undefined -> wf:session(Key, Value);
        _ -> skip
    end.

get_key(Key) ->
    case wf:session(Key) of
        undefined -> throw(session_error);
        Value -> Value
    end.

get_key_default(Key, Default) ->
    wf:session_default(Key, Default).

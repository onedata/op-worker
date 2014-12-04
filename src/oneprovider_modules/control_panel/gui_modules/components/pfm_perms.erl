%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains code for file_manager's permissions editor component.
%% It consists of several functions that render n2o elements and implement logic.
%% @end
%% ===================================================================
-module(pfm_perms).

-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/dao/dao_users.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/fslogic/fslogic_acl.hrl").
-include("fuse_messages_pb.hrl").
-include("files_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, perms_popup/1, file_manager_event/1, api_event/3]).
-export([populate_acl_list/1, change_perms_type/1, submit_perms/2, show_permissions_info/0]).
-export([edit_acl/1, move_acl/2, submit_acl/6, delete_acl/1]).
-export([fs_has_perms/2, fs_chmod/3, fs_get_acl/1, fs_set_acl/3]).


%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Initializes perms editor component and events connected with logic.
%% @end
-spec init() -> ok.
%% ====================================================================
init() ->
    gui_jq:wire(#api{name = "change_perms_type_event", tag = "change_perms_type_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "submit_perms_event", tag = "submit_perms_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "delete_acl_event", tag = "delete_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "edit_acl_event", tag = "edit_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "move_acl_event", tag = "move_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "submit_acl_event", tag = "submit_acl_event", delegate = ?MODULE}, false).


%% ====================================================================
%% Structural parts of perms editor component
%% ====================================================================

%% perms_popup/1
%% ====================================================================
%% @doc Renders perms editor body depending on selected files and their perms.
%% @end
-spec perms_popup([binary()]) -> [term()].
%% ====================================================================
perms_popup(Files) ->
    [FirstPath | Items] = Files,

    GetTypeAndValue =
        fun(ItemPath) ->
            Item = page_file_manager:item_find(ItemPath),
            case page_file_manager:item_attr(has_acl, Item) of
                true ->
                    {acl, fs_get_acl(ItemPath)};
                _ ->
                    {posix, page_file_manager:item_attr(perms, Item)}
            end
        end,
    {FirstType, FirstValue} = GetTypeAndValue(FirstPath),

    % CommonType can be undefined|acl|posix
    % CommonValue can be undefined|list()|integer()
    {CommonType, CommonValue} = lists:foldl(
        fun(ItemPath, {AccCommonType, AccCommonValue}) ->
            {ItemType, ItemValue} = GetTypeAndValue(ItemPath),
            case ItemType of
                AccCommonType ->
                    case ItemValue of
                        AccCommonValue -> {AccCommonType, AccCommonValue};
                        _ -> {AccCommonType, undefined}
                    end;
                _ ->
                    {undefined, undefined}
            end
        end, {FirstType, FirstValue}, Items),

    EnableACL = (CommonType =:= acl) or (CommonType =:= undefined),
    CommonPerms = case {CommonType, CommonValue} of
                      {posix, Int} when is_integer(Int) -> Int;
                      _ -> 0
                  end,
    CommonACL = case {CommonType, CommonValue} of
                    {acl, List} when is_list(List) -> List;
                    _ -> []
                end,
    set_files(Files),
    set_acl_enabled(EnableACL),
    set_acl_entries(CommonACL),

    PathToCheck = case FirstPath of
                      <<"/", ?SPACES_BASE_DIR_NAME>> -> <<"/">>;
                      _ -> FirstPath
                  end,
    {ok, FullFilePath} = fslogic_path:get_full_file_name(gui_str:binary_to_unicode_list(PathToCheck)),
    {ok, #space_info{users = Users, groups = Groups}} = fslogic_utils:get_space_info_for_path(FullFilePath),

    set_user_identifiers(uuids_to_identifiers(Users, true)),

    set_group_identifiers(uuids_to_identifiers(Groups, false)),

    gui_jq:wire(<<"init_chmod_table(", (integer_to_binary(CommonPerms))/binary, ");">>),
    {POSIXTabStyle, ACLTabStyle} = case EnableACL of
                                       true -> {<<"display: none;">>, <<"">>};
                                       false -> {<<"">>, <<"display: none;">>}
                                   end,
    Body = [
        #panel{id = <<"perms_wrapper">>, body = [
            #panel{id = <<"perms_header">>, body = [
                #p{id = <<"perms_header_info">>, body = <<"Permissions type:">>},
                #span{id = <<"perms_radios">>, body = [
                    #flatui_radio{id = <<"perms_radio_posix">>, name = <<"perms_radio">>,
                        label_class = <<"radio perms-radio-label">>, body = <<"POSIX">>, checked = not EnableACL},
                    #flatui_radio{id = <<"perms_radio_acl">>, name = <<"perms_radio">>,
                        label_class = <<"radio perms-radio-label">>, body = <<"ACL">>, checked = EnableACL}
                ]},
                #link{id = page_file_manager:wire_click(<<"perms_info_button">>, {action, ?MODULE, show_permissions_info}),
                    title = <<"Learn about permissions">>, class = <<"glyph-link">>,
                    body = #span{class = <<"icomoon-question">>}}
            ]},
            #panel{id = <<"tab_posix">>, style = POSIXTabStyle, body = [
                #table{class = <<"table table-bordered">>, id = <<"posix_table">>, header = [
                    #tr{cells = [
                        #th{body = <<"">>, class = <<"posix-cell">>},
                        #th{body = <<"read">>, class = <<"posix-cell">>},
                        #th{body = <<"write">>, class = <<"posix-cell">>},
                        #th{body = <<"execute">>, class = <<"posix-cell">>}
                    ]}
                ], body = #tbody{body = [
                    #tr{cells = [
                        #td{body = <<"user">>, class = <<"posix-cell fw700">>},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_ur">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_uw">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_ux">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]}
                    ]},
                    #tr{cells = [
                        #td{body = <<"group">>, class = <<"posix-cell fw700">>},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_gr">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_gw">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_gx">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]}
                    ]},
                    #tr{cells = [
                        #td{body = <<"other">>, class = <<"posix-cell fw700">>},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_or">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_ow">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]},
                        #td{class = <<"posix-cell">>, body = [
                            #flatui_checkbox{id = <<"chbx_ox">>,
                                label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                        ]}
                    ]}
                ]}
                },
                #panel{class = <<"posix-octal-form-wrapper">>, body = [
                    #p{class = <<"inline-block">>, body = <<"octal form:">>,
                        title = <<"Type in octal representation of perms to automatically adjust checkboxes">>},
                    #textbox{id = <<"posix_octal_form_textbox">>, class = <<"span2">>,
                        placeholder = <<"000">>, value = <<"">>}
                ]}
            ]},
            #panel{id = <<"tab_acl">>, style = ACLTabStyle, body = [
                #panel{class = <<"acl-info">>, body = [
                    #p{body = <<"proccessing">>},
                    #p{body = <<"order">>},
                    #span{class = <<"icomoon-arrow-down">>}
                ]},
                #panel{id = <<"acl_list">>},
                #panel{id = <<"acl-form">>, body = [
                    #table{id = <<"acl-form-table">>, body = [
                        #tr{cells = [
                            #td{body = [
                                #label{class = <<"label label-inverse acl-label">>, body = <<"Identifier">>}
                            ]},
                            #td{style = <<"padding-right: 20px;">>, body = identifiers_dropdown(<<"">>)}
                        ]},
                        #tr{cells = [
                            #td{body = [
                                #label{class = <<"label label-inverse acl-label">>, body = <<"Type">>}

                            ]},
                            #td{body = [
                                #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_type_checkbox">>,
                                    checked = true, body = #span{id = <<"acl_type_checkbox_label">>, body = <<"allow">>}}
                            ]}
                        ]},
                        #tr{cells = [
                            #td{body = [
                                #label{class = <<"label label-inverse acl-label">>, body = <<"Perms">>}

                            ]},
                            #td{body = [
                                #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_read_checkbox">>, checked = true, body = <<"read">>},
                                #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_write_checkbox">>, checked = true, body = <<"write">>},
                                #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_exec_checkbox">>, checked = true, body = <<"execute">>}
                            ]}
                        ]},
                        #tr{cells = [
                            #td{body = [
                                #button{id = <<"button_save_acl">>, class = <<"btn btn-success acl-form-button">>,
                                    body = <<"Save">>}
                            ]},
                            #td{body = [
                                #button{id = <<"button_discard_acl">>, class = <<"btn btn-danger acl-form-button">>,
                                    body = <<"Discard">>, postback = {action, ?MODULE, populate_acl_list, [-2]}}
                            ]}
                        ]}
                    ]}
                ]},
                #panel{class = <<"acl-info">>}
            ]}
        ]},
        #panel{class = <<"clearfix">>},
        #panel{id = <<"perms_warning_different">>, class = <<"perms-warning">>, body = [
            #span{class = <<"icomoon-warning">>},
            #p{body = <<"Selected files have different permissions. They will be overwritten by chosen permissions.">>}
        ]},
        #panel{id = <<"perms_warning_overwrite">>, class = <<"perms-warning">>, body = [
            #span{class = <<"icomoon-warning">>},
            #p{body = <<"Changing permissions recursively will overwrite <strong>ALL</strong> permissions in subdirectories.">>}
        ]},
        #form{class = <<"control-group">>, id = <<"perms_form">>, body = [
            #flatui_checkbox{id = <<"chbx_recursive">>, label_class = <<"checkbox">>,
                value = <<"">>, checked = false, body = <<"recursive">>,
                label_id = <<"perms_recursive_label">>,
                label_title = <<"Change perms in all subdirectories, recursively">>},
            #button{id = <<"ok_button">>, class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
            #button{class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>, postback = {action, hide_popup}}
        ]}
    ],
    flatui_checkbox:init_checkbox(<<"acl_type_checkbox">>),
    flatui_checkbox:init_checkbox(<<"acl_read_checkbox">>),
    flatui_checkbox:init_checkbox(<<"acl_write_checkbox">>),
    flatui_checkbox:init_checkbox(<<"acl_exec_checkbox">>),
    flatui_radio:init_radio_button(<<"perms_radio_posix">>),
    gui_jq:wire(<<"$('#acl_select_name').selectpicker({style: 'btn-small', menuStyle: 'dropdown-inverse'});">>),
    gui_jq:wire(<<"$('#perms_radio_acl').change(function(e){change_perms_type_event($(this).is(':checked'));});">>),
    flatui_radio:init_radio_button(<<"perms_radio_acl">>),
    gui_jq:bind_element_click(<<"button_save_acl">>, <<"function() { submit_acl(); }">>),
    gui_jq:bind_element_click(<<"ok_button">>, <<"function() { submit_perms(); }">>),
    case CommonValue of
        undefined -> gui_jq:show(<<"perms_warning_different">>);
        _ -> ok
    end,
    populate_acl_list(-2),
    Body.


%% identifiers_dropdown/1
%% ====================================================================
%% @doc Renders select box (dropdown) with identifiers for ACLs.
%% @end
-spec identifiers_dropdown(SelectedUUID :: binary()) -> #select{}.
%% ====================================================================
identifiers_dropdown(SelectedUUID) ->
    {UserOptions, _} = lists:foldl(
        fun({Ident, UUID}, {Acc, Counter}) ->
            Option = #option{body = Ident, selected = (UUID =:= SelectedUUID),
                value = <<"u", (integer_to_binary(Counter))/binary>>},
            {Acc ++ [Option], Counter + 1}
        end, {[], 1}, get_user_identifiers()),

    {GroupOptions, _} = lists:foldl(
        fun({Ident, UUID}, {Acc, Counter}) ->
            Option = #option{body = Ident, selected = (UUID =:= SelectedUUID),
                value = <<"g", (integer_to_binary(Counter))/binary>>},
            {Acc ++ [Option], Counter + 1}
        end, {[], 1}, get_group_identifiers()),

    #select{id = <<"acl_select_name">>, class = <<"select-block">>, body = [
        [
            #optgroup{label = <<"users">>, body = UserOptions},
            #optgroup{label = <<"groups">>, body = GroupOptions}
        ]
    ]}.


%% show_permissions_info/0
%% ====================================================================
%% @doc Displays a popup with info about file permissions.
%% @end
-spec show_permissions_info() -> ok.
%% ====================================================================
show_permissions_info() ->
    gui_jq:wire(<<"show_permissions_info();">>).


%% ====================================================================
%% Event handling
%% ====================================================================

%% api_event/3
%% ====================================================================
%% @doc n2o callback, called when a registered function is called from javascript.
%% @end
-spec api_event(Tag :: term(), Args :: list(), Ctx :: #context{}) -> term().
%% ====================================================================
api_event("submit_perms_event", Args, _Ctx) ->
    [Perms, Recursive] = mochijson2:decode(Args),
    file_manager_event({action, submit_perms, [Perms, Recursive]});

api_event("change_perms_type_event", Args, _Ctx) ->
    EnableACL = mochijson2:decode(Args),
    file_manager_event({action, change_perms_type, [EnableACL]});

api_event("delete_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    file_manager_event({action, delete_acl, [Index]});

api_event("edit_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    file_manager_event({action, edit_acl, [Index]});

api_event("move_acl_event", Args, _) ->
    [IndexRaw, MoveUp] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    file_manager_event({action, move_acl, [Index, MoveUp]});

api_event("submit_acl_event", Args, _) ->
    [IndexRaw, SelectedIdentifier, Type, Read, Write, Execute] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    file_manager_event({action, submit_acl, [Index, SelectedIdentifier, Type, Read, Write, Execute]}).


%% event/1
%% ====================================================================
%% @doc n2o callback, called when a postback is generated.
%% @end
-spec file_manager_event(Tag :: term()) -> term().
%% ====================================================================
file_manager_event({action, Fun}) ->
    page_file_manager:event({action, ?MODULE, Fun, []});


file_manager_event({action, Fun, Args}) ->
    page_file_manager:event({action, ?MODULE, Fun, Args}).


%% ====================================================================
%% ACL editor logic
%% ====================================================================

%% submit_perms/2
%% ====================================================================
%% @doc Called when user submits file permissions form.
%% @end
-spec submit_perms(Perms :: integer(), Recursive :: boolean()) -> term().
%% ====================================================================
submit_perms(Perms, Recursive) ->
    Files = get_files(),
    ACLEnabled = get_acl_enabled(),
    ACLEntries = get_acl_entries(),
    {Failed, Message} = case ACLEnabled of
                            true ->
                                FailedFiles = lists:foldl(
                                    fun(Path, Acc) ->
                                        {_Successful, Failed} = fs_set_acl(Path, ACLEntries, Recursive),
                                        Acc ++ Failed
                                    end, [], Files),
                                {FailedFiles, <<"Unable to set ACL for following file(s):">>};
                            false ->
                                FailedFiles = lists:foldl(
                                    fun(Path, Acc) ->
                                        {_Successful, Failed} = fs_chmod(Path, Perms, Recursive),
                                        Acc ++ Failed
                                    end, [], Files),
                                {FailedFiles, <<"Unable to change permissions for following file(s):">>}
                        end,
    case Failed of
        [] ->
            ok;
        _ ->
            FailedList = lists:foldl(
                fun({Path, Reason}, Acc) ->
                    ReasonBin = case Reason of
                                    {logical_file_system_error, "eacces"} ->
                                        <<"insufficient permissions">>;
                                    _ ->
                                        <<"error occured">>
                                end,
                    <<Acc/binary, Path/binary, ": ", ReasonBin/binary, "<br />">>
                end, <<"">>, Failed),
            gui_jq:info_popup(<<"Error(s) occured">>,
                <<Message/binary, "<br /><br />", FailedList/binary>>, <<"">>)
    end,
    page_file_manager:clear_manager().


%% populate_acl_list/1
%% ====================================================================
%% @doc Renders ACL list.
%% @end
-spec populate_acl_list(SelectedIndex :: integer()) -> term().
%% ====================================================================
populate_acl_list(SelectedIndex) ->
    ACLEntries = get_acl_entries(),

    JSON = rest_utils:encode_to_json(lists:map(
        fun(#accesscontrolentity{acetype = ACEType, aceflags = ACEFlags, identifier = Identifier, acemask = ACEMask}) ->
            IsGroup = ACEFlags band ?identifier_group_mask > 0,
            {Name, Identifier} = lists:keyfind(Identifier, 2,
                case IsGroup of true -> get_group_identifiers(); false -> get_user_identifiers() end),
            [
                {<<"identifier">>, Name},
                {<<"is_group">>, IsGroup},
                {<<"allow">>, ACEType =:= ?allow_mask},
                {<<"read">>, ACEMask band ?read_mask > 0},
                {<<"write">>, ACEMask band ?write_mask > 0},
                {<<"exec">>, ACEMask band ?execute_mask > 0}
            ]
        end, ACLEntries)),
    gui_jq:wire(<<"clicked_index = -2;">>),
    gui_jq:wire(<<"populate_acl_list(", JSON/binary, ", ", (integer_to_binary(SelectedIndex))/binary, ");">>).


%% change_perms_type/1
%% ====================================================================
%% @doc Called when user changes the state of perms type radio button.
%% @end
-spec change_perms_type(EnableACL :: boolean()) -> term().
%% ====================================================================
change_perms_type(EnableACL) ->
    set_acl_enabled(EnableACL),
    case EnableACL of
        true ->
            gui_jq:show(<<"tab_acl">>),
            gui_jq:hide(<<"tab_posix">>);
        _ ->
            gui_jq:hide(<<"tab_acl">>),
            gui_jq:show(<<"tab_posix">>)
    end.


%% delete_acl/1
%% ====================================================================
%% @doc Called when user confirms deletion of an ACL entry.
%% @end
-spec delete_acl(Index :: integer()) -> term().
%% ====================================================================
delete_acl(Index) ->
    ACLEntries = get_acl_entries(),
    {Head, [_ | Tail]} = lists:split(Index, ACLEntries),
    set_acl_entries(Head ++ Tail),
    case length(get_acl_entries()) of
        0 -> populate_acl_list(-2);
        Len when Len > Index -> populate_acl_list(Index);
        _ -> populate_acl_list(Index - 1)
    end.


%% edit_acl/1
%% ====================================================================
%% @doc Called when user select an entry to edit.
%% @end
-spec edit_acl(Index :: integer()) -> term().
%% ====================================================================
edit_acl(Index) ->
    gui_jq:show(<<"acl-form">>),
    case Index of
        -1 ->
            gui_jq:update(<<"acl_select_name">>, identifiers_dropdown(<<"">>)),
            gui_jq:wire(<<"$('#acl_type_checkbox').checkbox('check');">>),
            gui_jq:wire(<<"$('#acl_read_checkbox').checkbox('uncheck');">>),
            gui_jq:wire(<<"$('#acl_write_checkbox').checkbox('uncheck');">>),
            gui_jq:wire(<<"$('#acl_exec_checkbox').checkbox('uncheck');">>);
        _ ->
            ACLEntries = get_acl_entries(),
            #accesscontrolentity{acetype = ACEType, aceflags = _ACEFlags,
                identifier = Identifier, acemask = ACEMask} = lists:nth(Index + 1, ACLEntries),
            gui_jq:update(<<"acl_select_name">>, identifiers_dropdown(Identifier)),
            CheckJS = <<"').checkbox('check');">>,
            UncheckJS = <<"').checkbox('uncheck');">>,
            case ACEType of
                ?allow_mask -> gui_jq:wire(<<"$('#acl_type_checkbox", CheckJS/binary>>);
                _ -> gui_jq:wire(<<"$('#acl_type_checkbox", UncheckJS/binary>>)
            end,
            case ACEMask band ?read_mask of
                0 -> gui_jq:wire(<<"$('#acl_read_checkbox", UncheckJS/binary>>);
                _ -> gui_jq:wire(<<"$('#acl_read_checkbox", CheckJS/binary>>)
            end,
            case ACEMask band ?write_mask of
                0 -> gui_jq:wire(<<"$('#acl_write_checkbox", UncheckJS/binary>>);
                _ -> gui_jq:wire(<<"$('#acl_write_checkbox", CheckJS/binary>>)
            end,
            case ACEMask band ?execute_mask of
                0 -> gui_jq:wire(<<"$('#acl_exec_checkbox", UncheckJS/binary>>);
                _ -> gui_jq:wire(<<"$('#acl_exec_checkbox", CheckJS/binary>>)
            end


    end.


%% submit_acl/6
%% ====================================================================
%% @doc Called when user submits ACL form.
%% @end
-spec submit_acl(Index :: integer(), SelectedIdentifier :: binary(), Type :: boolean(),
    ReadFlag :: boolean(), WriteFlag :: boolean(), ExecFlag :: boolean()) -> term().
%% ====================================================================
submit_acl(Index, SelectedIdentifier, Type, ReadFlag, WriteFlag, ExecFlag) ->
    {IdentifierUUID, IsGroup} = case SelectedIdentifier of
                                    <<"u", Number/binary>> ->
                                        {_, ID} = lists:nth(binary_to_integer(Number), get_user_identifiers()),
                                        {ID, false};
                                    <<"g", Number/binary>> ->
                                        {_, ID} = lists:nth(binary_to_integer(Number), get_group_identifiers()),
                                        {ID, true}
                                end,
    ACLEntries = get_acl_entries(),
    ACEMask = (case ReadFlag of true -> ?read_mask; _ -> 0 end) bor
        (case WriteFlag of true -> ?write_mask; _ -> 0 end) bor
        (case ExecFlag of true -> ?execute_mask; _ -> 0 end),
    case ACEMask of
        0 ->
            gui_jq:info_popup(<<"Invalid values">>,
                <<"Acess List entry must allow or deny at least one permission.">>, <<"">>);
        _ ->
            NewEntity = #accesscontrolentity{
                acetype = (case Type of true -> ?allow_mask; _ -> ?deny_mask end),
                aceflags = case IsGroup of true -> ?identifier_group_mask; false -> ?no_flags_mask end,
                identifier = IdentifierUUID,
                acemask = ACEMask},
            SelectIndex = case Index of
                              -1 ->
                                  set_acl_entries(ACLEntries ++ [NewEntity]),
                                  length(get_acl_entries()) - 1;
                              _ ->
                                  {Head, [_Ident | Tail]} = lists:split(Index, ACLEntries),
                                  set_acl_entries(Head ++ [NewEntity] ++ Tail),
                                  Index
                          end,
            populate_acl_list(SelectIndex)
    end.


%% move_acl/1
%% ====================================================================
%% @doc Called when user pushes an ACL entry up or down.
%% @end
-spec move_acl(Index :: integer(), MoveUp :: boolean()) -> term().
%% ====================================================================
move_acl(Index, MoveUp) ->
    ACLEntries = get_acl_entries(),
    MaxIndex = length(ACLEntries) - 1,
    {NewEntries, SelectedIndex} = case {Index, MoveUp} of
                                      {0, true} ->
                                          {ACLEntries, Index};
                                      {MaxIndex, false} ->
                                          {ACLEntries, Index};
                                      _ ->
                                          {Head, [Ident | Tail]} = lists:split(Index, ACLEntries),
                                          case MoveUp of
                                              true ->
                                                  % Head length is at least 1, because Index is not 0
                                                  {AllButLast, Last} = lists:split(length(Head) - 1, Head),
                                                  {AllButLast ++ [Ident] ++ Last ++ Tail, Index - 1};
                                              false ->
                                                  % Tail length is at least 1, because Index is not MaxIndex
                                                  [First | AllButFirst] = Tail,
                                                  {Head ++ [First] ++ [Ident] ++ AllButFirst, Index + 1}
                                          end
                                  end,
    set_acl_entries(NewEntries),
    populate_acl_list(SelectedIndex).


%% uuids_to_identifiers/2
%% ====================================================================
%% @doc Converts uuids to sorted pairs {Name, UUID}. If names repeat,
%% a sufficient part of hash is concatenated so the names can be distinguished.
%% Removes duplicate UUIDs.
%% @end
-spec uuids_to_identifiers(UUIDs :: [binary()], ForUsers :: boolean()) -> [{Name :: binary(), UUID :: binary()}].
%% ====================================================================
uuids_to_identifiers(UUIDs, ForUsers) ->
    NamesWithGRUIDs = lists:map(
        fun(UUID) ->
            Name = case ForUsers of
                       true ->
                           {ok, #db_document{record = #user{name = UserName}}} = fslogic_objects:get_user({global_id, UUID}),
                           gui_str:unicode_list_to_binary(UserName);
                       false ->
                           {ok, #db_document{record = #group_details{name = GroupName}}} = dao_lib:apply(dao_groups, get_group, [UUID], 1),
                           GroupName
                   end,
            {Name, UUID}
        end, lists:usort(UUIDs)), % remove duplicates of UUIDS
    SortedNames = lists:keysort(1, NamesWithGRUIDs),
    {_, Identifiers} = lists:foldl(fun({Name, UUID}, {Temp, Acc}) ->
        case Temp of
            [] ->
                {[{Name, UUID}], Acc};
            [{FirstTemp, FirstGRUID} | _] ->
                case Name of
                    FirstTemp ->
                        {Temp ++ [{Name, UUID}], Acc};
                    _ ->
                        case length(Temp) of
                            1 ->
                                {[{Name, UUID}], Acc ++ [{FirstTemp, FirstGRUID}]};
                            _ ->
                                HashLength = calculate_hash_length(element(2, lists:unzip(Temp))),
                                {[{Name, UUID}], Acc ++ lists:map(
                                    fun({CName, CUUID}) ->
                                        {<<CName/binary, " (", (binary_part(CUUID, 0, HashLength))/binary, "...)">>, CUUID}
                                    end, Temp)}
                        end
                end
        end
    end, {[], []}, SortedNames ++ [{<<"">>, <<"">>}]),
    Identifiers.


%% calculate_hash_length/1
%% ====================================================================
%% @doc Calculates the length of hash to be extracted so gives UUISd can be
%% distinguished. Returns the longest common prefix of any two UUIDs + 2.
%% @end
-spec calculate_hash_length(UUIDs :: [binary()]) -> integer().
%% ====================================================================
calculate_hash_length(UUIDs) ->
    MaxCommonPrefixes = lists:map(
        fun(UUID) ->
            CommonPrefixes = lists:map(
                fun(RefUUID) ->
                    binary:longest_common_prefix([UUID, RefUUID])
                end, [Elem || Elem <- UUIDs, Elem /= UUID]),
            lists:max([0 | CommonPrefixes])
        end, UUIDs),
    lists:max([0 | MaxCommonPrefixes]) + 2.


%% ====================================================================
%% logical_files_manager interfacing
%% ====================================================================

%% fs_chmod/3
%% ====================================================================
%% @doc Changes mode for given path, and possibly recursively in all subpaths.
%% Returns a tuple {Successful, Failed}, where Succesfull is a list of
%% paths for which command succeded and Failed is a list of tuples {Path, Reason}
%% for paths that the command failed.
%% @end
-spec fs_chmod(Path :: binary(), Perms :: integer(), Recursive :: boolean()) -> {Successful :: [binary()], Failed :: [{binary(), term()}]}.
%% ====================================================================
fs_chmod(Path, Perms, Recursive) ->
    fs_chmod(Path, Perms, Recursive, {[], []}).

fs_chmod(Path, Perms, Recursive, {Successful, Failed}) ->
    IsDir = page_file_manager:item_is_dir(Path),
    {NewSuccessful, NewFailed} =
        case Recursive of
            false ->
                case logical_files_manager:change_file_perm(gui_str:binary_to_unicode_list(Path), Perms, not IsDir) of
                    ok -> {[Path], []};
                    Err1 -> {[], [{Path, Err1}]}
                end;
            true ->
                case logical_files_manager:change_file_perm(gui_str:binary_to_unicode_list(Path), Perms, not IsDir) of
                    ok ->
                        case IsDir of
                            false ->
                                {[Path], []};
                            true ->
                                lists:foldl(
                                    fun(ItemPath, {SuccAcc, FailAcc}) ->
                                        {Succ, Fail} = fs_chmod(ItemPath, Perms, Recursive),
                                        {SuccAcc ++ Succ, FailAcc ++ Fail}
                                    end, {[Path], []}, page_file_manager:fs_list_dir(Path))
                        end;
                    Err3 ->
                        {[], [{Path, Err3}]}
                end
        end,
    {Successful ++ NewSuccessful, Failed ++ NewFailed}.


%% fs_has_perms/2
%% ====================================================================
%% @doc Checks if the user has rights to perform given operation on given path.
%% @end
-spec fs_has_perms(Path :: binary(), CheckType :: root | owner | delete | read | write | execute | rdwr | '') -> boolean().
%% ====================================================================
fs_has_perms(Path, CheckType) ->
    logical_files_manager:check_file_perm(gui_str:binary_to_unicode_list(Path), CheckType).


%% fs_get_acl/1
%% ====================================================================
%% @doc Retrieves ACL entry list for given path.
%% @end
-spec fs_get_acl(Path :: binary()) -> [#accesscontrolentity{}].
%% ====================================================================
fs_get_acl(Path) ->
    case logical_files_manager:get_acl(gui_str:binary_to_unicode_list(Path)) of
        {ok, List} -> List;
        _ -> []
    end.


%% fs_set_acl/3
%% ====================================================================
%% @doc Changes ACL entry list for given path, and possibly recursively in all subpaths.
%% Returns a tuple {Successful, Failed}, where Succesfull is a list of
%% paths for which command succeded and Failed is a list of tuples {Path, Reason}
%% for paths that the command failed.
%% @end
-spec fs_set_acl(Path :: binary(), ACLEntries :: [#accesscontrolentity{}], Recursive :: boolean()) -> {Successful :: [binary()], Failed :: [{binary(), term()}]}.
%% ====================================================================
fs_set_acl(Path, ACLEntries, Recursive) ->
    fs_set_acl(Path, ACLEntries, Recursive, {[], []}).

fs_set_acl(Path, ACLEntries, Recursive, {Successful, Failed}) ->
    IsDir = page_file_manager:item_is_dir(Path),
    {NewSuccessful, NewFailed} =
        case Recursive of
            false ->
                case logical_files_manager:set_acl(gui_str:binary_to_unicode_list(Path), ACLEntries) of
                    ok -> {[Path], []};
                    Err1 -> {[], [{Path, Err1}]}
                end;
            true ->
                case logical_files_manager:set_acl(gui_str:binary_to_unicode_list(Path), ACLEntries) of
                    ok ->
                        case IsDir of
                            false ->
                                {[Path], []};
                            true ->
                                lists:foldl(
                                    fun(ItemPath, {SuccAcc, FailAcc}) ->
                                        {Succ, Fail} = fs_set_acl(ItemPath, ACLEntries, Recursive),
                                        {SuccAcc ++ Succ, FailAcc ++ Fail}
                                    end, {[Path], []}, page_file_manager:fs_list_dir_to_paths(Path))
                        end;
                    Err3 ->
                        {[], [{Path, Err3}]}
                end
        end,
    {Successful ++ NewSuccessful, Failed ++ NewFailed}.


%% ====================================================================
%% Convenience functions to remember page state in process dictionary
%% ====================================================================

%% set_files/1
%% ====================================================================
%% @doc Saves information what files' perms are being edited.
%% @end
-spec set_files(Files :: [binary()]) -> term().
%% ====================================================================
set_files(Files) -> put(perms_files, Files).


%% get_files/0
%% ====================================================================
%% @doc Retrieves information what files' perms are being edited.
%% @end
-spec get_files() -> [binary()] | term().
%% ====================================================================
get_files() -> get(perms_files).


%% set_acl_enabled/1
%% ====================================================================
%% @doc Saves information if ACL permissions are enabled.
%% @end
-spec set_acl_enabled(Flag :: boolean()) -> term().
%% ====================================================================
set_acl_enabled(Flag) -> put(acl_enabled, Flag).


%% get_acl_enabled/0
%% ====================================================================
%% @doc Retrieves information if ACL permissions are enabled.
%% @end
-spec get_acl_enabled() -> boolean() | term().
%% ====================================================================
get_acl_enabled() -> get(acl_enabled).


%% set_acl_entries/1
%% ====================================================================
%% @doc Saves current ACL entry list.
%% @end
-spec set_acl_entries(Entries :: [#accesscontrolentity{}]) -> term().
%% ====================================================================
set_acl_entries(Entries) -> put(acl_entries, Entries).


%% get_acl_entries/0
%% ====================================================================
%% @doc Retrieves current ACL entry list.
%% @end
-spec get_acl_entries() -> boolean() | term().
%% ====================================================================
get_acl_entries() -> get(acl_entries).


%% set_user_identifiers/1
%% ====================================================================
%% @doc Saves user IDs available for ACL for current file(s).
%% @end
-spec set_user_identifiers(Identifiers :: [binary()]) -> term().
%% ====================================================================
set_user_identifiers(Identifiers) -> put(acl_user_ids, Identifiers).


%% get_user_identifiers/0
%% ====================================================================
%% @doc Retrieves user IDs available for ACL for current file(s).
%% @end
-spec get_user_identifiers() -> boolean() | term().
%% ====================================================================
get_user_identifiers() -> get(acl_user_ids).


%% set_group_identifiers/1
%% ====================================================================
%% @doc Saves group IDs available for ACL for current file(s).
%% @end
-spec set_group_identifiers(Identifiers :: [binary()]) -> term().
%% ====================================================================
set_group_identifiers(Identifiers) -> put(acl_group_ids, Identifiers).


%% get_group_identifiers/0
%% ====================================================================
%% @doc Retrieves group IDs available for ACL for current file(s).
%% @end
-spec get_group_identifiers() -> boolean() | term().
%% ====================================================================
get_group_identifiers() -> get(acl_group_ids).
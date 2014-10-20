%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains code for file_manager permissions editor component.
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

%% API
-export([init/0, perms_popup/1, event/1, api_event/3]).
-export([populate_acl_list/1, change_perms_type/1, submit_perms/2, show_permissions_info/0]).
-export([add_acl/0, delete_acl/1, edit_acl/1, move_acl/2, submit_acl/6]).
-export([fs_has_perms/2, fs_chmod/3, fs_get_acl/1, fs_set_acl/3]).


%% init/0
%% ====================================================================
%% @doc Initializes perms editor component.
%% @end
-spec init() -> ok.
%% ====================================================================
init() ->
    gui_jq:wire(#api{name = "change_perms_type_event", tag = "change_perms_type_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "submit_perms_event", tag = "submit_perms_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "add_acl_event", tag = "add_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "delete_acl_event", tag = "delete_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "edit_acl_event", tag = "edit_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "move_acl_event", tag = "move_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "submit_acl_event", tag = "submit_acl_event", delegate = ?MODULE}, false).


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
    set_perms_state({Files, EnableACL, CommonACL}),

    PathToCheck = case FirstPath of
                      <<"/", ?SPACES_BASE_DIR_NAME>> -> <<"/">>;
                      _ -> FirstPath
                  end,
    {ok, FullFilePath} = fslogic_path:get_full_file_name(gui_str:binary_to_unicode_list(PathToCheck)),
    {ok, #space_info{users = Users}} = fslogic_utils:get_space_info_for_path(FullFilePath),
    Identifiers = gruids_to_identifiers(Users),

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
                #link{id = page_file_manager:wire_click(<<"perms_info_button">>, {action, pfm_perms, show_permissions_info}),
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
                            #td{style = <<"padding-right: 20px;">>, body = [
                                #select{id = <<"acl_select_name">>, class = <<"select-block">>, body = [
                                    lists:map(fun(Ident) -> #option{body = Ident} end, Identifiers)
                                ]}
                            ]}
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
                                    body = <<"Discard">>, postback = {action, populate_acl_list, [-1]}}
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
    populate_acl_list(-1),
    Body.


show_permissions_info() ->
    gui_jq:info_popup(<<"POSIX permissions and ACLs">>, <<"Basic POSIX permissions and ACLs are two ways of controlling ",
    "the access to your data. You can choose to use one of them for each file. They cannot be used together. <br /><br />",
    "<strong>POSIX permissions</strong> - basic file permissions, can be used to enable certain types ",
    "of users to read, write or execute given file. The types are: user (the owner of the file), group (all users ",
    "sharing the space where the file resides), other (not aplicable in GUI, but used in oneclient).<br /><br />",
    "<strong>ACL</strong> (Access Control List) - CDMI standard (compliant with NFSv4 ACLs), allows ",
    "defining ordered lists of permissions-granting or permissions-denying entries for users or groups. ",
    "ACLs are processed from top to bottom - entries higher on list will have higher priority.">>, <<"">>).


api_event("submit_perms_event", Args, _Ctx) ->
    [Perms, Recursive] = mochijson2:decode(Args),
    eval_in_comet(submit_perms, [Perms, Recursive]);

api_event("change_perms_type_event", Args, _Ctx) ->
    EnableACL = mochijson2:decode(Args),
    eval_in_comet(change_perms_type, [EnableACL]);

api_event("add_acl_event", _Args, _) ->
    eval_in_comet(add_acl, []);

api_event("delete_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(delete_acl, [Index]);

api_event("edit_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(edit_acl, [Index]);

api_event("move_acl_event", Args, _) ->
    [IndexRaw, MoveUp] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(move_acl, [Index, MoveUp]);

api_event("submit_acl_event", Args, _) ->
    [IndexRaw, Identifier, Type, Read, Write, Execute] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(submit_acl, [Index, Identifier, Type, Read, Write, Execute]).


event({action, Fun}) ->
    event({action, Fun, []});


event({action, Fun, Args}) ->
    eval_in_comet(Fun, Args).


eval_in_comet(Fun, Args) ->
    opn_gui_utils:apply_or_redirect(erlang, send, [get(comet_pid), {action, ?MODULE, Fun, Args}]).


submit_perms(Perms, Recursive) ->
    {Files, ACLEnabled, ACLEntries} = get_perms_state(),
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


populate_acl_list(SelectedIndex) ->
    {_Files, _EnableACL, ACLEntries} = get_perms_state(),
    JSON = rest_utils:encode_to_json(lists:map(
        fun(#accesscontrolentity{acetype = ACEType, aceflags = _ACEFlags, identifier = Identifier, acemask = ACEMask}) ->
            {ok, #db_document{record = #user{name = Name}}} = fslogic_objects:get_user({global_id, Identifier}),
            [
                {<<"identifier">>, gui_str:unicode_list_to_binary(Name)},
                {<<"allow">>, ACEType =:= ?allow_mask},
                {<<"read">>, ACEMask band ?read_mask > 0},
                {<<"write">>, ACEMask band ?write_mask > 0},
                {<<"exec">>, ACEMask band ?execute_mask > 0}
            ]
        end, ACLEntries)),
    gui_jq:hide(<<"acl-form">>),
    gui_jq:wire(<<"clicked_index = -2;">>),
    gui_jq:wire(<<"populate_acl_list(", JSON/binary, ", ", (integer_to_binary(SelectedIndex))/binary, ");">>).


change_perms_type(EnableACL) ->
    {Files, _CurrentEnableACL, ACLEntries} = get_perms_state(),
    set_perms_state({Files, EnableACL, ACLEntries}),
    case EnableACL of
        true ->
            gui_jq:show(<<"tab_acl">>),
            gui_jq:hide(<<"tab_posix">>);
        _ ->
            gui_jq:hide(<<"tab_acl">>),
            gui_jq:show(<<"tab_posix">>)
    end.


add_acl() ->
    gui_jq:show(<<"acl-form">>),
    gui_jq:wire(<<"$('#acl_textbox').val('');">>),
    gui_jq:wire(<<"$('#acl_type_checkbox').checkbox('check');">>),
    gui_jq:wire(<<"$('#acl_read_checkbox').checkbox('uncheck');">>),
    gui_jq:wire(<<"$('#acl_write_checkbox').checkbox('uncheck');">>),
    gui_jq:wire(<<"$('#acl_exec_checkbox').checkbox('uncheck');">>).


delete_acl(Index) ->
    {Files, EnableACL, ACLEntries} = get_perms_state(),
    {Head, [_ | Tail]} = lists:split(Index, ACLEntries),
    set_perms_state({Files, EnableACL, Head ++ Tail}),
    populate_acl_list(-1).


edit_acl(Index) ->
    gui_jq:show(<<"acl-form">>),
    {_Files, _EnableACL, ACLEntries} = get_perms_state(),
    #accesscontrolentity{acetype = ACEType, aceflags = _ACEFlags,
        identifier = Identifier, acemask = ACEMask} = lists:nth(Index + 1, ACLEntries),
    gui_jq:set_value(<<"acl_textbox">>, Identifier),
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
    end.


submit_acl(Index, Name, Type, ReadFlag, WriteFlag, ExecFlag) ->
    {Files, EnableACL, ACLEntries} = get_perms_state(),
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
                aceflags = ?no_flags_mask,
                identifier = fslogic_acl:name_to_gruid(Name),
                acemask = ACEMask},
            case Index of
                -1 ->
                    set_perms_state({Files, EnableACL, ACLEntries ++ [NewEntity]});
                _ ->
                    {Head, [_Ident | Tail]} = lists:split(Index, ACLEntries),
                    set_perms_state({Files, EnableACL, Head ++ [NewEntity] ++ Tail})
            end,
            populate_acl_list(-1)
    end.


move_acl(Index, MoveUp) ->
    {Files, EnableACL, ACLEntries} = get_perms_state(),
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
    set_perms_state({Files, EnableACL, NewEntries}),
    populate_acl_list(SelectedIndex).


gruids_to_identifiers(GRUIDs) ->
    NamesWithGRUIDs = lists:map(
        fun(GRUID) ->
            {ok, #db_document{record = #user{name = Name}}} = fslogic_objects:get_user({global_id, GRUID}),
            {Name, GRUID}
        end, GRUIDs),
    SortedNames = lists:keysort(1, NamesWithGRUIDs),
    {_, Identifiers} = lists:foldl(fun({Name, GRUID}, {Temp, Acc}) ->
        case Temp of
            [] ->
                {[{Name, GRUID}], Acc};
            [{FirstTemp, _} | _] ->
                case Name of
                    FirstTemp ->
                        {Temp ++ [{Name, GRUID}], Acc};
                    _ ->
                        case length(Temp) of
                            1 ->
                                {[{Name, GRUID}], Acc ++ [gui_str:unicode_list_to_binary(FirstTemp)]};
                            _ ->
                                {[{Name, GRUID}], Acc ++ lists:map(
                                    fun({Nam, GRU}) ->
                                        <<(gui_str:unicode_list_to_binary(Nam))/binary, "#", GRU/binary>>
                                    end, Temp)}
                        end
                end
        end
    end, {[], []}, SortedNames ++ [{<<"">>, <<"">>}]),
    Identifiers.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% logical_files_manager interfacing

% Returns a tuple {Successful, Failed}, where Succesfull is a list of
% paths for which command succeded and Failed is a list of tuples {Path, Reason}
% for paths that the command failed.
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


fs_has_perms(Path, CheckType) ->
    logical_files_manager:check_file_perm(gui_str:binary_to_unicode_list(Path), CheckType).

fs_get_acl(Path) ->
    case logical_files_manager:get_acl(gui_str:binary_to_unicode_list(Path)) of
        {ok, List} -> List;
        _ -> []
    end.

% Returns a tuple {Successful, Failed}, where Succesfull is a list of
% paths for which command succeded and Failed is a list of tuples {Path, Reason}
% for paths that the command failed.
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


% Holds information what files' ACLs are being edited and what is the current state
% Stored term is in the following form:
% {Files, EnableACL, ACLEntries, Users, Groups}
% Files - list of paths, for which perms are being changed
% EnableACL - boolean(), determines if POSIX or ACL is selected
% ACLEntries - current list of ACL entries
% Users - possible ACL Identifiers of users
% Groups - possible ACL Identifiers of groups
set_perms_state(State) -> put(acl_state, State).
get_perms_state() -> get(acl_state).

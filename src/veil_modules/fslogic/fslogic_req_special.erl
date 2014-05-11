%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Write me !
%% @end
%% ===================================================================
-module(fslogic_req_special).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("logging.hrl").

%% API
-export([create_dir/2, get_file_children/3, create_link/2]).

%% ====================================================================
%% API functions
%% ====================================================================

create_dir(FullFileName, Mode) ->
    {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(FullFileName, fslogic_context:get_protocol_version()),
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    case ParentFound of
        ok ->
            {FileName, Parent} = ParentInfo,
            {PermsStat, PermsOK} = fslogic_utils:check_file_perms(FullFileName, UserDocStatus, UserDoc, Parent, write),
            case PermsStat of
                ok ->
                    case PermsOK of
                        true ->
                            {UserIdStatus, UserId} = fslogic_utils:get_user_id(),
                            case UserIdStatus of
                                ok ->
                                    Groups = fslogic_utils:get_group_owner(fslogic_utils:get_user_file_name(FullFileName)), %% Get owner group name based on file access path

                                    FileInit = #file{type = ?DIR_TYPE, name = FileName, uid = UserId, gids = Groups, parent = Parent#veil_document.uuid, perms = Mode},
                                    %% Async *times update
                                    CTime = fslogic_utils:time(),
                                    File = fslogic_utils:update_meta_attr(FileInit, times, {CTime, CTime, CTime}),

                                    {Status, TmpAns} = dao_lib:apply(dao_vfs, save_new_file, [FullFileName, File], fslogic_context:get_protocol_version()),
                                    case {Status, TmpAns} of
                                        {ok, _} ->
                                            fslogic_utils:update_parent_ctime(fslogic_utils:get_user_file_name(FullFileName), CTime),
                                            #atom{value = ?VOK};
                                        {error, file_exists} ->
                                            #atom{value = ?VEEXIST};
                                        _BadStatus ->
                                            lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, error: ~p", [FullFileName, _BadStatus]),
                                            #atom{value = ?VEREMOTEIO}
                                    end;
                                _ -> #atom{value = ?VEREMOTEIO}
                            end;
                        false ->
                            lager:warning("Creating directory without permissions: ~p", [FileName]),
                            #atom{value = ?VEPERM}
                    end;
                _ ->
                    lager:warning("Cannot create directory. Reason: ~p:~p", [PermsStat, PermsOK]),
                    #atom{value = ?VEREMOTEIO}
            end;
        _ParentError ->
            lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, parentInfo: ~p", [FullFileName, _ParentError]),
            #atom{value = ?VEREMOTEIO}
    end.

get_file_children(FullFileName, ROffset, RCount) ->
    UserFilePath = fslogic_utils:get_user_file_name(FullFileName),
    TokenizedPath = string:tokens(UserFilePath, "/"),
    {Num, Offset} =
        case {ROffset, TokenizedPath} of
            {0 = Off0, []} -> %% First iteration over "/" dir has to contain "groups" folder, so fetch `num - 1` files instead `num`
                {RCount - 1, Off0};
            {Off1, []} -> %% Next iteration over "/" dir has start one entry earlier, so fetch `num` files starting on `offset - 1`
                {ROffset, Off1 - 1};
            {Off2, _} -> %% Non-root dir -> proceed normally
                {RCount, Off2}
        end,

    {Status, TmpAns} = dao_lib:apply(dao_vfs, list_dir, [FullFileName, Num, Offset], fslogic_context:get_protocol_version()),
    case Status of
        ok ->
            Children = fslogic_utils:create_children_list(TmpAns),
            case {ROffset, TokenizedPath} of
                {0, []}    -> %% When asking about root, add virtual ?GROUPS_BASE_DIR_NAME entry
                    #filechildren{child_logic_name = Children ++ [?GROUPS_BASE_DIR_NAME]}; %% Only for offset = 0
                {_, [?GROUPS_BASE_DIR_NAME]} -> %% For group list query ignore DB result and generate list based on user's teams
                    Teams = user_logic:get_team_names({dn, get(user_dn)}),
                    {_Head, Tail} = lists:split(min(Offset, length(Teams)), Teams),
                    {Ret, _} = lists:split(min(Num, length(Tail)), Tail),
                    #filechildren{child_logic_name = Ret};
                _ ->
                    #filechildren{child_logic_name = Children}
            end;
        _BadStatus ->
            lager:error([{mod, ?MODULE}], "Error: can not list files in dir: ~s", [FullFileName]),
            #filechildren{answer = ?VEREMOTEIO, child_logic_name = []}
    end.

create_link(FullFileName, LinkValue) ->
    {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(FullFileName, fslogic_context:get_protocol_version()),
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    UserFilePath = fslogic_utils:get_user_file_name(FullFileName),
    case ParentFound of
        ok ->
            {FileName, Parent} = ParentInfo,
            {PermsStat, PermsOK} = fslogic_utils:check_file_perms(FullFileName, UserDocStatus, UserDoc, Parent, write),
            case PermsStat of
                ok ->
                    case PermsOK of
                        true ->
                            UserId =
                                case UserDoc of
                                    get_user_id_error -> ?CLUSTER_USER_ID;
                                    _ -> UserDoc#veil_document.uuid
                                end,

                            Groups = fslogic_utils:get_group_owner(UserFilePath), %% Get owner group name based on file access path

                            LinkDocInit = #file{type = ?LNK_TYPE, name = FileName, uid = UserId, gids = Groups, ref_file = LinkValue, parent = Parent#veil_document.uuid},
                            CTime = fslogic_utils:time(),
                            LinkDoc = fslogic_utils:update_meta_attr(LinkDocInit, times, {CTime, CTime, CTime}),

                            case dao_lib:apply(dao_vfs, save_new_file, [FullFileName, LinkDoc], fslogic_context:get_protocol_version()) of
                                {ok, _} ->
                                    fslogic_utils:update_parent_ctime(UserFilePath, CTime),
                                    #atom{value = ?VOK};
                                {error, file_exists} ->
                                    lager:error("Cannot create link - file already exists: ~p", [FullFileName]),
                                    #atom{value = ?VEEXIST};
                                {error, Reason} ->
                                    lager:error("Cannot save link file (from ~p to ~p) due to error: ~p", [FullFileName, LinkValue, Reason]),
                                    #atom{value = ?VEREMOTEIO}
                            end;
                        false ->
                            lager:warning("Creation of link without permissions: from ~p to ~p", [FullFileName, LinkValue]),
                            #atom{value = ?VEPERM}
                    end;
                _ ->
                    lager:warning("Cannot create link. Reason: ~p:~p", [PermsStat, PermsOK]),
                    #atom{value = ?VEREMOTEIO}
            end;
        {error, Reason1} ->
            lager:error("Cannot fetch file information for file ~p due to error: ~p", [FullFileName, Reason1]),
            #atom{value = ?VEREMOTEIO}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides methods for permission management and assertions.
%% @end
%% ===================================================================
-module(fslogic_perms).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

-define(permission_denied_error(UserDoc,FileName,CheckType), {error, {permission_denied, {{user, UserDoc}, {file, FileName}, {check, CheckType}}}}).

%% API
-export([check_file_perms/4]).
-export([assert_group_access/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% %% check_file_perms/4
%% %% ====================================================================
%% %% @doc Checks if the user has permission to modify file (e,g. change owner).
%% %% @end
%% -spec check_file_perms(FileName :: string(), UserDoc :: term(), FileDoc :: term(), CheckType :: root | owner | read | write | execute) -> Result when
%%     Result :: {ok, boolean()} | {error, ErrorDetail},
%%     ErrorDetail :: term().
%% %% ====================================================================
%% check_file_perms(_FileName, #veil_document{uuid = ?CLUSTER_USER_ID}, _FileDoc, _CheckType) -> %root, always return ok
%%     ?info("[!!!]check ~p, ~p, OK",[_FileName,_CheckType]),
%%     ok;
%% check_file_perms(FileName, UserDoc, _FileDoc, root = CheckType) -> % check if root
%%     ?info("[!!!]check ~p, root, FAIL",[FileName]),
%%     ?permission_denied_error(UserDoc,FileName,CheckType);
%% check_file_perms(_FileName, #veil_document{uuid = UserUid}, #veil_document{record = #file{uid = UserUid}}, owner) -> % check if owner
%%     ?info("[!!!]check ~p, owner, OK",[_FileName]),
%%     ok;
%% check_file_perms(FileName, UserDoc, _FileDoc, owner = CheckType) ->
%%     ?info("[!!!]check ~p, owner, FAIL",[FileName]),
%%     ?permission_denied_error(UserDoc,FileName,CheckType);
%% check_file_perms(FileName, UserDoc, #veil_document{record = #file{uid = FileOwnerUid, perms = FilePerms}}, CheckType) -> %check read/write/execute perms
%%     UserUid = UserDoc#veil_document.uuid,
%%     storage_files_manager:setup_ctx(FileName),
%%     UserOwnsFile = UserUid=:=FileOwnerUid,
%%     UserGroupOwnsFile = fslogic_context:get_fs_group_ctx() =/= [],
%%     case has_permission(CheckType,FilePerms,UserOwnsFile,UserGroupOwnsFile) of
%%         true ->
%%             ?info("[!!!]check ~p, ~p, OK",[FileName,CheckType]),
%%             ok;
%%         false ->
%%             ?info("[!!!]check ~p, ~p, FAIL",[FileName,CheckType]),
%%             ?permission_denied_error(UserDoc,FileName,CheckType)
%%     end.

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDoc :: term(), FileDoc :: term(), CheckType :: atom()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(_FileName, #veil_document{uuid = ?CLUSTER_USER_ID}, _FileDoc, _CheckType) ->
    ?info("[!!!]check ~p, ~p, OK",[_FileName,_CheckType]),
    ok;
check_file_perms(FileName, UserDoc, _FileDoc, root = CheckType) ->
    ?info("[!!!]check ~p, ~p, FAIL",[FileName,CheckType]),
    {error, {permission_denied, {{user, UserDoc}, {file, FileName}, {check, CheckType}}}};
check_file_perms(FileName, UserDoc, #veil_document{record = #file{uid = FileOwnerUid, perms = FilePerms}}, CheckType) when CheckType==read orelse CheckType==execute ->
    UserUid = UserDoc#veil_document.uuid,
    storage_files_manager:setup_ctx(FileName),
    UserOwnsFile = UserUid=:=FileOwnerUid,
    UserGroupOwnsFile = fslogic_context:get_fs_group_ctx() =/= [],
    case has_permission(CheckType,FilePerms,UserOwnsFile,UserGroupOwnsFile) of
        true ->
            ?info("[!!!]check ~p, ~p, OK",[FileName,CheckType]),
            ok;
        false ->
            ?info("[!!!]check ~p, ~p, FAIL",[FileName,CheckType]),
            ?permission_denied_error(UserDoc,FileName,CheckType)
    end;
check_file_perms(FileName, UserDoc, FileDoc, CheckType) ->
    case string:tokens(FileName, "/") of
        [?GROUPS_BASE_DIR_NAME | _] ->
            FileRecord = FileDoc#veil_document.record,
            CheckOwn =
                case CheckType of
                    owner -> true;
                    _ -> %write
                        FileRecord#file.perms band ?WR_GRP_PERM =:= 0
                end,

            case CheckOwn of
                true ->
                    case UserDoc#veil_document.uuid =:= FileRecord#file.uid of
                        true ->
                            ?info("[!!!]check ~p, ~p, OK",[FileName,CheckType]),
                            ok;
                        _ ->
                            ?info("[!!!]check ~p, ~p, FAIL",[FileName,CheckType]),
                            {error, {permission_denied, {{user, UserDoc}, {file, FileName}, {check, CheckType}}}}
                    end;
                false ->
                    ?info("[!!!]check ~p, ~p, OK",[FileName,CheckType]),
                    ok
            end;
        _ ->
            ?info("[!!!]check ~p, ~p, OK",[FileName,CheckType]),
            ok
    end.

%% assert_group_access/3
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_group_access(UserDoc :: tuple(), Request :: atom(), LogicalPath :: string()) -> ok | error.
%% ====================================================================
assert_group_access(_UserDoc, cluster_request, _LogicalPath) ->
    ok;
assert_group_access(#veil_document{uuid = ?CLUSTER_USER_ID}, _Request, _LogicalPath) ->
    ok;
assert_group_access(UserDoc, Request, LogicalPath) ->
    assert_grp_access(UserDoc, Request, string:tokens(LogicalPath, "/")).

%% assert_grp_access/1
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_grp_access(UserDoc :: tuple(), Request :: atom(), Path :: list()) -> ok | error.
%% ====================================================================
assert_grp_access(_UserDoc, Request, [?GROUPS_BASE_DIR_NAME]) ->
    case lists:member(Request, ?GROUPS_BASE_ALLOWED_ACTIONS) of
        false   -> error;
        true    -> ok
    end;
assert_grp_access(UserDoc, Request, [?GROUPS_BASE_DIR_NAME | Tail]) ->
    TailCheck = case Tail of
                    [_GroupName] ->
                        case lists:member(Request, ?GROUPS_ALLOWED_ACTIONS) of
                            false   -> error;
                            true    -> ok
                        end;
                    _ ->
                        ok
                end,

    case TailCheck of
        ok ->
            UserTeams = user_logic:get_team_names(UserDoc),
            [GroupName2 | _] = Tail,
            case lists:member(GroupName2, UserTeams) of
                true    -> ok;
                false   -> error
            end;
        _ ->
            TailCheck
    end;
assert_grp_access(_, _, _) ->
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% has_permission/4
%% ====================================================================
%% @doc Check if basing on FilePerms and ownership information, user has
%% permission to read/write/execute file
%% @end
-spec has_permission( PermissionType :: read|write|execute , FilePerms :: integer(), UserOwnsFile :: boolean(), UserGroupOwnsFile :: boolean()) ->
    boolean().
%% ====================================================================
has_permission(read, FilePerms, true, _) ->
    FilePerms band ?RD_USR_PERM =/= 0;
has_permission(read, FilePerms, _, true) ->
    FilePerms band ?RD_GRP_PERM =/= 0;
has_permission(read, FilePerms, _, _) ->
    FilePerms band ?RD_OTH_PERM =/= 0;
has_permission(write, FilePerms, true, _) ->
    FilePerms band ?WR_USR_PERM =/= 0;
has_permission(write, FilePerms, _, true) ->
    FilePerms band ?WR_GRP_PERM =/= 0;
has_permission(write, FilePerms, _, _) ->
    FilePerms band ?WR_OTH_PERM =/= 0;
has_permission(execute, FilePerms, true, _) ->
    FilePerms band ?EX_USR_PERM =/= 0;
has_permission(execute, FilePerms, _, true) ->
    FilePerms band ?EX_GRP_PERM =/= 0;
has_permission(execute, FilePerms, _, _) ->
    FilePerms band ?EX_OTH_PERM =/= 0.

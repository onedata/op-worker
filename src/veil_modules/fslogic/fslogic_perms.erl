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

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDoc :: term(), FileDoc :: term(), CheckType :: root | owner | delete | read | write | execute | rdwr | '') -> Result when
    Result :: ok | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(_FileName, _UserDoc, _FileDoc, '') -> %root, always return ok
    ok;
check_file_perms(_FileName, #veil_document{uuid = ?CLUSTER_USER_ID}, _FileDoc, _CheckType) -> %root, always return ok
    ok;
check_file_perms(FileName, UserDoc, _FileDoc, root = CheckType) -> % check if root
    ?permission_denied_error(UserDoc,FileName,CheckType);
check_file_perms(_FileName, #veil_document{uuid = UserUid}, #veil_document{record = #file{uid = UserUid}}, owner) -> % check if owner
    ok;
check_file_perms(FileName, UserDoc, _FileDoc, owner = CheckType) ->
    ?permission_denied_error(UserDoc,FileName,CheckType);
check_file_perms(FileName, UserDoc, FileDoc, delete) ->
    {ok, {_,ParentFileDoc}} = fslogic_path:get_parent_and_name_from_path(FileName,fslogic_context:get_protocol_version()),
    ParentFileName = fslogic_path:strip_path_leaf(FileName),
    case ParentFileDoc#veil_document.record#file.perms band ?STICKY_BIT of
        0 -> check_file_perms(ParentFileName,UserDoc,ParentFileDoc,write);
        _ -> check_file_perms(FileName,UserDoc,FileDoc,owner)
    end;
check_file_perms(FileName, UserDoc, FileDoc, rdwr) ->
    case check_file_perms(FileName, UserDoc, FileDoc, read) of
        ok -> check_file_perms(FileName, UserDoc, FileDoc, write);
        Error -> Error
    end;
check_file_perms(FileName, UserDoc, #veil_document{record = #file{uid = FileOwnerUid, perms = FilePerms}}, CheckType) -> %check read/write/execute perms
    UserUid = UserDoc#veil_document.uuid,
    FileGroup = get_group(FileName),
    UserGroups = user_logic:get_team_names(UserDoc#veil_document.record),

    UserOwnsFile = UserUid=:=FileOwnerUid,
    UserGroupOwnsFile = lists:member(FileGroup,UserGroups),
    case has_permission(CheckType,FilePerms,UserOwnsFile,UserGroupOwnsFile) of
        true -> ok;
        false -> ?permission_denied_error(UserDoc,FileName,CheckType)
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

%% get_group/1
%% ====================================================================
%% @doc Returns file group based on filepath
%% @end
-spec get_group(FileName :: string()) -> Result when
    Result :: string() | none.
%% ====================================================================
get_group(File) ->
    FileTokens = string:tokens(File, "/"),
    case lists:nth(1, FileTokens) of
        "groups" ->
            lists:nth(2, FileTokens);
        _ ->
            none
    end.
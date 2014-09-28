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
-include("veil_modules/dao/dao_types.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").

-define(permission_denied_error(UserDoc,FileName,CheckType), {error, {permission_denied, {{user, UserDoc}, {file, FileName}, {check, CheckType}}}}).

%% API
-export([check_file_perms/2, check_file_perms/4]).
-export([assert_group_access/3]).
-export([has_permission/4]).

%% ====================================================================
%% API functions
%% ====================================================================

%% check_file_perms/2
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% UserDoc and FileDoc is based on current context.
%% @end
-spec check_file_perms(FileName :: string(), CheckType :: root | owner | delete | read | write | execute | rdwr | '') -> Result when
    Result :: ok | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, Mode) ->
    {ok, UserDoc} = fslogic_objects:get_user(),
    {ok, FileDoc} = fslogic_objects:get_file(FileName),
    check_file_perms(FileName, UserDoc, FileDoc, Mode).

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDoc :: term(), FileDoc :: term(), CheckType :: root | owner | delete | read | write | execute | rdwr | '') -> Result when
    Result :: ok | {ok, by_acl} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(_FileName, _UserDoc, _FileDoc, '') -> %undefined mode
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
    case check_file_perms(ParentFileName,UserDoc,ParentFileDoc,write) of
        ok -> check_file_perms(FileName,UserDoc,FileDoc,owner);
        Error -> Error
    end;
check_file_perms(FileName, UserDoc, FileDoc, rdwr) ->
    case check_file_perms(FileName, UserDoc, FileDoc, read) of
        ok -> check_file_perms(FileName, UserDoc, FileDoc, write);
        Error -> Error
    end;
check_file_perms(FileName, UserDoc, #veil_document{record = #file{uid = FileOwnerUid, perms = FilePerms, meta_doc = MetaUuid}}, CheckType) -> %check read/write/execute perms
    #veil_document{uuid = UserUid, record = #user{global_id = GlobalId}} = UserDoc,
    FileSpace = get_group(FileName),

    UserOwnsFile = UserUid=:=FileOwnerUid,
    UserGroupOwnsFile = is_member_of_space(UserDoc, {name, FileSpace}),
    RealAcl =
        case FilePerms of
            0 ->
                {ok, #veil_document{record = #file_meta{acl = Acl}}} = dao_lib:apply(dao_vfs, get_file_meta, [MetaUuid], fslogic_context:get_protocol_version()),
                Acl;
            _ -> []
        end,
    case RealAcl of
        [] ->
            case has_permission(CheckType,FilePerms,UserOwnsFile,UserGroupOwnsFile) of
                true -> ok;
                false -> ?permission_denied_error(UserDoc,FileName,CheckType)
            end;
        _ ->
            case (catch fslogic_acl:check_permission(RealAcl, vcn_utils:ensure_binary(GlobalId), CheckType)) of
                ok ->
                    put(acl_permission, [{FileName, [CheckType]}]),
                    ok;
                ?VEPERM -> ?permission_denied_error(UserDoc,FileName,CheckType)
            end
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
    case assert_grp_access(UserDoc, Request, string:tokens(LogicalPath, "/")) of
        true -> ok;
        false -> error
    end.


%% is_member_of_space/2
%% ====================================================================
%% @doc Checks if the user is an member of given space.
%% @end
-spec is_member_of_space(UserDoc :: user_doc(), space_info() | {name, SpaceName :: string() | binary()}) -> boolean().
%% ====================================================================
is_member_of_space(#veil_document{record = #user{}} = UserDoc, SpaceReq) ->
    try
        is_member_of_space3(UserDoc, SpaceReq, true)
    catch
        _:Reason ->
            ?error("Cannot check space (~p) membership of user ~p due to: ~p", [SpaceReq, UserDoc, Reason]),
            false
    end.
is_member_of_space3(#veil_document{record = #user{global_id = GRUID}} = UserDoc, #space_info{users = Users} = SpaceInfo, Retry) ->
    case lists:member(vcn_utils:ensure_binary(GRUID), Users) of
        true -> true;
        false when Retry ->
            fslogic_spaces:sync_all_supported_spaces(),
            is_member_of_space3(UserDoc, SpaceInfo, false);
        false -> false
    end;
is_member_of_space3(#veil_document{record = #user{}} = UserDoc, {name, SpaceName}, Retry) ->
    UserSpaces = user_logic:get_space_names(UserDoc),
    case lists:member(SpaceName, UserSpaces) of
        true -> true;
        false ->
            case fslogic_objects:get_space(SpaceName) of
                {ok, #space_info{} = SpaceInfo} ->
                    is_member_of_space3(UserDoc, SpaceInfo, Retry);
                _ when Retry ->
                    fslogic_spaces:sync_all_supported_spaces(),
                    is_member_of_space3(UserDoc, SpaceName, false);
                _ ->
                    false
            end
    end.


%% assert_grp_access/3
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_grp_access(UserDoc :: tuple(), Request :: atom(), Path :: list()) -> boolean().
%% ====================================================================
assert_grp_access(_UserDoc, Request, [?SPACES_BASE_DIR_NAME]) ->
    lists:member(Request, ?GROUPS_BASE_ALLOWED_ACTIONS);
assert_grp_access(#veil_document{record = #user{}} = UserDoc, Request, [?SPACES_BASE_DIR_NAME | Tail]) ->
    TailCheck = case Tail of
                    [_GroupName] ->
                        lists:member(Request, ?GROUPS_ALLOWED_ACTIONS);
                    _ ->
                        true
                end,
    case TailCheck of
        true ->
            [SpaceName | _] = Tail,
            is_member_of_space(UserDoc, {name, SpaceName});
        _ ->
            TailCheck
    end;
assert_grp_access(_, _, _) ->
    true.

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
        ?SPACES_BASE_DIR_NAME ->
            lists:nth(2, FileTokens);
        _ ->
            none
    end.

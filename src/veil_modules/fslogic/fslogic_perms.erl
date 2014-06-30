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
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_file_perms/3, check_file_perms/4]).
-export([assert_group_access/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% check_file_perms/3
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDoc :: term(), FileDoc :: term()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDoc, FileDoc) ->
    check_file_perms(FileName, UserDoc, FileDoc, perms).

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDoc :: term(), FileDoc :: term(), CheckType :: atom()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(_FileName, #veil_document{uuid = ?CLUSTER_USER_ID}, _FileDoc, _CheckType) ->
    ok;
check_file_perms(FileName, UserDoc, _FileDoc, root = CheckType) ->
    {error, {permission_denied, {{user, UserDoc}, {file, FileName}, {check, CheckType}}}};
check_file_perms(FileName, UserDoc, FileDoc, CheckType) ->
    case string:tokens(FileName, "/") of
        [?GROUPS_BASE_DIR_NAME | _] ->
            FileRecord = FileDoc#veil_document.record,
            CheckOwn =
                case CheckType of
                    perms -> true;
                    _ -> %write
                        FileRecord#file.perms band ?WR_GRP_PERM =:= 0
                end,

            case CheckOwn of
                true ->
                    case UserDoc#veil_document.uuid =:= FileRecord#file.uid of
                        true ->
                            ok;
                        _ ->
                            {error, {permission_denied, {{user, UserDoc}, {file, FileName}, {check, CheckType}}}}
                    end;
                false ->
                    ok
            end;
        _ ->
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

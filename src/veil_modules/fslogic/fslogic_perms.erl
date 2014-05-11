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
-module(fslogic_perms).
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
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([check_file_perms/4, check_file_perms/5]).
-export([assert_group_access/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDocStatus :: atom(), UserDoc :: term(), FileDoc :: term()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc) ->
    check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc, perms).

%% check_file_perms/5
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDocStatus :: atom(), UserDoc :: term(), FileDoc :: term(), CheckType :: atom()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc, CheckType) ->
    case CheckType of
        root ->
            case {UserDocStatus, UserDoc} of
                {ok, _} ->
                    {ok, false};
                {error, get_user_id_error} -> {ok, true};
                _ -> {UserDocStatus, UserDoc}
            end;
        _ ->
            case string:tokens(FileName, "/") of
                [?GROUPS_BASE_DIR_NAME | _] ->
                    FileRecord = FileDoc#veil_document.record,
                    CheckOwn = case CheckType of
                                   perms -> true;
                                   _ -> %write
                                       case FileRecord#file.perms band ?WR_GRP_PERM of
                                           0 -> true;
                                           _ -> false
                                       end
                               end,

                    case CheckOwn of
                        true ->
                            case {UserDocStatus, UserDoc} of
                                {ok, _} ->
                                    UserUuid = UserDoc#veil_document.uuid,
                                    {ok, FileRecord#file.uid =:= UserUuid};
                                {error, get_user_id_error} -> {ok, true};
                                _ -> {UserDocStatus, UserDoc}
                            end;
                        false ->
                            {ok, true}
                    end;
                _ ->
                    {ok, true}
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

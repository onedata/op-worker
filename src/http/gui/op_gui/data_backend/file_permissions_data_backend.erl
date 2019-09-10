%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file-permission model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(file_permissions_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% data_backend_behaviour callbacks
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
find_record(<<"file-permission">>, FileId) ->
    SessionId = op_gui_session:get_session_id(),
    file_permissions_record(SessionId, FileId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(<<"file-permission">>) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(<<"file-permission">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(<<"file-permission">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(<<"file-permission">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().
update_record(<<"file-permission">>, FileId, Data) ->
    try
        SessId = op_gui_session:get_session_id(),
        Type = proplists:get_value(<<"type">>, Data),
        case Type of
            <<"acl">> ->
                AclJson = proplists:get_value(<<"aclValue">>, Data, <<"[]">>),
                Acl = gui_acl_parser:json_to_acl(AclJson),
                case lfm:set_acl(SessId, {guid, FileId}, Acl) of
                    ok ->
                        ok;
                    {error, ?EACCES} ->
                        op_gui_error:report_warning(
                            <<"Cannot change ACL - access denied.">>
                        )
                end;
            <<"posix">> ->
                PosixValue = case proplists:get_value(<<"posixValue">>, Data) of
                    undefined ->
                        {ok, #file_attr{
                            mode = PermissionsAttr
                        }} = lfm:stat(SessId, {guid, FileId}),
                        PermissionsAttr rem 8#1000;
                    Val ->
                        case is_integer(Val) of
                            true ->
                                binary_to_integer(integer_to_binary(Val), 8);
                            false ->
                                binary_to_integer(Val, 8)
                        end
                end,
                case PosixValue >= 0 andalso PosixValue =< 8#777 of
                    true ->
                        case lfm:set_perms(
                            SessId, {guid, FileId}, PosixValue) of
                            {error, ?EACCES} ->
                                op_gui_error:report_warning(<<"Cannot set "
                                "permissions - access denied.">>);
                            {error, ?EPERM} ->
                                op_gui_error:report_warning(<<"Cannot set "
                                "permissions - access denied.">>);
                            ok ->
                                ok
                        end;
                    false ->
                        op_gui_error:report_warning(<<"Cannot change permissions, "
                        "invalid octal value.">>)
                end
        end
    catch Error:Message ->
        ?error_stacktrace("Cannot set permissions for file ~p - ~p:~p", [
            FileId, Error, Message
        ]),
        op_gui_error:report_warning(
            <<"Cannot change permissions due to unknown error.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(<<"file-permission">>, _FileId) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a file acl record for given FileId. There are three possible
%% values of Type:
%%     # posix - the file has POSIX permissions set
%%     # acl - the file has ACL set and the user can read it
%%     # eaccess - the user cannot view file permissions
%% @end
%%--------------------------------------------------------------------
-spec file_permissions_record(SessionId :: session:id(),
    fslogic_worker:file_guid()) -> {ok, proplists:proplist()}.
file_permissions_record(SessId, FileId) ->
    case lfm:stat(SessId, {guid, FileId}) of
        {error, ?ENOENT} ->
            op_gui_error:report_error(<<"No such file or directory.">>);
        {ok, #file_attr{mode = PermissionsAttr}} ->
            PosixValue = integer_to_binary((PermissionsAttr rem 8#1000), 8),
            GetAclResult = lfm:get_acl(SessId, {guid, FileId}),
            {Type, AclValue} = case GetAclResult of
                {error, ?ENOATTR} ->
                    {<<"posix">>, null};
                {error, ?EACCES} ->
                    {<<"eaccess">>, null};
                {ok, AclEntries} ->
                    FileUuid = file_id:guid_to_uuid(FileId),
                    Acl = gui_acl_parser:acl_to_json(AclEntries),
                    case file_meta:get_active_perms_type(FileUuid) of
                        {ok, acl} -> {<<"acl">>, Acl};
                        _ -> {<<"posix">>, Acl}
                    end
            end,
            {ok, [
                {<<"id">>, FileId},
                {<<"file">>, FileId},
                {<<"type">>, Type},
                {<<"posixValue">>, PosixValue},
                {<<"aclValue">>, AclValue}
            ]}
    end.

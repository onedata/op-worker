%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements callback_backend_behaviour.
%%% It is used to handle RPC calls from clients with active session.
%%% @end
%%%-------------------------------------------------------------------
-module(private_rpc_backend).
-author("Lukasz Opiola").
-behaviour(rpc_backend_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle/2]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rpc_backend_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | gui_error:error_result().
%%--------------------------------------------------------------------
%% File upload related procedures
%%--------------------------------------------------------------------
handle(<<"fileUploadSuccess">>, Props) ->
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    FileId = upload_handler:upload_map_lookup(UploadId),
    upload_handler:upload_map_delete(UploadId),
    % @todo VFS-2051 temporary solution for model pushing during upload
    SessionId = g_session:get_session_id(),
    % This is sent to the client via sessionDetails object
    {ok, FileHandle} =
        logical_file_manager:open(SessionId, {guid, FileId}, read),
    ok = logical_file_manager:fsync(FileHandle),
    ok = logical_file_manager:release(FileHandle),
    {ok, FileData} = file_data_backend:file_record(SessionId, FileId),
    gui_async:push_created(<<"file">>, FileData),
    % @todo end
    ok;

handle(<<"fileUploadFailure">>, Props) ->
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    upload_handler:upload_map_delete(UploadId),
    ok;

% Checks if file can be downloaded (i.e. can be read by the user) and if so,
% returns download URL.
handle(<<"getFileDownloadUrl">>, [{<<"fileId">>, FileId}]) ->
    % TODO VFS-2426 Read permissions should be checked using
    % logical_file_manager:check_perms, however it is not implemented yet.
    % For now, just try to open the file for reading.
    SessionId = g_session:get_session_id(),
    case logical_file_manager:open(SessionId, {guid, FileId}, read) of
        {ok, FileHandle} ->
            logical_file_manager:release(FileHandle),
            Hostname = g_ctx:get_requested_hostname(),
            URL = str_utils:format_bin("https://~s/download/~s",
                [Hostname, FileId]),
            {ok, [{<<"fileUrl">>, URL}]};
        {error, eacces} ->
            gui_error:report_error(<<"Permission denied">>);
        Other ->
            ?error("Error while downloading file ~s: ~p", [Other]),
            gui_error:internal_server_error()
    end;

%%--------------------------------------------------------------------
%% Space related procedures
%%--------------------------------------------------------------------
handle(<<"getTokenUserJoinSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:get_invite_user_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"userJoinSpace">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:join_space(UserAuth, Token) of
        {ok, SpaceId} ->
            SpaceRecord = space_data_backend:space_record(SpaceId),
            SpaceName = proplists:get_value(<<"name">>, SpaceRecord),
            gui_async:push_created(<<"space">>, SpaceRecord, self()),
            {ok, [{<<"spaceName">>, SpaceName}]};
        {error, invalid_token_value} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"userLeaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:leave_space(UserAuth, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoinSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:get_invite_group_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"groupJoinSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:join_space(UserAuth, GroupId, Token) of
        {ok, SpaceId} ->
            SpaceRecord = space_data_backend:space_record(SpaceId),
            SpaceName = proplists:get_value(<<"name">>, SpaceRecord),
            gui_async:push_created(<<"space">>, SpaceRecord, self()),
            {ok, [{<<"spaceName">>, SpaceName}]};
        {error, invalid_token_value} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"groupLeaveSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    SpaceId = proplists:get_value(<<"spaceId">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:leave_space(UserAuth, GroupId, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenProviderSupportSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:get_invite_provider_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(<<"createFileShare">>, Props) ->
    SessionId = g_session:get_session_id(),
    FileId = proplists:get_value(<<"fileId">>, Props),
    Name = proplists:get_value(<<"shareName">>, Props),
    {ok, ShareId} = logical_file_manager:create_share(SessionId, FileId),
    share_data_backend:add_share_mapping(FileId, ShareId),
    {ok, [{<<"shareId">>, ShareId}]};


%%--------------------------------------------------------------------
%% Group related procedures
%%--------------------------------------------------------------------
handle(<<"getTokenUserJoinGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:get_invite_user_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"userJoinGroup">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case user_logic:join_group(UserAuth, Token) of
        {ok, GroupId} ->
            GroupRecord = group_data_backend:group_record(GroupId),
            GroupName = proplists:get_value(<<"name">>, GroupRecord),
            gui_async:push_created(<<"group">>, GroupRecord, self()),
            {ok, [{<<"groupName">>, GroupName}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot join group due to unknown error.">>)
    end;

handle(<<"userLeaveGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case user_logic:leave_group(UserAuth, GroupId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave group due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoinGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:get_invite_group_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"groupJoinGroup">>, Props) ->
    ChildGroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:join_group(UserAuth, ChildGroupId, Token) of
        {ok, ParentGroupId} ->
            ParentGroupRecord = group_data_backend:group_record(ParentGroupId),
            ChildGroupRecord = group_data_backend:group_record(ChildGroupId),
            gui_async:push_updated(<<"group">>, ParentGroupRecord, self()),
            gui_async:push_updated(<<"group">>, ChildGroupRecord, self()),
            PrntGroupName = proplists:get_value(<<"name">>, ParentGroupRecord),
            {ok, [{<<"groupName">>, PrntGroupName}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot join group due to unknown error.">>)
    end;

handle(<<"groupLeaveGroup">>, Props) ->
    ParentGroupId = proplists:get_value(<<"parentGroupId">>, Props),
    ChildGroupId = proplists:get_value(<<"childGroupId">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:leave_group(UserAuth, ParentGroupId, ChildGroupId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave group due to unknown error.">>)
    end;

handle(<<"getTokenRequestSpaceCreation">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:get_create_space_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end.

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

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

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
handle(<<"fileUploadComplete">>, Props) ->
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    FileId = upload_handler:upload_map_lookup(UploadId),
    upload_handler:upload_map_delete(UploadId),
    % @todo VFS-2051 temporary solution for model pushing during upload
    SessionId = g_session:get_session_id(),
    % This is sent to the client via sessionDetails object
    ConnRef = proplists:get_value(<<"connectionRef">>, Props),
    ConnPid = list_to_pid(binary_to_list(base64:decode(ConnRef))),
    {ok, FileHandle} =
        logical_file_manager:open(SessionId, {guid, FileId}, read),
    ok = logical_file_manager:fsync(FileHandle),
    {ok, FileData} = file_data_backend:file_record(SessionId, FileId),
    gui_async:push_created(<<"file">>, FileData, ConnPid),
    % @todo end
    ok;

handle(<<"joinSpace">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    % @TODO VFS-1860 should use space_info join!
    case oz_users:join_space(UserAuth, [{<<"token">>, Token}]) of
        {ok, SpaceID} ->
            {ok, #space_details{
                name = SpaceName
            }} = oz_spaces:get_details(UserAuth, SpaceID),
            {ok, SpaceName};
        {error, {
            400,
            <<"invalid_request">>,
            <<"invalid 'token' value: ", _/binary>>
        }} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"leaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:leave_space(UserAuth, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"userToken">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:get_invite_user_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"groupToken">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:get_invite_group_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"supportToken">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:get_invite_provider_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end.

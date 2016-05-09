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
handle(<<"fileUploadComplete">>, [{<<"fileId">>, FileId}]) ->
    upload_handler:upload_map_delete(FileId),
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

handle(<<"userLeaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
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
    end;

handle(<<"leaveGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:leave_group(UserAuth, GroupId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"userToken">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:get_invite_user_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"groupToken">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:get_invite_group_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"createSpaceToken">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:get_create_space_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(<<"groupLeaveSpace">>, [{<<"spaceId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:leave_space(UserAuth, GroupId, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end.

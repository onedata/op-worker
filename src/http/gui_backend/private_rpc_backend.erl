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
handle(<<"joinSpace">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, SpaceID} = oz_users:join_space(UserAuth, [{<<"token">>, Token}]),
    {ok, #space_details{
        name = SpaceName
    }} = oz_spaces:get_details(UserAuth, SpaceID),
    {ok, SpaceName};

handle(<<"leaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    ok = oz_users:leave_space(UserAuth, SpaceId);

handle(<<"userToken">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, Token} = oz_spaces:get_invite_user_token(UserAuth, SpaceId),
    {ok, Token};

handle(<<"groupToken">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, Token} = oz_spaces:get_invite_group_token(UserAuth, SpaceId),
    {ok, Token};

handle(<<"supportToken">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, Token} = oz_spaces:get_invite_provider_token(UserAuth, SpaceId),
    {ok, Token}.

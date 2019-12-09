%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for reloading storage helper
%%% parameters after storage configuration has changed.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_reload).
-author("Wojciech Geisler").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([refresh_helpers_by_storage/1, refresh_handle_params/4]).

%% RPC
-export([local_refresh_helpers/1]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Reloads all helpers of given storage to ensure they use up-to-date
%% args and ctx.
%% @end
%%--------------------------------------------------------------------
-spec refresh_helpers_by_storage(od_storage:id()) -> ok.
refresh_helpers_by_storage(StorageId) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(Nodes, ?MODULE, local_refresh_helpers, [StorageId]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Regenerates user context with up-to-date args for given storage
%% and calls nif to update them in the existing helper.
%% @end
%%--------------------------------------------------------------------
-spec refresh_handle_params(helpers:helper_handle() | helpers:file_handle(),
    session:id(), od_space:id(), storage:record() | od_storage:id()) -> ok.
refresh_handle_params(Handle, SessionId, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    refresh_handle_params(Handle, SessionId, SpaceId, Storage);
refresh_handle_params(Handle, SessionId, SpaceId, Storage) ->
    % gather information
    Helper = storage:get_helper(Storage),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = luma:get_server_user_ctx(SessionId, UserId, undefined, SpaceId, Storage),
    {ok, ArgsWithUserCtx} = helper:get_args_with_user_ctx(Helper, UserCtx),
    % do the refresh
    helpers:refresh_params(Handle, ArgsWithUserCtx).

%%%===================================================================
%%% RPC exports
%%%===================================================================

-spec local_refresh_helpers(StorageId :: od_storage:id()) -> ok.
local_refresh_helpers(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    {ok, Sessions} = session:list(),
    try
        lists:foreach(fun(#document{key = SessId}) ->
            {ok, HandlesSpaces} = session_helpers:get_local_handles_by_storage(SessId, StorageId),
            lists:foreach(fun({HandleId, SpaceId}) ->
                {ok, #document{value = Handle}} = helper_handle:get(HandleId),
                refresh_handle_params(Handle, SessId, SpaceId, Storage)
            end, HandlesSpaces)
        end, Sessions)
    catch Type:Error ->
        StorageName = storage:get_name(Storage),
        ?error("Error updating active helper for storage ~tp with new args: ~p:~tp",
            [StorageName, Type, Error])
    end.

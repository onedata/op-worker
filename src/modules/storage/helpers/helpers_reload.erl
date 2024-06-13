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
-spec refresh_helpers_by_storage(storage:id()) -> ok.
refresh_helpers_by_storage(StorageId) ->
    utils:rpc_multicall(consistent_hashing:get_all_nodes(), ?MODULE, local_refresh_helpers, [StorageId]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Regenerates user context with up-to-date args for given storage
%% and calls nif to update them in the existing helper.
%% @end
%%--------------------------------------------------------------------
-spec refresh_handle_params(helpers:helper_handle() | helpers:file_handle(),
    session:id(), od_space:id(), storage:data() | storage:id()) -> ok.
refresh_handle_params(Handle, SessionId, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    refresh_handle_params(Handle, SessionId, SpaceId, Storage);
refresh_handle_params(Handle, SessionId, SpaceId, Storage) ->
    % gather information
    Helper = storage:get_helper(Storage),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage),
    {ok, ArgsWithUserCtx} = helper:get_args_with_user_ctx(Helper, UserCtx),
    ArgsWithUserCtxAndType = maps:put(<<"type">>, helper:get_name(Helper), ArgsWithUserCtx),
    % do the refresh
    % @TODO VFS-11947 Propagate storage update errors to onepanel and roll back
    helpers:refresh_params(Handle, ArgsWithUserCtxAndType).

%%%===================================================================
%%% RPC exports
%%%===================================================================

-spec local_refresh_helpers(StorageId :: storage:id()) -> ok.
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
        StorageName = storage:fetch_name_of_local_storage(StorageId),
        ?error("Error updating active helper for storage ~tp with new args: ~tp:~tp",
            [StorageName, Type, Error])
    end.

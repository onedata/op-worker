%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module is responsible for selection of storage and helper
%%% used during filesystem operations.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_storage).
-author("Rafal Slota").

%% API
-export([select_storage/1, select_helper/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns first configured storage for given space.
%% @end
%%--------------------------------------------------------------------
-spec select_storage(od_space:id()) ->
    {ok, storage_config:doc()} | {error, Reason :: term()}.
select_storage(SpaceId) ->
    case space_logic:get_storage_ids(SpaceId) of
        {ok, []} -> {error, {no_storage_avaliable, SpaceId}};
        {ok, [StorageId | _]} -> storage_config:get(StorageId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns first configured helper for given storage.
%% @end
%%--------------------------------------------------------------------
-spec select_helper(storage_config:doc()) ->
    {ok, storage_config:helper()} | {error, Reason :: term()}.
select_helper(StorageConfig) ->
    case storage_config:get_helpers(StorageConfig) of
        [] -> {error, {no_helper_available, storage_config:get_id(StorageConfig)}};
        [Helper | _] -> {ok, Helper}
    end.

%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains helper functions for storage_sync
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_utils).
-author("Jakub Kudzia").

-include("modules/storage_sync/strategy_config.hrl").


%% API
-export([take_children_storage_ctxs_for_batch/2, take_hash_for_batch/2, module/1]).


%%%-------------------------------------------------------------------
%%% @doc
%%% Takes list of storage_file_ctxs for given batch from job Data.
%%% @end
%%%-------------------------------------------------------------------
-spec take_children_storage_ctxs_for_batch(non_neg_integer(), space_strategy:job_data()) ->
    {[storage_file_ctx:ctx()], space_strategy:job_data()}.
take_children_storage_ctxs_for_batch(BatchKey, Data) ->
    recursive_take([children_storage_file_ctxs, BatchKey], Data).

%%%-------------------------------------------------------------------
%%% @doc
%%% Takes hash of file attributes for given batch from job Data.
%%% @end
%%%-------------------------------------------------------------------
-spec take_hash_for_batch(non_neg_integer(), space_strategy:job_data()) ->
    {binary(), space_strategy:job_data()}.
take_hash_for_batch(BatchKey, Data) ->
    recursive_take([hashes_map, BatchKey], Data).

%%%-------------------------------------------------------------------
%%% @doc
%%% Returns module responsible for handling given strategy_type job.
%%% @end
%%%-------------------------------------------------------------------
-spec module(space_strategy:job()) -> atom().
module(#space_strategy_job{strategy_type = Module}) ->
    Module.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Extension of maps:take function. Calls take on the lowest nested map,
%%% and returns tuple {Value, UpdatedMap}.
%%% @end
%%%-------------------------------------------------------------------
-spec recursive_take(term(), map()) -> {term(), map()}.
recursive_take([Key], Map) ->
    maps:take(Key, Map);
recursive_take([Key | Keys], Map) ->
    SubMap = maps:get(Key, Map, #{}),
    case recursive_take(Keys, SubMap) of
        error ->
            {undefined, Map};
        {Value, SubMap2} ->
            {Value, Map#{Key => SubMap2}}
    end;
recursive_take(Key, Map) ->
    recursive_take([Key], Map).

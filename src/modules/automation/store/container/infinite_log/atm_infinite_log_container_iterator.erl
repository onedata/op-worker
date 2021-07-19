%%%-------------------------------------------------------------------
%%% @author Michal Stanisz, Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_infinite_log_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_infinite_log_container_iterator).
-author("Michal Stanisz").
-author("Lukasz Opiola").

-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([build/1, get_next_batch/5, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_infinite_log_container_iterator, {
    backend_id :: atm_infinite_log_container:backend_id(),
    index = 0 :: non_neg_integer()
}).
-type record() :: #atm_infinite_log_container_iterator{}.

-type result_mapper() :: fun((infinite_log:timestamp(), json_utils:json_term()) -> automation:item()).

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_infinite_log_container:backend_id()) -> record().
build(BackendId) ->
    #atm_infinite_log_container_iterator{backend_id = BackendId}.


-spec get_next_batch(atm_workflow_execution_ctx:record(), atm_store_container_iterator:batch_size(), 
    record(), atm_data_spec:record(), result_mapper()
) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(AtmWorkflowExecutionCtx, BatchSize, #atm_infinite_log_container_iterator{} = Record, AtmDataSpec, ResultMapper) ->
    #atm_infinite_log_container_iterator{backend_id = BackendId, index = StartIndex} = Record,
    {ok, {IsLast, EntrySeries}} = atm_infinite_log_backend:list(
        AtmWorkflowExecutionCtx, BackendId, #{start_from => {index, StartIndex}, limit => BatchSize}, AtmDataSpec),
    FilteredEntries = lists:filtermap(fun
        ({_Index, {ok, Timestamp, Item}}) ->
            {true, ResultMapper(Timestamp, Item)};
        ({_Index, _Error}) ->
            false
    end, EntrySeries),
    case {EntrySeries, IsLast} of
        {[], true} -> 
            stop;
        _ ->
            {LastIndex, _} = lists:last(EntrySeries),
            {ok, FilteredEntries, Record#atm_infinite_log_container_iterator{
                index = binary_to_integer(LastIndex) + 1}
            }
    end.


-spec forget_before(record()) -> ok.
forget_before(_AtmStoreContainerIterator) ->
    ok.


-spec mark_exhausted(record()) -> ok.
mark_exhausted(_AtmStoreContainerIterator) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_infinite_log_container_iterator{
    backend_id = BackendId,
    index = Index
}, _NestedRecordEncoder) ->
    #{<<"backendId">> => BackendId, <<"index">> => Index}.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"backendId">> := BackendId, <<"index">> := Index}, _NestedRecordDecoder) ->
    #atm_infinite_log_container_iterator{
        backend_id = BackendId,
        index = Index
    }.

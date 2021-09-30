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
%% @TODO VFS-8068 Get rid of this module
-module(atm_infinite_log_container_iterator).
-author("Michal Stanisz").
-author("Lukasz Opiola").

-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([build/1, get_next_batch/3, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_infinite_log_container_iterator, {
    backend_id :: atm_infinite_log_container:backend_id(),
    index = 0 :: non_neg_integer()
}).
-type record() :: #atm_infinite_log_container_iterator{}.

-type result_mapper() :: fun((infinite_log:timestamp(), atm_value:compressed()) -> 
    {true, automation:item()} | false).

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_infinite_log_container:backend_id()) -> record().
build(BackendId) ->
    #atm_infinite_log_container_iterator{backend_id = BackendId}.


-spec get_next_batch(atm_store_container_iterator:batch_size(), record(), result_mapper()) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(BatchSize, #atm_infinite_log_container_iterator{} = Record, ResultMapper) ->
    #atm_infinite_log_container_iterator{backend_id = BackendId, index = StartIndex} = Record,
    {ok, {Marker, EntrySeries}} = atm_infinite_log_backend:list(
        BackendId, #{start_from => {index, StartIndex}, limit => BatchSize}),
    FilteredEntries = lists:filtermap(fun({_Index, Compressed, Timestamp}) ->
        ResultMapper(Timestamp, Compressed)
    end, EntrySeries),
    case {EntrySeries, Marker} of
        {[], done} -> 
            stop;
        _ ->
            {LastIndex, _, _} = lists:last(EntrySeries),
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

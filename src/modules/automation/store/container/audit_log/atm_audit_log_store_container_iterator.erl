%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_audit_log_store_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_container_iterator).
-author("Michal Stanisz").

-behaviour(atm_store_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([build/1]).

% atm_store_container_iterator callbacks
-export([get_next_batch/4, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_audit_log_store_container_iterator, {
    backend_id :: atm_infinite_log_container:backend_id(),
    index = 0 :: non_neg_integer()
}).
-type record() :: #atm_audit_log_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_infinite_log_container:backend_id()) -> record().
build(BackendId) ->
    #atm_audit_log_store_container_iterator{backend_id = BackendId}.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(atm_workflow_execution_auth:record(), atm_store_container_iterator:batch_size(),
    record(), atm_data_spec:record()
) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(AtmWorkflowExecutionAuth, BatchSize, #atm_audit_log_store_container_iterator{} = Record, AtmDataSpec) ->
    #atm_audit_log_store_container_iterator{backend_id = BackendId, index = StartIndex} = Record,
    {ok, {Marker, EntrySeries}} = json_based_infinite_log_backend:list(BackendId, #{
        start_from => {index, StartIndex},
        limit => BatchSize
    }),
    FilteredEntries = lists:filtermap(fun({_Index, Timestamp, Object}) ->
        case atm_value:expand(AtmWorkflowExecutionAuth, maps:get(<<"entry">>, Object), AtmDataSpec) of
            {ok, ExpandedItem} ->
                {true, Object#{
                    <<"timestamp">> => Timestamp,
                    <<"entry">> => ExpandedItem,
                    <<"severity">> => maps:get(<<"severity">>, Object)
                }};
            {error, _} ->
                false
        end
    end, EntrySeries),
    case {EntrySeries, Marker} of
        {[], done} ->
            stop;
        _ ->
            {LastIndex, _, _} = lists:last(EntrySeries),
            {ok, FilteredEntries, Record#atm_audit_log_store_container_iterator{
                index = LastIndex + 1}
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
db_encode(#atm_audit_log_store_container_iterator{
    backend_id = BackendId,
    index = Index
}, _NestedRecordEncoder) ->
    #{<<"backendId">> => BackendId, <<"index">> => Index}.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"backendId">> := BackendId, <<"index">> := Index}, _NestedRecordDecoder) ->
    #atm_audit_log_store_container_iterator{
        backend_id = BackendId,
        index = Index
    }.

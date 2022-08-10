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
-export([build/2]).
-export([gen_listing_postprocessor/2]).

% atm_store_container_iterator callbacks
-export([get_next_batch/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_audit_log_store_container_iterator, {
    log_content_data_spec :: atm_data_spec:record(),
    backend_id :: atm_store_container_infinite_log_backend:id(),
    last_listed_index = atm_store_container_infinite_log_backend:iterator_start_index() ::
        atm_store_container_infinite_log_backend:index()
}).
-type record() :: #atm_audit_log_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_data_spec:record(), atm_store_container_infinite_log_backend:id()) ->
    record().
build(LogContentDataSpec, BackendId) ->
    #atm_audit_log_store_container_iterator{
        log_content_data_spec = LogContentDataSpec,
        backend_id = BackendId
    }.


-spec gen_listing_postprocessor(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    atm_store_container_infinite_log_backend:listing_postprocessor().
gen_listing_postprocessor(AtmWorkflowExecutionAuth, LogContentDataSpec) ->
    fun({Index, {Timestamp, CompressedLog}}) ->
        CompressedLogContent = maps:get(<<"compressedContent">>, CompressedLog),
        case atm_value:expand(AtmWorkflowExecutionAuth, CompressedLogContent, LogContentDataSpec) of
            {ok, ExpandedLogContent} ->
                {Index, {ok, #{
                    <<"timestamp">> => Timestamp,
                    <<"content">> => ExpandedLogContent,
                    <<"severity">> => maps:get(<<"severity">>, CompressedLog)}
                }};
            {error, _} = Error ->
                {Index, Error}
        end
    end.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(AtmWorkflowExecutionAuth, BatchSize, Record = #atm_audit_log_store_container_iterator{
    log_content_data_spec = LogContentDataSpec,
    backend_id = BackendId,
    last_listed_index = LastListedIndex
}) ->
    Result = atm_store_container_infinite_log_backend:iterator_get_next_batch(
        BatchSize, BackendId, LastListedIndex,
        gen_listing_postprocessor(AtmWorkflowExecutionAuth, LogContentDataSpec)
    ),
    case Result of
        stop ->
            stop;
        {ok, FilteredEntries, NewLastListedIndex} ->
            {ok, FilteredEntries, Record#atm_audit_log_store_container_iterator{
                last_listed_index = NewLastListedIndex
            }}
    end.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_audit_log_store_container_iterator{
    log_content_data_spec = LogContentDataSpec,
    backend_id = BackendId,
    last_listed_index = LastListedIndex
}, NestedRecordEncoder) ->
    #{
        <<"logContentDataSpec">> => NestedRecordEncoder(LogContentDataSpec, atm_data_spec),
        <<"backendId">> => BackendId,
        <<"lastListedIndex">> => LastListedIndex
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    JsonRecord = #{<<"logContentDataSpec">> := LogContentDataSpecJson, <<"backendId">> := BackendId},
    NestedRecordDecoder
) ->
    #atm_audit_log_store_container_iterator{
        log_content_data_spec = NestedRecordDecoder(LogContentDataSpecJson, atm_data_spec),
        backend_id = BackendId,
        last_listed_index = maps:get(<<"lastListedIndex">>, JsonRecord, <<"-1">>)
    }.

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
-export([get_next_batch/3, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_audit_log_store_container_iterator, {
    backend_id :: audit_log:id(),
    last_listed_index = audit_log:iterator_start_index() :: audit_log_browse_opts:index()
}).
-type record() :: #atm_audit_log_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(audit_log:id()) -> record().
build(BackendId) ->
    #atm_audit_log_store_container_iterator{backend_id = BackendId}.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(_AtmWorkflowExecutionAuth, BatchSize, Record = #atm_audit_log_store_container_iterator{
    backend_id = BackendId,
    last_listed_index = LastListedIndex
}) ->
    case audit_log:iterator_get_next_batch(BatchSize, BackendId, LastListedIndex) of
        stop ->
            stop;
        {ok, Entries, NewLastListedIndex} ->
            {ok, Entries, Record#atm_audit_log_store_container_iterator{
                last_listed_index = NewLastListedIndex
            }}
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
    last_listed_index = LastListedIndex
}, _NestedRecordEncoder) ->
    #{
        <<"backendId">> => BackendId,
        <<"lastListedIndex">> => LastListedIndex
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(JsonRecord = #{<<"backendId">> := BackendId}, _NestedRecordDecoder) ->
    #atm_audit_log_store_container_iterator{
        backend_id = BackendId,
        last_listed_index = maps:get(<<"lastListedIndex">>, JsonRecord, <<"-1">>)
    }.

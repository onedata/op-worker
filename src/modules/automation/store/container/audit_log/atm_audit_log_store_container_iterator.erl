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
-export([get_next_batch/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_audit_log_store_container_iterator, {
    backend_id :: audit_log:id(),
    audit_log_iterator = audit_log:new_iterator() :: audit_log:iterator()
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
    {ok, [automation:item()], record()} | stop.
get_next_batch(_AtmWorkflowExecutionAuth, BatchSize, Record = #atm_audit_log_store_container_iterator{
    backend_id = BackendId,
    audit_log_iterator = AuditLogIterator
}) ->
    case audit_log:next_batch(BatchSize, BackendId, AuditLogIterator) of
        ?ERROR_NOT_FOUND ->
            % audit logs that have been deleted (or expired) will appear as having zero entries during iteration
            stop;
        stop ->
            stop;
        {ok, Entries, NewAuditLogIterator} ->
            {ok, Entries, Record#atm_audit_log_store_container_iterator{
                audit_log_iterator = NewAuditLogIterator
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
    backend_id = BackendId,
    audit_log_iterator = AuditLogIterator
}, _NestedRecordEncoder) ->
    #{
        <<"backendId">> => BackendId,
        <<"auditLogIterator">> => AuditLogIterator
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    #{<<"backendId">> := BackendId, <<"auditLogIterator">> := AuditLogIterator},
    _NestedRecordDecoder
) ->
    #atm_audit_log_store_container_iterator{
        backend_id = BackendId,
        audit_log_iterator = AuditLogIterator
    }.

%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `audit_log`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_container).
-author("Lukasz Opiola").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% atm_store_container callbacks
-export([
    create/3,
    get_data_spec/1, browse_content/3, acquire_iterator/1,
    apply_operation/2,
    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_value() :: atm_infinite_log_container:initial_value().
-type operation_options() :: atm_infinite_log_container:operation_options().
-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    start_timestamp => time:millis(),
    offset => atm_store_api:offset()
}.

-record(atm_audit_log_store_container, {
    atm_infinite_log_container :: atm_infinite_log_container:record()
}).
-type record() :: #atm_audit_log_store_container{}.

-export_type([initial_value/0, operation_options/0, browse_options/0, record/0]).

-define(ALLOWED_SEVERITY, [
    <<"debug">>, <<"info">>, <<"notice">>, <<"warning">>, 
    <<"error">>, <<"critical">>, <<"alert">>, <<"emergency">>
]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_workflow_execution_ctx:record(), atm_data_spec:record(), initial_value()) ->
    record() | no_return().
create(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch) ->
    #atm_audit_log_store_container{
        atm_infinite_log_container = atm_infinite_log_container:create(
            AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch, fun prepare_audit_log_object/1
        )
    }.


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_audit_log_store_container{atm_infinite_log_container = AtmInfiniteLogContainer}) ->
    atm_infinite_log_container:get_data_spec(AtmInfiniteLogContainer).


-spec browse_content(atm_workflow_execution_ctx:record(), atm_store_api:browse_opts(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionCtx, BrowseOpts, #atm_audit_log_store_container{
    atm_infinite_log_container = AtmInfiniteLogContainer
}) ->
    SanitizedBrowseOpts = sanitize_browse_options(BrowseOpts),
    {Entries, IsLast} = atm_infinite_log_container:browse_content(
        AtmWorkflowExecutionCtx, SanitizedBrowseOpts, AtmInfiniteLogContainer),
    MappedEntries = lists:map(fun
        ({Index, {ok, Timestamp, Object}}) ->
            {Index, {ok, #{
                <<"timestamp">> => Timestamp, 
                <<"entry">> => maps:get(<<"entry">>, Object), 
                <<"severity">> => maps:get(<<"severity">>, Object)}
            }};
        ({Index, {error, _} = Error}) ->
            {Index, Error}
    end, Entries),
    {MappedEntries, IsLast}.


-spec acquire_iterator(record()) -> atm_audit_log_store_container_iterator:record().
acquire_iterator(#atm_audit_log_store_container{atm_infinite_log_container = AtmInfiniteLogContainer}) ->
    AtmInfiniteLogContainerIterator = atm_infinite_log_container:acquire_iterator(AtmInfiniteLogContainer),
    atm_audit_log_store_container_iterator:build(AtmInfiniteLogContainerIterator).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(AtmAuditLogStoreContainer = #atm_audit_log_store_container{
    atm_infinite_log_container = AtmInfiniteLogContainer
}, AtmStoreContainerOperation) ->
    AtmAuditLogStoreContainer#atm_audit_log_store_container{
        atm_infinite_log_container = atm_infinite_log_container:apply_operation(
            AtmInfiniteLogContainer, AtmStoreContainerOperation, fun prepare_audit_log_object/1
        )
    }.


-spec delete(record()) -> ok.
delete(#atm_audit_log_store_container{atm_infinite_log_container = AtmInfiniteLogContainer}) ->
    atm_infinite_log_container:delete(AtmInfiniteLogContainer).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_audit_log_store_container{
    atm_infinite_log_container = AtmInfiniteLogContainer
}, NestedRecordEncoder) ->
        #{
            <<"atmInfiniteLogContainer">> => NestedRecordEncoder(
                AtmInfiniteLogContainer, atm_infinite_log_container
            )
        }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"atmInfiniteLogContainer">> := AtmInfiniteLogContainerJson}, NestedRecordDecoder) ->
    #atm_audit_log_store_container{
        atm_infinite_log_container = NestedRecordDecoder(
            AtmInfiniteLogContainerJson, atm_infinite_log_container
        )
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec sanitize_browse_options(atm_store_container:browse_options()) -> browse_options().
sanitize_browse_options(BrowseOpts) ->
    middleware_sanitizer:sanitize_data(BrowseOpts, #{
        required => #{
            limit => {integer, {not_lower_than, 1}}
        },
        at_least_one => #{
            offset => {integer, any},
            start_index => {binary, any},
            start_timestamp => {integer, any}
        }
    }).


%% @private
-spec prepare_audit_log_object(automation:item()) -> json_utils:json_map().
prepare_audit_log_object(#{<<"entry">> := Entry, <<"severity">> := Severity}) ->
    #{
        <<"entry">> => Entry, 
        <<"severity">> => normalize_severity(Severity)
    };
prepare_audit_log_object(#{<<"entry">> := Entry}) ->
    #{
        <<"entry">> => Entry, 
        <<"severity">> => <<"info">>
    };
prepare_audit_log_object(#{<<"severity">> := Severity} = Object) ->
    #{
        <<"entry">> => maps:without(<<"severity">>, Object), 
        <<"severity">> => normalize_severity(Severity)
    };
prepare_audit_log_object(Entry) ->
    #{
        <<"entry">> => Entry, 
        <<"severity">> => <<"info">>
    }.


%% @private
-spec normalize_severity(any()) -> binary().
normalize_severity(ProvidedSeverity) ->
    case lists:member(ProvidedSeverity, ?ALLOWED_SEVERITY) of
        true -> ProvidedSeverity;
        false -> <<"info">>
    end.
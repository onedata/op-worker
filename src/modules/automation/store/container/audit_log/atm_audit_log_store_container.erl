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
%%%
%%%                             !! CAUTION !!
%%% This store container is directly used by `atm_workflow_execution_logger`
%%% which in turn depends that `apply_operation` doesn't change container.
%%% Any changes made in this module may affect logger and should be
%%% accounted for.
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


-type initial_value() :: [automation:item()] | undefined.
%% Full 'operation_options' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying individual binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"isBatch">> := boolean()
%% }
-type operation_options() :: #{binary() => boolean()}.

%@formatter:off
-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    start_timestamp => time:millis(),
    offset => atm_store_api:offset()
}.
%@formatter:on

-record(atm_audit_log_store_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: json_based_infinite_log_model:id()
}).
-type record() :: #atm_audit_log_store_container{}.

-export_type([initial_value/0, operation_options/0, browse_options/0, record/0]).

-define(ALLOWED_SEVERITY, [
    ?LOGGER_DEBUG, ?LOGGER_INFO, ?LOGGER_NOTICE,
    ?LOGGER_WARNING, ?LOGGER_ALERT,
    ?LOGGER_ERROR, ?LOGGER_CRITICAL, ?LOGGER_EMERGENCY
]).

%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_workflow_execution_auth:record(), atm_data_spec:record(), initial_value()) ->
    record() | no_return().
create(_AtmWorkflowExecutionAuth, AtmDataSpec, undefined) ->
    create_container(AtmDataSpec);

create(AtmWorkflowExecutionAuth, AtmDataSpec, InitialValueBatch) ->
    % validate and sanitize given batch first, to simulate atomic operation
    SanitizedBatch = sanitize_data_batch(AtmWorkflowExecutionAuth, AtmDataSpec, InitialValueBatch),
    append_sanitized_batch(SanitizedBatch, create_container(AtmDataSpec)).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_audit_log_store_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec browse_content(atm_workflow_execution_auth:record(), browse_options(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionAuth, BrowseOpts, #atm_audit_log_store_container{
    backend_id = BackendId,
    data_spec = AtmDataSpec
}) ->
    atm_infinite_log_based_stores_common:browse_content(
        audit_log_store, BackendId, BrowseOpts,
        atm_audit_log_store_container_iterator:gen_listing_postprocessor(AtmWorkflowExecutionAuth, AtmDataSpec)
    ).


-spec acquire_iterator(record()) -> atm_audit_log_store_container_iterator:record().
acquire_iterator(#atm_audit_log_store_container{backend_id = BackendId}) ->
    atm_audit_log_store_container_iterator:build(BackendId).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(#atm_audit_log_store_container{data_spec = AtmDataSpec} = Record, #atm_store_container_operation{
    type = append,
    options = #{<<"isBatch">> := true},
    argument = Batch,
    workflow_execution_auth = AtmWorkflowExecutionAuth
}) ->
    % validate and sanitize given batch first, to simulate atomic operation
    SanitizedBatch = sanitize_data_batch(AtmWorkflowExecutionAuth, AtmDataSpec, Batch),
    append_sanitized_batch(SanitizedBatch, Record);

apply_operation(#atm_audit_log_store_container{} = Record, Operation = #atm_store_container_operation{
    type = append,
    argument = Item,
    options = Options
}) ->
    apply_operation(Record, Operation#atm_store_container_operation{
        options = Options#{<<"isBatch">> => true},
        argument = [Item]
    });

apply_operation(_Record, _Operation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_audit_log_store_container{backend_id = BackendId}) ->
    json_based_infinite_log_model:destroy(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_audit_log_store_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}, NestedRecordEncoder) ->
    #{
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"backendId">> => BackendId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"dataSpec">> := AtmDataSpecJson, <<"backendId">> := BackendId}, NestedRecordDecoder) ->
    #atm_audit_log_store_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        backend_id = BackendId
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create_container(atm_data_spec:record()) -> record().
create_container(AtmDataSpec) ->
    {ok, Id} = json_based_infinite_log_model:create(#{}),
    #atm_audit_log_store_container{
        data_spec = AtmDataSpec,
        backend_id = Id
    }.


%% @private
-spec sanitize_data_batch(
    atm_workflow_execution_auth:record(),
    atm_data_spec:record(),
    [json_utils:json_term()]
) ->
    [atm_value:expanded()] | no_return().
sanitize_data_batch(AtmWorkflowExecutionAuth, AtmDataSpec, Batch) when is_list(Batch) ->
    lists:map(fun(Item) ->
        #{<<"entry">> := Entry} = Object = prepare_audit_log_object(Item),
        atm_value:validate(AtmWorkflowExecutionAuth, Entry, AtmDataSpec),
        Object
    end, Batch);
sanitize_data_batch(_AtmWorkflowExecutionAuth, _AtmDataSpec, _Item) ->
    throw(?ERROR_BAD_DATA(<<"value">>, <<"not a batch">>)).


%% @private
-spec append_sanitized_batch([atm_value:expanded()], record()) -> record().
append_sanitized_batch(Batch, Record = #atm_audit_log_store_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}) ->
    lists:foreach(fun(#{<<"entry">> := Item} = Object) ->
        ok = json_based_infinite_log_model:append(BackendId, Object#{
            <<"entry">> => atm_value:compress(Item, AtmDataSpec)
        }) 
    end, Batch),
    Record.


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
        <<"entry">> => maps:without([<<"severity">>], Object), 
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

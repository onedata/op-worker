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


-type initial_value() :: [automation:item()] | undefined.
%% Full 'operation_options' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying individual binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"isBatch">> := boolean()
%% }
-type operation_options() :: #{binary() => boolean()}.

-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    start_timestamp => time:millis(),
    offset => atm_store_api:offset()
}.

-record(atm_audit_log_store_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: atm_infinite_log_backend:id()
}).
-type record() :: #atm_audit_log_store_container{}.

-export_type([initial_value/0, operation_options/0, browse_options/0, record/0]).

-define(ALLOWED_SEVERITY, [
    <<"debug">>, <<"info">>, <<"notice">>, <<"warning">>, 
    <<"error">>, <<"critical">>, <<"alert">>, <<"emergency">>
]).

%% @TODO VFS-8068 Reuse duplicated code with atm_list_store_container and atm_infinite_log_container

%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_workflow_execution_ctx:record(), atm_data_spec:record(), initial_value()) ->
    record() | no_return().
create(_AtmWorkflowExecutionCtx, AtmDataSpec, undefined) ->
    create_container(AtmDataSpec);

create(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch) ->
    % validate and sanitize given batch first, to simulate atomic operation
    SanitizedBatch = sanitize_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch),
    append_sanitized_batch(SanitizedBatch, create_container(AtmDataSpec)).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_audit_log_store_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.



-spec browse_content(atm_workflow_execution_ctx:record(), browse_options(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionCtx, BrowseOpts, #atm_audit_log_store_container{
    backend_id = BackendId,
    data_spec = AtmDataSpec
}) ->
    SanitizedBrowseOpts = sanitize_browse_options(BrowseOpts),
    {ok, {Marker, Entries}} = atm_infinite_log_backend:list(BackendId, #{
        start_from => infer_start_from(SanitizedBrowseOpts),
        offset => maps:get(offset, SanitizedBrowseOpts, 0),
        limit => maps:get(limit, SanitizedBrowseOpts)
    }),
    MappedEntries = lists:map(fun({Index, Object, Timestamp}) ->
        case atm_value:expand(AtmWorkflowExecutionCtx, maps:get(<<"entry">>, Object), AtmDataSpec) of
            {ok, ExpandedEntry} ->
                {Index, {ok, Object#{
                    <<"timestamp">> => Timestamp, 
                    <<"entry">> => ExpandedEntry, 
                    <<"severity">> => maps:get(<<"severity">>, Object)}
                }};
            {error, _} = Error ->
                {Index, Error}
        end
    end, Entries),
    {MappedEntries, Marker =:= done}.


-spec acquire_iterator(record()) -> atm_audit_log_store_container_iterator:record().
acquire_iterator(#atm_audit_log_store_container{backend_id = BackendId}) ->
    atm_audit_log_store_container_iterator:build(BackendId).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(#atm_audit_log_store_container{data_spec = AtmDataSpec} = Record, #atm_store_container_operation{
    type = append,
    options = #{<<"isBatch">> := true},
    value = Batch,
    workflow_execution_ctx = AtmWorkflowExecutionCtx
}) ->
    % validate and sanitize given batch first, to simulate atomic operation
    SanitizedBatch = sanitize_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, Batch),
    append_sanitized_batch(SanitizedBatch, Record);

apply_operation(#atm_audit_log_store_container{} = Record, Operation = #atm_store_container_operation{
    type = append,
    value = Item,
    options = Options
}) ->
    apply_operation(Record, Operation#atm_store_container_operation{
        options = Options#{<<"isBatch">> => true},
        value = [Item]
    });

apply_operation(_Record, _Operation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_audit_log_store_container{backend_id = BackendId}) ->
    atm_infinite_log_backend:destroy(BackendId).


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
    {ok, Id} = atm_infinite_log_backend:create(#{}),
    #atm_audit_log_store_container{
        data_spec = AtmDataSpec,
        backend_id = Id
    }.


%% @private
-spec sanitize_data_batch(
    atm_workflow_execution_ctx:record(),
    atm_data_spec:record(),
    [json_utils:json_term()]
) ->
    ok | no_return().
sanitize_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, Batch) when is_list(Batch) ->
    lists:map(fun
        (#{<<"entry">> := Item} = Object) ->
            atm_value:validate(AtmWorkflowExecutionCtx, Item, AtmDataSpec),
            prepare_audit_log_object(Object);
        (Item) ->
            atm_value:validate(AtmWorkflowExecutionCtx, Item, AtmDataSpec),
            prepare_audit_log_object(Item)
    end, Batch);
sanitize_data_batch(_AtmWorkflowExecutionCtx, _AtmDataSpec, _Item) ->
    throw(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>)).


%% @private
-spec append_sanitized_batch([automation:item()], record()) -> record().
append_sanitized_batch(Batch, Record = #atm_audit_log_store_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}) ->
    lists:foreach(fun(#{<<"entry">> := Item} = Object) ->
        ok = atm_infinite_log_backend:append(BackendId, Object#{
            <<"entry">> => atm_value:compress(Item, AtmDataSpec)
        }) 
    end, Batch),
    Record.


%% @private
-spec infer_start_from(atm_store_api:browse_options()) ->
    undefined | {index, infinite_log:entry_index()} | {timestamp, infinite_log:timestamp()}.
infer_start_from(#{start_index := <<>>}) ->
    undefined;
infer_start_from(#{start_index := StartIndexBin}) ->
    try
        {index, binary_to_integer(StartIndexBin)}
    catch _:_ ->
        throw(?ERROR_ATM_BAD_DATA(<<"index">>, <<"not numerical">>))
    end;
infer_start_from(#{start_timestamp := StartTimestamp}) ->
    {timestamp, StartTimestamp};
infer_start_from(_) ->
    undefined.


%% @private
-spec sanitize_browse_options(browse_options()) -> browse_options().
sanitize_browse_options(BrowseOpts) ->
    middleware_sanitizer:sanitize_data(BrowseOpts, #{
        required => #{
            limit => {integer, {not_lower_than, 1}}
        },
        at_least_one => #{
            offset => {integer, any},
            start_index => {binary, any},
            start_timestamp => {integer, {not_lower_than, 0}}
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
%%%-------------------------------------------------------------------
%%% @author Michal Stanisz, Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality as a common
%%% backend for stores based on infinite_log.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_infinite_log_container).
-author("Michal Stanisz").
-author("Lukasz Opiola").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/4,
    get_data_spec/1, browse_content/2, acquire_iterator/1,
    apply_operation/3,
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

% id of underlying persistent record implemented by `atm_infinite_log_backend`
-type backend_id() :: binary().

-record(atm_infinite_log_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: backend_id()
}).
-type record() :: #atm_infinite_log_container{}.

-type item_sanitizer() :: fun((automation:item()) -> json_utils:json_term()).

-export_type([initial_value/0, operation_options/0, browse_options/0, backend_id/0, record/0, item_sanitizer/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_workflow_execution_ctx:record(), atm_data_spec:record(), initial_value(), item_sanitizer()) ->
    record() | no_return().
create(_AtmWorkflowExecutionCtx, AtmDataSpec, undefined, _ItemMapper) ->
    create_container(AtmDataSpec);

create(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch, ItemMapper) ->
    validate_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch),
    append_insecure(InitialValueBatch, create_container(AtmDataSpec), ItemMapper).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_infinite_log_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec browse_content(browse_options(), record()) ->
    {[atm_infinite_log_backend:entry()], boolean()} | no_return().
browse_content(BrowseOpts, #atm_infinite_log_container{
    backend_id = BackendId
}) ->
    {ok, {Marker, Entries}} = atm_infinite_log_backend:list(BackendId, #{
        start_from => infer_start_from(BrowseOpts),
        offset => maps:get(offset, BrowseOpts, 0),
        limit => maps:get(limit, BrowseOpts)
    }),
    {Entries, Marker =:= done}.


-spec acquire_iterator(record()) -> atm_infinite_log_container_iterator:record().
acquire_iterator(#atm_infinite_log_container{backend_id = BackendId}) ->
    atm_infinite_log_container_iterator:build(BackendId).


-spec apply_operation(record(), atm_store_container:operation(), item_sanitizer()) ->
    record() | no_return().
apply_operation(#atm_infinite_log_container{data_spec = AtmDataSpec} = Record, #atm_store_container_operation{
    type = append,
    options = #{<<"isBatch">> := true},
    value = Batch,
    workflow_execution_ctx = AtmWorkflowExecutionCtx
}, ItemMapper) ->
    validate_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, Batch),
    append_insecure(Batch, Record, ItemMapper);

apply_operation(#atm_infinite_log_container{} = Record, Operation = #atm_store_container_operation{
    type = append,
    value = Item,
    options = Options
}, ItemMapper) ->
    apply_operation(Record, Operation#atm_store_container_operation{
        options = Options#{<<"isBatch">> => true},
        value = [Item]
    }, ItemMapper);

apply_operation(_Record, _Operation, _ItemMapper) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_infinite_log_container{backend_id = BackendId}) ->
    atm_infinite_log_backend:destroy(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_infinite_log_container{
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
    #atm_infinite_log_container{
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
    #atm_infinite_log_container{
        data_spec = AtmDataSpec,
        backend_id = Id
    }.


%% @private
-spec validate_data_batch(
    atm_workflow_execution_ctx:record(),
    atm_data_spec:record(),
    [json_utils:json_term()]
) ->
    ok | no_return().
validate_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, Batch) when is_list(Batch) ->
    lists:foreach(fun(Item) ->
        atm_value:validate(AtmWorkflowExecutionCtx, Item, AtmDataSpec)
    end, Batch);
validate_data_batch(_AtmWorkflowExecutionCtx, _AtmDataSpec, _Item) ->
    throw(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>)).


%% @private
-spec append_insecure([automation:item()], record(), item_sanitizer()) -> record().
append_insecure(Batch, Record = #atm_infinite_log_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}, ItemMapper) ->
    lists:foreach(fun(Item) ->
        ok = atm_infinite_log_backend:append(
            BackendId, ItemMapper(atm_value:compress(Item, AtmDataSpec)))
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

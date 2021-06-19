%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_container` functionality for `list`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_container).
-author("Michal Stanisz").

-behaviour(atm_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/3, get_data_spec/1, acquire_iterator/1, apply_operation/2, delete/1]).

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

% id of underlying persistent record implemented by `atm_list_store_backend`
-type backend_id() :: binary().

-record(atm_list_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: backend_id()
}).
-type record() :: #atm_list_container{}.

-export_type([initial_value/0, operation_options/0, backend_id/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create(atm_data_spec:record(), initial_value(), atm_workflow_execution_ctx:record()) ->
    record() | no_return().
create(AtmDataSpec, undefined, _AtmWorkflowExecutionCtx) ->
    create_container(AtmDataSpec);

create(AtmDataSpec, InitialValueBatch, AtmWorkflowExecutionCtx) ->
    validate_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch),
    append_insecure(InitialValueBatch, create_container(AtmDataSpec)).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_list_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec acquire_iterator(record()) -> atm_list_container_iterator:record().
acquire_iterator(#atm_list_container{backend_id = BackendId}) ->
    atm_list_container_iterator:build(BackendId).


-spec apply_operation(record(), atm_container:operation()) ->
    record() | no_return().
apply_operation(#atm_list_container{data_spec = AtmDataSpec} = Record, #atm_container_operation{
    type = append,
    options = #{<<"isBatch">> := true},
    value = Batch,
    workflow_execution_ctx = AtmWorkflowExecutionCtx
}) ->
    validate_data_batch(AtmWorkflowExecutionCtx, AtmDataSpec, Batch),
    append_insecure(Batch, Record);

apply_operation(#atm_list_container{} = Record, AtmContainerOperation = #atm_container_operation{
    type = append,
    value = Item,
    options = Options
}) ->
    apply_operation(Record, AtmContainerOperation#atm_container_operation{
        options = Options#{<<"isBatch">> => true},
        value = [Item]
    });

apply_operation(_Record, _AtmContainerOperation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_list_container{backend_id = BackendId}) ->
    atm_list_store_backend:destroy(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_container{
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
    #atm_list_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        backend_id = BackendId
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_container(atm_data_spec:record()) -> record().
create_container(AtmDataSpec) ->
    {ok, Id} = atm_list_store_backend:create(#{}),
    #atm_list_container{
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
        atm_data_validator:validate(AtmWorkflowExecutionCtx, Item, AtmDataSpec)
    end, Batch);
validate_data_batch(_AtmWorkflowExecutionCtx, _AtmDataSpec, _Item) ->
    throw(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>)).


%% @private
-spec append_insecure([automation:item()], record()) -> record().
append_insecure(Batch, #atm_list_container{data_spec = AtmDataSpec, backend_id = BackendId} = Record) ->
    lists:foreach(fun(Item) ->
        CompressedItem = atm_data_compressor:compress(Item, AtmDataSpec),
        ok = atm_list_store_backend:append(BackendId, json_utils:encode(CompressedItem))
    end, Batch),
    Record.

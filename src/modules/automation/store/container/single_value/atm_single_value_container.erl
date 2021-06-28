%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_container` functionality for `single_value`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_container).
-author("Bartosz Walkowicz").

-behaviour(atm_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/3, get_data_spec/1, acquire_iterator/1, apply_operation/2, delete/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_value() :: undefined | automation:item().
-type operation_options() :: #{binary() => boolean()}.

-record(atm_single_value_container, {
    data_spec :: atm_data_spec:record(),
    value :: undefined | automation:item()
}).
-type record() :: #atm_single_value_container{}.

-export_type([initial_value/0, operation_options/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create(atm_data_spec:record(), initial_value(), atm_workflow_execution_ctx:record()) ->
    record() | no_return().
create(AtmDataSpec, undefined, _AtmWorkflowExecutionCtx) ->
    #atm_single_value_container{
        data_spec = AtmDataSpec
    };
create(AtmDataSpec, InitialValue, AtmWorkflowExecutionCtx) ->
    atm_value:validate(AtmWorkflowExecutionCtx, InitialValue, AtmDataSpec),

    #atm_single_value_container{
        data_spec = AtmDataSpec,
        value = atm_value:compress(InitialValue, AtmDataSpec)
    }.


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_single_value_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec acquire_iterator(record()) -> atm_single_value_container_iterator:record().
acquire_iterator(#atm_single_value_container{value = Value}) ->
    atm_single_value_container_iterator:build(Value).


-spec apply_operation(record(), atm_container:operation()) ->
    record() | no_return().
apply_operation(#atm_single_value_container{} = Record, #atm_container_operation{
    type = set,
    value = Item,
    workflow_execution_ctx = AtmWorkflowExecutionCtx
}) ->
    #atm_single_value_container{data_spec = AtmDataSpec} = Record,
    atm_value:validate(AtmWorkflowExecutionCtx, Item, AtmDataSpec),

    Record#atm_single_value_container{value = atm_value:compress(Item, AtmDataSpec)};

apply_operation(_Record, _Operation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(_Record) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_container{
    data_spec = AtmDataSpec,
    value = Value
}, NestedRecordEncoder) ->
    maps_utils:put_if_defined(
        #{<<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec)},
        <<"value">>, Value
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"dataSpec">> := AtmDataSpecJson} = AtmContainerJson, NestedRecordDecoder) ->
    #atm_single_value_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        value = maps:get(<<"value">>, AtmContainerJson, undefined)
    }.

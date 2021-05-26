%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on automation task execution argument spec.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_argument_spec).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/2, construct_arg/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type record() :: #atm_task_execution_argument_spec{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_lambda_argument_spec:record(),
    undefined | atm_task_schema_argument_mapper:record()
) ->
    record().
build(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_batch = IsBatch,
    default_value = DefaultValue
}, undefined) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = #atm_task_argument_value_builder{type = const, recipe = DefaultValue},
        data_spec = AtmDataSpec,
        is_batch = IsBatch
    };

build(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_batch = IsBatch
}, #atm_task_schema_argument_mapper{value_builder = ValueBuilder}) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = ValueBuilder,
        data_spec = AtmDataSpec,
        is_batch = IsBatch
    }.


-spec construct_arg(atm_task_execution:ctx(), record()) ->
    json_utils:json_term() | no_return().
construct_arg(AtmTaskExecutionCtx, #atm_task_execution_argument_spec{
    value_builder = ArgValueBuilder
} = AtmTaskExecutionArgSpec) ->
    ArgValue = build_value(AtmTaskExecutionCtx, ArgValueBuilder),
    validate_value(ArgValue, AtmTaskExecutionArgSpec),
    ArgValue.


%%%===================================================================
%%% Internal functions
%%%===================================================================


% TODO VFS-7660 handle rest of atm_task_argument_value_builder:type()
%% @private
-spec build_value(atm_task_execution:ctx(), atm_task_argument_value_builder:record()) ->
    json_utils:json_term() | no_return().
build_value(_AtmTaskExecutionCtx, #atm_task_argument_value_builder{
    type = const,
    recipe = ConstValue
}) ->
    ConstValue;

build_value(#atm_task_execution_ctx{item = Item}, #atm_task_argument_value_builder{
    type = iterated_item,
    recipe = undefined
}) ->
    Item;

build_value(#atm_task_execution_ctx{item = Item}, #atm_task_argument_value_builder{
    type = iterated_item,
    recipe = Query
}) ->
    % TODO VFS-7660 fix query in case of array indices
    case json_utils:query(Item, Query) of
        {ok, Value} -> Value;
        error -> throw(?ERROR_ATM_TASK_ARG_MAPPER_ITEM_QUERY_FAILED(Item, Query))
    end;

build_value(_AtmTaskExecutionArgSpec, _InputSpec) ->
    throw(?ERROR_ATM_TASK_ARG_MAPPER_INVALID_INPUT_SPEC).


%% @private
-spec validate_value(json_utils:json_term() | [json_utils:json_term()], record()) ->
    ok | no_return().
validate_value(ArgsBatch, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_batch = true
}) ->
    lists:foreach(fun(ArgValue) ->
        atm_data_validator:validate(ArgValue, AtmDataSpec)
    end, ArgsBatch);

validate_value(ArgValue, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_batch = false
}) ->
    atm_data_validator:validate(ArgValue, AtmDataSpec).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_task_execution_argument_spec{
    name = Name,
    value_builder = ValueBuilder,
    data_spec = AtmDataSpec,
    is_batch = IsBatch
}, NestedRecordEncoder) ->
    #{
        <<"name">> => Name,
        <<"valueBuilder">> => NestedRecordEncoder(ValueBuilder, atm_task_argument_value_builder),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"isBatch">> => IsBatch
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"name">> := Name,
    <<"valueBuilder">> := ValueBuilderJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"isBatch">> := IsBatch
}, NestedRecordDecoder) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = NestedRecordDecoder(ValueBuilderJson, atm_task_argument_value_builder),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        is_batch = IsBatch
    }.

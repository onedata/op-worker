%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling atm task execution arguments.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_args).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_task_execution.hrl").
-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([build_specs/2, build_args/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_lambda_argument_spec:record()],
    [atm_task_schema_argument_mapper()]
) ->
    [atm_task_execution:arg_spec()].
build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers) ->
    build_specs(
        lists:usort(fun order_atm_lambda_arg_specs_by_name/2, AtmLambdaArgSpecs),
        lists:usort(fun order_atm_task_schema_arg_mappers_by_name/2, AtmTaskSchemaArgMappers),
        []
    ).


-spec build_args(atm_task_execution:ctx(), [atm_task_execution:arg_spec()]) ->
    json_utils:json_map() | no_return().
build_args(AtmTaskExecutionCtx, AtmTaskExecutionArgSpecs) ->
    lists:foldl(fun(#atm_task_execution_argument_spec{
        name = Name,
        input_spec = ArgInputSpec
    } = AtmTaskExecutionArgSpec, Args) ->
        try
            ArgValue = build_arg(AtmTaskExecutionCtx, ArgInputSpec),
            validate_arg(ArgValue, AtmTaskExecutionArgSpec),
            Args#{Name => ArgValue}
        catch _:Reason ->
            throw(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(Name, Reason))
        end
    end, #{}, AtmTaskExecutionArgSpecs).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec order_atm_lambda_arg_specs_by_name(
    atm_lambda_argument_spec:record(),
    atm_lambda_argument_spec:record()
) ->
    boolean().
order_atm_lambda_arg_specs_by_name(
    #atm_lambda_argument_spec{name = Name1},
    #atm_lambda_argument_spec{name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec order_atm_task_schema_arg_mappers_by_name(
    atm_task_schema_argument_mapper(),
    atm_task_schema_argument_mapper()
) ->
    boolean().
order_atm_task_schema_arg_mappers_by_name(
    #atm_task_schema_argument_mapper{argument_name = Name1},
    #atm_task_schema_argument_mapper{argument_name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec build_specs(
    OrderedUniqueAtmLambdaArgSpecs :: [atm_lambda_argument_spec:record()],
    OrderedUniqueAtmTaskSchemaArgMappers :: [atm_task_schema_argument_mapper()],
    AtmTaskExecutionArgSpecs :: [atm_task_execution:arg_spec()]
) ->
    [atm_task_execution:arg_spec()] | no_return().
build_specs([], [], AtmTaskExecutionArgSpecs) ->
    AtmTaskExecutionArgSpecs;

build_specs(
    [#atm_lambda_argument_spec{name = Name} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    [#atm_task_schema_argument_mapper{argument_name = Name} = AtmTaskSchemaArgMapper | RestAtmTaskSchemaArgMappers],
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, RestAtmTaskSchemaArgMappers, [
        build_spec(AtmLambdaArgSpec, AtmTaskSchemaArgMapper) | AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_lambda_argument_spec{default_value = Default} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) when Default /= undefined ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, [
        build_spec(AtmLambdaArgSpec, undefined), AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_lambda_argument_spec{is_optional = true} | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, AtmTaskExecutionArgSpecs);

build_specs(
    [#atm_lambda_argument_spec{name = Name} | _],
    _AtmTaskSchemaArgMappers,
    _AtmTaskExecutionArgSpecs
) ->
    throw(?ERROR_ATM_NO_TASK_ARG_MAPPER_FOR_REQUIRED_LAMBDA_ARG(Name));

build_specs(
    [],
    [#atm_task_schema_argument_mapper{argument_name = Name} | _],
    _AtmTaskExecutionArgSpecs
) ->
    throw(?ERROR_ATM_TASK_ARG_MAPPER_FOR_NONEXISTENT_LAMBDA_ARG(Name)).


%% @private
-spec build_spec(
    atm_lambda_argument_spec:record(),
    undefined | atm_task_schema_argument_mapper()
) ->
    atm_task_execution:arg_spec().
build_spec(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_batch = IsBatch,
    default_value = DefaultValue
}, undefined) ->
    #atm_task_execution_argument_spec{
        name = Name,
        input_spec = #{
            <<"inputRefType">> => <<"const">>,
            <<"inputRef">> => DefaultValue
        },
        data_spec = AtmDataSpec,
        is_batch = IsBatch
    };

build_spec(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_batch = IsBatch
}, #atm_task_schema_argument_mapper{value_builder = InputSpec}) ->
    #atm_task_execution_argument_spec{
        name = Name,
        input_spec = InputSpec,
        data_spec = AtmDataSpec,
        is_batch = IsBatch
    }.


%% @private
-spec build_arg(atm_task_execution:ctx(), atm_task_execution:arg_input_spec()) ->
    json_utils:json_term() | no_return().
build_arg(_AtmTaskExecutionArgSpec, #{
    <<"inputRefType">> := <<"const">>,
    <<"inputRef">> := ConstValue
}) ->
    ConstValue;

build_arg(#atm_task_execution_ctx{stores = Stores}, #{
    <<"inputRefType">> := <<"store">>,
    <<"inputRef">> := StoreSchemaId
}) ->
    case maps:get(StoreSchemaId, Stores, undefined) of
        undefined -> throw(?ERROR_ATM_REFERENCED_NONEXISTENT_STORE(StoreSchemaId));
        StoreCredentials -> StoreCredentials
    end;

build_arg(#atm_task_execution_ctx{item = Item}, #{
    <<"inputRefType">> := <<"item">>
} = InputSpec) ->
    case maps:get(<<"inputRef">>, InputSpec, undefined) of
        undefined ->
            Item;
        Query ->
            % TODO fix query in case of array indices
            case json_utils:query(Item, Query) of
                {ok, Value} -> Value;
                error -> throw(?ERROR_ATM_TASK_ARG_MAPPER_ITEM_QUERY_FAILED(Item, Query))
            end
    end;

build_arg(_AtmTaskExecutionArgSpec, _InputSpec) ->
    throw(?ERROR_ATM_TASK_ARG_MAPPER_INVALID_INPUT_SPEC).


%% @private
-spec validate_arg(
    json_utils:json_term() | [json_utils:json_term()],
    atm_task_execution:arg_spec()
) ->
    ok | no_return().
validate_arg(ArgsBatch, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_batch = true
}) ->
    lists:foreach(fun(ArgValue) ->
        atm_data_validator:assert_instance(ArgValue, AtmDataSpec)
    end, ArgsBatch);

validate_arg(ArgValue, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_batch = false
}) ->
    atm_data_validator:assert_instance(ArgValue, AtmDataSpec).

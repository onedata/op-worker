%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for constructing task execution argument specs and values.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_arguments).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build_specs/2, acquire_args/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_parameter_spec:record()],
    [atm_task_schema_argument_mapper:record()]
) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers) ->
    build_specs(
        lists:usort(fun order_atm_lambda_arg_specs_by_name/2, AtmLambdaArgSpecs),
        lists:usort(fun order_atm_task_schema_arg_mappers_by_name/2, AtmTaskSchemaArgMappers),
        []
    ).


-spec acquire_args(
    atm_workflow_execution_handler:item(),
    atm_run_job_batch_ctx:record(),
    [atm_task_execution_argument_spec:record()]
) ->
    json_utils:json_map() | no_return().
acquire_args(Item, AtmRunJobBatchCtx, AtmTaskExecutionArgSpecs) ->
    #atm_item_execution{trace_id = TraceId, value = ItemValue} = Item,

    lists:foldl(fun(AtmTaskExecutionArgSpec, Args) ->
        ArgName = atm_task_execution_argument_spec:get_name(AtmTaskExecutionArgSpec),
        try
            Args#{ArgName => atm_task_execution_argument_spec:acquire_arg(
                ItemValue, AtmRunJobBatchCtx, AtmTaskExecutionArgSpec
            )}
        catch Type:Reason:Stacktrace ->
            Error = ?examine_exception(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(ArgName, Error))
        end
    end, #{<<"__meta">> => #{<<"id">> => TraceId}}, AtmTaskExecutionArgSpecs).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec order_atm_lambda_arg_specs_by_name(
    atm_parameter_spec:record(),
    atm_parameter_spec:record()
) ->
    boolean().
order_atm_lambda_arg_specs_by_name(
    #atm_parameter_spec{name = Name1},
    #atm_parameter_spec{name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec order_atm_task_schema_arg_mappers_by_name(
    atm_task_schema_argument_mapper:record(),
    atm_task_schema_argument_mapper:record()
) ->
    boolean().
order_atm_task_schema_arg_mappers_by_name(
    #atm_task_schema_argument_mapper{argument_name = Name1},
    #atm_task_schema_argument_mapper{argument_name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec build_specs(
    OrderedUniqueAtmLambdaArgSpecs :: [atm_parameter_spec:record()],
    OrderedUniqueAtmTaskSchemaArgMappers :: [atm_task_schema_argument_mapper:record()],
    AtmTaskExecutionArgSpecs :: [atm_task_execution_argument_spec:record()]
) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_specs([], [], AtmTaskExecutionArgSpecs) ->
    AtmTaskExecutionArgSpecs;

build_specs(
    [#atm_parameter_spec{name = Name} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    [#atm_task_schema_argument_mapper{argument_name = Name} = AtmTaskSchemaArgMapper | RestAtmTaskSchemaArgMappers],
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, RestAtmTaskSchemaArgMappers, [
        atm_task_execution_argument_spec:build(AtmLambdaArgSpec, AtmTaskSchemaArgMapper)
        | AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_parameter_spec{default_value = Default} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) when Default /= undefined ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, [
        atm_task_execution_argument_spec:build(AtmLambdaArgSpec, undefined)
        | AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_parameter_spec{is_optional = true} | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, AtmTaskExecutionArgSpecs);

build_specs(
    [#atm_parameter_spec{name = Name} | _],
    _AtmTaskSchemaArgMappers,
    _AtmTaskExecutionArgSpecs
) ->
    throw(?ERROR_ATM_TASK_ARG_MAPPER_FOR_REQUIRED_LAMBDA_ARG_MISSING(Name));

build_specs(
    [],
    [#atm_task_schema_argument_mapper{argument_name = Name} | _],
    _AtmTaskExecutionArgSpecs
) ->
    throw(?ERROR_ATM_TASK_ARG_MAPPER_FOR_NONEXISTENT_LAMBDA_ARG(Name)).

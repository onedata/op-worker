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
-export([build_specs/2, construct_args/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_lambda_argument_spec:record()],
    [atm_task_schema_argument_mapper:record()]
) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers) ->
    build_specs(
        lists:usort(fun order_atm_lambda_arg_specs_by_name/2, AtmLambdaArgSpecs),
        lists:usort(fun order_atm_task_schema_arg_mappers_by_name/2, AtmTaskSchemaArgMappers),
        []
    ).


-spec construct_args(
    atm_job_execution_ctx:record(),
    [atm_task_execution_argument_spec:record()]
) ->
    json_utils:json_map() | no_return().
construct_args(AtmJobExecutionCtx, AtmTaskExecutionArgSpecs) ->
    BasicArgs = lists:foldl(fun(AtmTaskExecutionArgSpec, Args) ->
        ArgName = atm_task_execution_argument_spec:get_name(AtmTaskExecutionArgSpec),
        try
            Args#{ArgName => atm_task_execution_argument_spec:construct_arg(
                AtmJobExecutionCtx, AtmTaskExecutionArgSpec
            )}
        catch _:Reason ->
            throw(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(ArgName, Reason))
        end
    end, #{}, AtmTaskExecutionArgSpecs),

    BasicArgs#{<<"heartbeatUrl">> => atm_job_execution_ctx:get_heartbeat_url(AtmJobExecutionCtx)}.


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
    OrderedUniqueAtmLambdaArgSpecs :: [atm_lambda_argument_spec:record()],
    OrderedUniqueAtmTaskSchemaArgMappers :: [atm_task_schema_argument_mapper:record()],
    AtmTaskExecutionArgSpecs :: [atm_task_execution_argument_spec:record()]
) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_specs([], [], AtmTaskExecutionArgSpecs) ->
    AtmTaskExecutionArgSpecs;

build_specs(
    [#atm_lambda_argument_spec{name = Name} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    [#atm_task_schema_argument_mapper{argument_name = Name} = AtmTaskSchemaArgMapper | RestAtmTaskSchemaArgMappers],
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, RestAtmTaskSchemaArgMappers, [
        atm_task_execution_argument_spec:build(AtmLambdaArgSpec, AtmTaskSchemaArgMapper)
        | AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_lambda_argument_spec{default_value = Default} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) when Default /= undefined ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, [
        atm_task_execution_argument_spec:build(AtmLambdaArgSpec, undefined)
        | AtmTaskExecutionArgSpecs
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

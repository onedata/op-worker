%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for constructing task execution result specs and values.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_results).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build_specs/2, apply/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_lambda_result_spec:record()],
    [atm_task_schema_result_mapper:record()]
) ->
    [atm_task_execution_result_spec:record()] | no_return().
build_specs(AtmLambdaResultSpecs, AtmTaskSchemaResultMappers) ->
    AtmTaskSchemaResultMappersGroupedByName = group_atm_task_schema_result_mappers_by_name(
        AtmTaskSchemaResultMappers
    ),

    lists:foldl(fun(AtmLambdaResultSpec = #atm_lambda_result_spec{name = Name}, Acc) ->
        AtmTaskSchemaResultMappersForName = maps:get(
            Name, AtmTaskSchemaResultMappersGroupedByName, []
        ),
        AtmTaskExecutionResultSpec = atm_task_execution_result_spec:build(
            AtmLambdaResultSpec, AtmTaskSchemaResultMappersForName
        ),
        [AtmTaskExecutionResultSpec | Acc]
    end, [], lists:usort(fun order_atm_lambda_result_specs_by_name/2, AtmLambdaResultSpecs)).


-spec apply(
    atm_workflow_execution_ctx:record(),
    [atm_task_execution_result_spec:record()],
    json_utils:json_map()
) ->
    ok | no_return().
apply(AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpecs, Results) ->
    lists:foreach(fun(AtmTaskExecutionResultSpec) ->
        ResultName = atm_task_execution_result_spec:get_name(AtmTaskExecutionResultSpec),
        ResultValue = maps:get(ResultName, Results, undefined),
        apply_result(AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpec, ResultName, ResultValue)
    end, AtmTaskExecutionResultSpecs).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec order_atm_lambda_result_specs_by_name(
    atm_lambda_result_spec:record(),
    atm_lambda_result_spec:record()
) ->
    boolean().
order_atm_lambda_result_specs_by_name(
    #atm_lambda_result_spec{name = Name1},
    #atm_lambda_result_spec{name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec group_atm_task_schema_result_mappers_by_name([atm_task_schema_result_mapper:record()]) ->
    #{AtmTaskResultName :: binary() => [atm_task_schema_result_mapper:record()]}.
group_atm_task_schema_result_mappers_by_name(AtmTaskSchemaResultMappers) ->
    lists:foldl(fun(AtmTaskSchemaResultMapper, Acc) ->
        Name = AtmTaskSchemaResultMapper#atm_task_schema_result_mapper.result_name,
        AtmTaskSchemaResultMapperForName = maps:get(Name, Acc, []),
        Acc#{Name => [AtmTaskSchemaResultMapper | AtmTaskSchemaResultMapperForName]}
    end, #{}, AtmTaskSchemaResultMappers).


%% @private
-spec apply_result(
    atm_workflow_execution_ctx:record(),
    atm_task_execution_result_spec:record(),
    binary(),
    undefined | json_utils:json_term()
) ->
    ok | no_return().
apply_result(_AtmWorkflowExecutionCtx, _, <<"exception">>, _) ->
    % TODO VFS-8248 rm case when 'exception' result name became be forbidden
    % 'exception' result is optional - if it is present it should be handled
    % before code comes here and this case should never be called.
    % Since it was, exception hasn't occurred.
    ok;

apply_result(_AtmWorkflowExecutionCtx, _AtmTaskExecutionResultSpec, ResultName, undefined) ->
    throw(?ERROR_ATM_TASK_MISSING_RESULT(ResultName));

apply_result(AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpec, ResultName, ResultValue) ->
    try
        atm_task_execution_result_spec:apply_result(
            AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpec, ResultValue
        )
    catch _:Reason ->
        throw(?ERROR_ATM_TASK_RESULT_MAPPING_FAILED(ResultName, Reason))
    end.

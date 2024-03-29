%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for building task execution result specs and consuming
%%% result values.
%%% There are 2 types of task execution results:
%%% - item related - mandatory results returned directly by lambda and associated
%%%                  with items the lambda was called for. When their processing
%%%                  fails their associated items are saved in lane run exception
%%%                  store and after lane run failure the lane run can be retried.
%%% - uncorrelated - optional/extra results relayed via alternative channels
%%%                  e.g. asynchronously via websocket a.k.a. file pipe (may be
%%%                  used for streaming logs or time series measurements in real
%%%                  time). When their processing fails entire lane run should
%%%                  fail with no possibility of retrying it.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_results).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    build_specs/2,
    consume_results/4
]).

-type type() :: item_related | uncorrelated.
-export_type([type/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_lambda_result_spec:record()],
    [atm_task_schema_result_mapper:record()]
) ->
    {
        ItemRelatedResultSpecs :: [atm_task_execution_result_spec:record()],
        UncorrelatedResultSpecs :: [atm_task_execution_result_spec:record()]
    }.
build_specs(AtmLambdaResultSpecs, AtmTaskSchemaResultMappers) ->
    AtmTaskSchemaResultMappersGroupedPerName = group_atm_task_schema_result_mappers_by_name(
        AtmTaskSchemaResultMappers
    ),

    lists:foldl(fun(AtmLambdaResultSpec, {ItemRelatedResultSpecs, UncorrelatedResultSpecs}) ->
        ResultName = AtmLambdaResultSpec#atm_lambda_result_spec.name,
        ResultSpec = atm_task_execution_result_spec:build(
            AtmLambdaResultSpec,
            maps:get(ResultName, AtmTaskSchemaResultMappersGroupedPerName, [])
        ),
        case AtmLambdaResultSpec#atm_lambda_result_spec.relay_method of
            return_value -> {[ResultSpec | ItemRelatedResultSpecs], UncorrelatedResultSpecs};
            file_pipe -> {ItemRelatedResultSpecs, [ResultSpec | UncorrelatedResultSpecs]}
        end
    end, {[], []}, lists:usort(fun order_atm_lambda_result_specs_by_name/2, AtmLambdaResultSpecs)).


-spec consume_results(
    atm_workflow_execution_ctx:record(),
    type(),
    [atm_task_execution_result_spec:record()],
    json_utils:json_map()
) ->
    ok | no_return().
consume_results(AtmWorkflowExecutionCtx, Type, ResultSpecs, ResultValues) ->
    lists:foreach(fun(ResultSpec) ->
        ResultName = atm_task_execution_result_spec:get_name(ResultSpec),

        case maps:find(ResultName, ResultValues) of
            {ok, ResultValue} ->
                consume_result(AtmWorkflowExecutionCtx, ResultName, ResultSpec, ResultValue);
            error ->
                Type == item_related andalso throw(?ERROR_ATM_TASK_RESULT_MISSING(
                    ResultName, maps:keys(ResultValues)
                ))
        end
    end, ResultSpecs).


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
        Acc#{Name => [AtmTaskSchemaResultMapper | maps:get(Name, Acc, [])]}
    end, #{}, AtmTaskSchemaResultMappers).


%% @private
-spec consume_result(
    atm_workflow_execution_ctx:record(),
    automation:name(),
    atm_task_execution_result_spec:record(),
    json_utils:json_term()
) ->
    ok | no_return().
consume_result(AtmWorkflowExecutionCtx, ResultName, ResultSpec, ResultValue) ->
    try
        atm_task_execution_result_spec:consume_result(
            AtmWorkflowExecutionCtx,
            ResultSpec,
            ResultValue
        )
    catch Type:Reason:Stacktrace ->
        Error = ?examine_exception(Type, Reason, Stacktrace),
        throw(?ERROR_ATM_TASK_RESULT_MAPPING_FAILED(ResultName, Error))
    end.

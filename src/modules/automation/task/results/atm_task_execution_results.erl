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
-export([build_specs/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_lambda_result_spec:record()],
    [atm_task_schema_result_mapper:record()]
) ->
    [atm_task_execution_result_spec:record()].
build_specs(AtmLambdaResultSpecs, AtmTaskSchemaResultMappers) ->
    AtmTaskSchemaResultMappersGroupedByName = group_atm_task_schema_result_mappers_by_name(
        AtmTaskSchemaResultMappers
    ),

    lists:foldl(fun(AtmLambdaResultSpec = #atm_lambda_result_spec{name = Name}, Acc) ->
        AtmTaskSchemaResultMapperForName = maps:get(
            Name, AtmTaskSchemaResultMappersGroupedByName, []
        ),
        AtmTaskExecutionResultSpec = atm_task_execution_result_spec:build(
            AtmLambdaResultSpec, AtmTaskSchemaResultMapperForName
        ),
        [AtmTaskExecutionResultSpec | Acc]
    end, [], lists:usort(fun order_atm_lambda_result_specs_by_name/2, AtmLambdaResultSpecs)).


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

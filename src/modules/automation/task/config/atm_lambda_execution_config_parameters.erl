%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for constructing lambda execution config specs and values.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_execution_config_parameters).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build_specs/3, acquire_config/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    atm_workflow_execution_auth:record(),
    [atm_parameter_spec:record()],
    #{atm_parameter_spec:name() => json_utils:json_term()}
) ->
    [atm_lambda_execution_config_parameter_spec:record()] | no_return().
build_specs(AtmWorkflowExecutionAuth, AtmLambdaConfigParameterSpecs, AtmLambdaConfigValues) ->
    lists:foldl(fun(AtmParameterSpec = #atm_parameter_spec{name = ParameterName}, Acc) ->
        AtmLambdaExecutionConfigParameterSpec = atm_lambda_execution_config_parameter_spec:build(
            AtmWorkflowExecutionAuth,
            AtmParameterSpec,
            maps:get(ParameterName, AtmLambdaConfigValues, undefined)
        ),
        [AtmLambdaExecutionConfigParameterSpec | Acc]
    end, [], AtmLambdaConfigParameterSpecs).


-spec acquire_config(
    atm_run_job_batch_ctx:record(),
    [atm_lambda_execution_config_parameter_spec:record()]
) ->
    json_utils:json_map() | no_return().
acquire_config(AtmRunJobBatchCtx, AtmLambdaConfigParameterSpecs) ->
    lists:foldl(fun(AtmLambdaConfigParameterSpec, Config) ->
        ParameterName = atm_lambda_execution_config_parameter_spec:get_name(
            AtmLambdaConfigParameterSpec
        ),

        try
            Config#{ParameterName => atm_lambda_execution_config_parameter_spec:acquire_value(
                AtmRunJobBatchCtx, AtmLambdaConfigParameterSpec
            )}
        catch Type:Reason:Stacktrace ->
            Error = ?examine_exception(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_LAMBDA_CONFIG_BAD_VALUE(ParameterName, Error))
        end
    end, #{}, AtmLambdaConfigParameterSpecs).

%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for constructing lambda execution config entries.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_execution_config_entries).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build_entries/3, acquire_config/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_entries(
    atm_workflow_execution_auth:record(),
    [atm_parameter_spec:record()],
    #{atm_parameter_spec:name() => json_utils:json_term()}
) ->
    [atm_lambda_execution_config_entry:record()] | no_return().
build_entries(AtmWorkflowExecutionAuth, AtmLambdaConfigParameterSpecs, AtmLambdaConfigValues) ->
    lists:foldl(fun(AtmParameterSpec = #atm_parameter_spec{name = ParameterName}, Acc) ->
        try
            AtmLambdaExecutionConfigEntry = atm_lambda_execution_config_entry:build(
                AtmWorkflowExecutionAuth,
                AtmParameterSpec,
                maps:get(ParameterName, AtmLambdaConfigValues, undefined)
            ),
            [AtmLambdaExecutionConfigEntry | Acc]
        catch Type:Reason:Stacktrace ->
            Error = ?examine_exception(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_LAMBDA_CONFIG_BAD_VALUE(ParameterName, Error))
        end
    end, [], AtmLambdaConfigParameterSpecs).


-spec acquire_config(
    atm_run_job_batch_ctx:record(),
    [atm_lambda_execution_config_entry:record()]
) ->
    json_utils:json_map() | no_return().
acquire_config(AtmRunJobBatchCtx, AtmLambdaConfigEntries) ->
    maps_utils:generate_from_list(fun(AtmLambdaConfigEntry) ->
        Name = atm_lambda_execution_config_entry:get_name(AtmLambdaConfigEntry),

        try
            Value = atm_lambda_execution_config_entry:acquire_value(
                AtmRunJobBatchCtx, AtmLambdaConfigEntry
            ),
            {Name, Value}
        catch Type:Reason:Stacktrace ->
            Error = ?examine_exception(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_LAMBDA_CONFIG_BAD_VALUE(Name, Error))
        end
    end, AtmLambdaConfigEntries).

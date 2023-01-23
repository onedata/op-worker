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
-module(atm_lambda_execution_config).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build_specs/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs(
    [atm_parameter_spec:record()],
    #{atm_parameter_spec:name() => json_utils:json_term()}
) ->
    [atm_lambda_execution_config_spec:record()] | no_return().
build_specs(AtmLambdaConfigParameterSpecs, AtmLambdaConfigValues) ->
    lists:foldl(fun(AtmParameterSpec = #atm_parameter_spec{name = ParameterName}, Acc) ->
        Value = maps:get(ParameterName, AtmLambdaConfigValues, undefined),
        [atm_lambda_execution_config_spec:build(AtmParameterSpec, Value) | Acc]
    end, [], AtmLambdaConfigParameterSpecs).


%%%===================================================================
%%% Internal functions
%%%===================================================================

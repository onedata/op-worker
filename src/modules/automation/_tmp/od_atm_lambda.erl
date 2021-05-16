%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(od_atm_lambda).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([get/1]).

%%%===================================================================
%%% API functions
%%%===================================================================


get(_AtmLambdaId) ->
    #od_atm_lambda{
        name = str_utils:rand_hex(10),
        summary = str_utils:rand_hex(10),
        description = str_utils:rand_hex(10),

        operation_spec = #atm_openfaas_operation_spec{
            docker_image = <<"image">>,
            docker_execution_options = #atm_docker_execution_options{}
        },
        argument_specs = []
    }.

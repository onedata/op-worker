%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation lambda snapshot entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_snapshot_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, #atm_lambda_snapshot{
    name = AtmLambdaName,
    summary = AtmLambdaSummary,
    description = AtmLambdaDescription,

    operation_spec = AtmLambdaOperationSpec,
    argument_specs = AtmLambdaArgumentSpecs,
    result_specs = AtmLambdaResultSpecs,

    atm_inventories = AtmInventories
}) ->
    #{
        <<"name">> => AtmLambdaName,
        <<"summary">> => AtmLambdaSummary,
        <<"description">> => AtmLambdaDescription,

        <<"operationSpec">> => jsonable_record:to_json(AtmLambdaOperationSpec, atm_lambda_operation_spec),
        <<"argumentSpecs">> => jsonable_record:list_to_json(AtmLambdaArgumentSpecs, atm_lambda_argument_spec),
        <<"resultSpecs">> => jsonable_record:list_to_json(AtmLambdaResultSpecs, atm_lambda_result_spec),

        <<"atmInventoryList">> => AtmInventories
    }.

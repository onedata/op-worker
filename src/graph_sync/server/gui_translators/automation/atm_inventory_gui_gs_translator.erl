%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation inventory entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_inventory_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(GRI = #gri{aspect = instance, scope = private}, #od_atm_inventory{
    name = AtmInventoryName
}) ->
    #{
        <<"name">> => AtmInventoryName,
        <<"atmWorkflowSchemaList">> => gri:serialize(GRI#gri{
            aspect = atm_workflow_schemas,
            scope = private
        })
    };
translate_resource(#gri{aspect = atm_workflow_schemas, scope = private}, AtmWorkflowSchemas) ->
    #{
        <<"list">> => lists:map(fun(AtmWorkflowSchemaId) ->
            gri:serialize(#gri{
                type = op_atm_workflow_schema,
                id = AtmWorkflowSchemaId,
                aspect = instance,
                scope = private
            })
        end, AtmWorkflowSchemas)
    }.

%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation workflow schema snapshot entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_schema_snapshot_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) -> gs_protocol:data().
translate_resource(#gri{aspect = instance, scope = private}, #atm_workflow_schema_snapshot{
    schema_id = AtmWorkflowSchemaId,
    name = AtmWorkflowSchemaName,
    description = AtmWorkflowSchemaDescription,
    stores = AtmStoreSchemas,
    lanes = AtmLaneSchemas,
    state = AtmWorkflowSchemaState,
    atm_inventory = AtmInventoryId
}) ->
    #{
        <<"atmWorkflowSchema">> => gri:serialize(#gri{
            type = op_atm_workflow_schema, id = AtmWorkflowSchemaId,
            aspect = instance, scope = private
        }),
        <<"name">> => AtmWorkflowSchemaName,
        <<"description">> => AtmWorkflowSchemaDescription,

        <<"stores">> => jsonable_record:list_to_json(AtmStoreSchemas, atm_store_schema),
        <<"lanes">> => jsonable_record:list_to_json(AtmLaneSchemas, atm_lane_schema),

        <<"state">> => automation:workflow_schema_state_to_json(AtmWorkflowSchemaState),

        <<"atmInventory">> => gri:serialize(#gri{
            type = op_atm_inventory, id = AtmInventoryId,
            aspect = instance, scope = private
        })
    }.

%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation workflow schema entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_schema_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) -> gs_protocol:data().
translate_resource(#gri{aspect = instance, scope = private}, #od_atm_workflow_schema{
    name = AtmWorkflowSchemaName,
    summary = AtmWorkflowSchemaSummary,
    revision_registry = RevisionRegistry,
    compatible = IsCompatible,
    atm_inventory = AtmInventoryId
}) ->
    #{
        <<"name">> => AtmWorkflowSchemaName,
        <<"summary">> => AtmWorkflowSchemaSummary,
        <<"revisionRegistry">> => jsonable_record:to_json(
            RevisionRegistry, atm_workflow_schema_revision_registry
        ),
        <<"isCompatible">> => IsCompatible,
        <<"atmInventory">> => gri:serialize(#gri{
            type = op_atm_inventory, id = AtmInventoryId,
            aspect = instance, scope = private
        })
    }.

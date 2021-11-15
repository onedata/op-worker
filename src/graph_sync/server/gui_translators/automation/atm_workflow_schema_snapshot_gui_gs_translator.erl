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
    summary = AtmWorkflowSchemaSummary,
    revision_number = RevisionNumber,
    revision = Revision,
    atm_inventory = AtmInventoryId
}) ->
    #{
        <<"atmWorkflowSchema">> => gri:serialize(#gri{
            type = op_atm_workflow_schema, id = AtmWorkflowSchemaId,
            aspect = instance, scope = private
        }),
        <<"name">> => AtmWorkflowSchemaName,
        <<"summary">> => AtmWorkflowSchemaSummary,

        <<"revisionNumber">> => RevisionNumber,
        <<"revision">> => jsonable_record:to_json(Revision, atm_workflow_schema_revision),

        <<"atmInventory">> => gri:serialize(#gri{
            type = op_atm_inventory, id = AtmInventoryId,
            aspect = instance, scope = private
        })
    }.

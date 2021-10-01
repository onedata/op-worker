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
    name = AtmWorkflowSchemaName
} = AtmWorkflowSchema) ->
    #atm_workflow_schema_revision{
        description = AtmWorkflowSchemaDescription,
        stores = AtmStoreSchemas
    } = od_atm_workflow_schema:get_latest_revision(AtmWorkflowSchema),

    #{
        <<"name">> => AtmWorkflowSchemaName,
        <<"description">> => AtmWorkflowSchemaDescription,
        <<"stores">> => jsonable_record:list_to_json(AtmStoreSchemas, atm_store_schema)
    }.

%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation store entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = content}, AtmStoreContentBrowseResult) ->
    atm_store_content_browse_result:to_json(AtmStoreContentBrowseResult).


-spec translate_resource(gri:gri(), Data :: term()) -> gs_protocol:data().
translate_resource(#gri{aspect = instance, scope = private}, #atm_store{
    workflow_execution_id = AtmWorkflowExecutionId,
    schema_id = AtmStoreSchemaId,
    initial_content = InitialContent,
    frozen = Frozen,
    container = AtmsStoreContainer
}) ->
    AtmStoreType = atm_store_container:get_store_type(AtmsStoreContainer),
    AtmStoreConfig = atm_store_container:get_config(AtmsStoreContainer),

    #{
        <<"atmWorkflowExecution">> => gri:serialize(#gri{
            type = op_atm_workflow_execution, id = AtmWorkflowExecutionId,
            aspect = instance, scope = private
        }),
        <<"schemaId">> => AtmStoreSchemaId,

        <<"initialContent">> => utils:undefined_to_null(InitialContent),
        <<"frozen">> => Frozen,

        <<"type">> => atm_store:type_to_json(AtmStoreType),
        <<"config">> => atm_store:config_to_json(AtmStoreConfig)
    }.

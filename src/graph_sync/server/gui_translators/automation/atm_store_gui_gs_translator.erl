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
translate_value(#gri{aspect = content}, {Entries, IsLast}) ->
    #{
        <<"list">> => lists:map(fun
            ({Index, {ok, Value}}) ->
                #{
                    <<"index">> => Index,
                    <<"success">> => true,
                    <<"value">> => Value
                };
            ({Index, {error, _} = Error}) ->
                #{
                    <<"index">> => Index,
                    <<"success">> => false,
                    <<"error">> => errors:to_json(Error)
                }
        end, Entries),
        <<"isLast">> => IsLast
    }.


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

        <<"type">> => automation:store_type_to_json(AtmStoreType),
        <<"config">> => atm_store_config:encode(
            AtmStoreConfig, AtmStoreType, fun jsonable_record:to_json/2
        )
    }.

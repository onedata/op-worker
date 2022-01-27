%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper functions for management of automation inventory used in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_test_inventory).
-author("Bartosz Walkowicz").

-include("atm_test_schema.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").

-export([
    ensure_exists/0,
    get_id/0,

    add_user/1,
    add_workflow_schema/2,
    get_workflow_schema_id/1,
    get_workflow_schema_json/1
]).

-type atm_workflow_schema_alias() :: binary().
-type atm_workflow_schema_dump() :: #atm_workflow_schema_dump{}.

-export_type([
    atm_workflow_schema_alias/0,
    atm_workflow_schema_dump/0
]).


-define(ATM_INVENTORY_ID_KEY, atm_test_inventory_id).
-define(ATM_WORKFLOW_SCHEMA_ID_KEY(__ALIAS), {atm_workflow_schema_id, __ALIAS}).


%%%===================================================================
%%% API
%%%===================================================================


-spec ensure_exists() -> ok.
ensure_exists() ->
    case node_cache:get(?ATM_INVENTORY_ID_KEY, undefined) of
        undefined ->
            AtmInventoryId = ozt_atm:create_inventory(str_utils:rand_hex(10)),
            node_cache:put(?ATM_INVENTORY_ID_KEY, AtmInventoryId);
        _AtmInventoryId ->
            ok
    end.


-spec get_id() -> od_atm_inventory:id().
get_id() ->
    node_cache:get(?ATM_INVENTORY_ID_KEY).


-spec add_user(oct_background:entity_selector()) -> ok.
add_user(UserPlaceholder) ->
    UserId = oct_background:get_user_id(UserPlaceholder),
    ozt_atm:add_user_to_inventory(UserId, get_id()).


-spec add_workflow_schema(
    atm_workflow_schema_alias(),
    atm_workflow_schema_dump() | atm_test_schema_factory:atm_workflow_schema_dump_draft()
) ->
    ok.
add_workflow_schema(AtmWorkflowSchemaAlias, #atm_workflow_schema_dump{} = AtmWorkflowSchemaDump) ->
    AtmWorkflowSchemaDumpJson = atm_workflow_schema_dump_to_json(AtmWorkflowSchemaDump),
    AtmWorkflowSchemaId = ozt_atm:create_workflow_schema(AtmWorkflowSchemaDumpJson#{
        <<"atmInventoryId">> => get_id()
    }),
    node_cache:put(?ATM_WORKFLOW_SCHEMA_ID_KEY(AtmWorkflowSchemaAlias), AtmWorkflowSchemaId);

add_workflow_schema(AtmWorkflowSchemaAlias, AtmWorkflowSchemaDumpDraft) ->
    add_workflow_schema(
        AtmWorkflowSchemaAlias,
        atm_test_schema_factory:create_from_draft(AtmWorkflowSchemaDumpDraft)
    ).


-spec get_workflow_schema_id(atm_workflow_schema_alias()) -> od_atm_workflow_schema:id().
get_workflow_schema_id(AtmWorkflowSchemaAlias) ->
    node_cache:get(?ATM_WORKFLOW_SCHEMA_ID_KEY(AtmWorkflowSchemaAlias)).


-spec get_workflow_schema_json(atm_workflow_schema_alias()) -> json_utils:json_map().
get_workflow_schema_json(AtmWorkflowSchemaAlias) ->
    AtmWorkflowSchemaId = get_workflow_schema_id(AtmWorkflowSchemaAlias),

    ozw_test_rpc:call(provider_gs_translator, translate_resource, [
        ?GS_PROTOCOL_VERSION,
        #gri{type = od_atm_workflow_schema, aspect = instance, scope = private},
        ozt_atm:get_workflow_schema(AtmWorkflowSchemaId)
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec atm_workflow_schema_dump_to_json(atm_workflow_schema_dump()) -> json_utils:json_map().
atm_workflow_schema_dump_to_json(#atm_workflow_schema_dump{
    name = Name,
    summary = Summary,
    revision_num = AtmWorkflowSchemaRevisionNum,
    revision = AtmWorkflowSchemaRevision,
    supplementary_lambdas = SupplementaryLambdas
}) ->
    #{
        <<"schemaFormatVersion">> => 2,
        <<"name">> => Name,
        <<"summary">> => Summary,

        <<"revision">> => #{
            <<"originalRevisionNumber">> => AtmWorkflowSchemaRevisionNum,
            <<"atmWorkflowSchemaRevision">> => jsonable_record:to_json(
                AtmWorkflowSchemaRevision, atm_workflow_schema_revision
            ),
            <<"supplementaryAtmLambdas">> => maps:map(fun(AtmLambdaId, AtmLambdaRevisionRegistry) ->
                maps:map(fun(AtmLambdaRevisionNumBin, AtmLambdaRevision) ->
                    #{
                        <<"schemaFormatVersion">> => 2,
                        <<"originalAtmLambdaId">> => AtmLambdaId,
                        <<"revision">> => #{
                            <<"schemaFormatVersion">> => 2,
                            <<"originalRevisionNumber">> => binary_to_integer(AtmLambdaRevisionNumBin),
                            <<"atmLambdaRevision">> => AtmLambdaRevision
                        }
                    }
                end, jsonable_record:to_json(AtmLambdaRevisionRegistry, atm_lambda_revision_registry))
            end, SupplementaryLambdas)
        }
    }.

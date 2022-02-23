%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles management of singleton automation inventory created
%%% for use in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_test_inventory).
-author("Bartosz Walkowicz").

-include("atm_test_schema.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-export([
    init_per_suite/2,

    get_id/0,

    add_member/1,
    add_workflow_schema/1,
    get_workflow_schema/1,
    get_workflow_schema_revision/2
]).

-type atm_workflow_schema_dump() :: #atm_workflow_schema_dump{}.

-export_type([atm_workflow_schema_dump/0]).


-define(PROVIDER_SELECTOR_KEY, provider_selector).
-define(ATM_INVENTORY_ID_KEY, atm_test_inventory_id).
-define(ATM_INVENTORY_ADMIN_KEY, atm_test_inventory_admin).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_per_suite(
    oct_background:entity_selector(),
    oct_background:entity_selector()
) ->
    ok.
init_per_suite(ProviderSelector, AdminUserSelector) ->
    case node_cache:get(?ATM_INVENTORY_ID_KEY, undefined) of
        undefined ->
            node_cache:put(?PROVIDER_SELECTOR_KEY, ProviderSelector),

            AtmInventoryId = ozt_atm:create_inventory(<<"ONEPROVIDER CT TESTS">>),
            node_cache:put(?ATM_INVENTORY_ID_KEY, AtmInventoryId),

            UserId = oct_background:get_user_id(AdminUserSelector),
            add_user(UserId, privileges:atm_inventory_admin()),
            node_cache:put(?ATM_INVENTORY_ADMIN_KEY, UserId);

        _AtmInventoryId ->
            ct:pal("Attempt to init already initiated test inventory!!!"),
            error(not_gonna_happen)
    end.


-spec get_id() -> od_atm_inventory:id().
get_id() ->
    node_cache:get(?ATM_INVENTORY_ID_KEY).


-spec add_member(oct_background:entity_selector()) -> ok.
add_member(UserPlaceholder) ->
    UserId = oct_background:get_user_id(UserPlaceholder),
    add_user(UserId, privileges:atm_inventory_member()).


-spec add_workflow_schema(
    atm_workflow_schema_dump() | atm_test_schema_factory:atm_workflow_schema_dump_draft()
) ->
    od_atm_workflow_schema:id().
add_workflow_schema(#atm_workflow_schema_dump{} = AtmWorkflowSchemaDump) ->
    AtmWorkflowSchemaDumpJson = atm_workflow_schema_dump_to_json(AtmWorkflowSchemaDump),
    TestAtmInventoryId = get_id(),
    AtmWorkflowSchemaId = ozt_atm:create_workflow_schema(AtmWorkflowSchemaDumpJson#{
        <<"atmInventoryId">> => TestAtmInventoryId
    }),

    % Invalidate cached od_atm_inventory entry to force op to fetch it on access
    % rather than use stale data
    opt:invalidate_cache(od_atm_inventory, TestAtmInventoryId),

    AtmWorkflowSchemaId;

add_workflow_schema(AtmWorkflowSchemaDumpDraft) ->
    add_workflow_schema(atm_test_schema_factory:create_from_draft(AtmWorkflowSchemaDumpDraft)).


-spec get_workflow_schema(od_atm_workflow_schema:id()) -> od_atm_workflow_schema:record().
get_workflow_schema(AtmWorkflowSchemaId) ->
    AdminUserId = node_cache:get(?ATM_INVENTORY_ADMIN_KEY),
    ProviderSelector = node_cache:get(?PROVIDER_SELECTOR_KEY),
    AdminUserSessionId = oct_background:get_user_session_id(AdminUserId, ProviderSelector),

    {ok, #document{value = AtmWorkflowSchema}} = ?assertMatch(
        {ok, _},
        ?rpc(ProviderSelector, atm_workflow_schema_logic:get(AdminUserSessionId, AtmWorkflowSchemaId))
    ),
    AtmWorkflowSchema.


-spec get_workflow_schema_revision(
    atm_workflow_schema_revision:revision_number(),
    od_atm_workflow_schema:id()
) ->
    atm_workflow_schema_revision:record().
get_workflow_schema_revision(RevisionNum, AtmWorkflowSchemaId) ->
    #od_atm_workflow_schema{revision_registry = RevisionRegistry} = get_workflow_schema(
        AtmWorkflowSchemaId
    ),
    atm_workflow_schema_revision_registry:get_revision(RevisionNum, RevisionRegistry).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec add_user(od_user:id(), [privileges:atm_inventory_privilege()]) -> ok.
add_user(UserId, AtmInventoryPrivs) ->
    AtmInventoryId = get_id(),
    ozt_atm:add_user_to_inventory(UserId, AtmInventoryId, AtmInventoryPrivs),

    % Invalidate cached user entry to force op to fetch it on access rather
    % than use stale data
    opt:invalidate_cache(od_user, UserId).


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

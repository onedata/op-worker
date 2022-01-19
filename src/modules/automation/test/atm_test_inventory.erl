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

-include("modules/automation/atm_schema_test_utils.hrl").

-export([
    ensure_exists/0,
    get_id/0,

    add_user/1,
    add_workflow/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec ensure_exists() -> ok.
ensure_exists() ->
    case node_cache:get(atm_test_inventory_id, undefined) of
        undefined ->
            AtmInventoryId = ozt_atm:create_inventory(str_utils:rand_hex(10)),
            node_cache:put(atm_test_inventory_id, AtmInventoryId);
        _AtmInventoryId ->
            ok
    end.


-spec get_id() -> od_atm_inventory:id().
get_id() ->
    node_cache:get(atm_test_inventory_id).


-spec add_user(oct_background:entity_selector()) -> ok.
add_user(UserPlaceholder) ->
    UserId = oct_background:get_user_id(UserPlaceholder),
    ozt_atm:add_user_to_inventory(UserId, get_id()).


-spec add_workflow(atm_test_schema_factory:atm_workflow_schema_dump()) -> ok.
add_workflow(AtmWorkflowSchemaDump) ->
    AtmWorkflowSchemaDumpJson = atm_test_schema_factory:to_json(AtmWorkflowSchemaDump),
    AtmWorkflowSchemaId = ozt_atm:create_workflow_schema(AtmWorkflowSchemaDumpJson#{
        <<"atmInventoryId">> => get_id()
    }),
    AtmWorkflowSchemaId.


%%%===================================================================
%%% Internal functions
%%%===================================================================

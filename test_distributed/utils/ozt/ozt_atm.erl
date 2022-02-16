%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to automation operations in Onezone to be used
%%% in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ozt_atm).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-export([
    create_inventory/1,
    add_user_to_inventory/2, add_user_to_inventory/3,

    create_workflow_schema/1,
    get_workflow_schema/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_inventory(od_atm_inventory:name()) -> od_atm_inventory:id().
create_inventory(Name) ->
    {ok, AtmInventoryId} = ?assertMatch({ok, _}, ozw_test_rpc:call(atm_inventory_logic, create, [
        ?ROOT, #{<<"name">> => Name}
    ])),
    AtmInventoryId.


-spec add_user_to_inventory(od_user:id(), od_atm_inventory:id()) -> ok.
add_user_to_inventory(UserId, AtmInventoryId) ->
    add_user_to_inventory(UserId, AtmInventoryId, privileges:atm_inventory_member()).


-spec add_user_to_inventory(
    od_user:id(),
    od_atm_inventory:id(),
    [privileges:atm_inventory_privilege()]
) ->
    ok.
add_user_to_inventory(UserId, AtmInventoryId, Privileges) ->
    ?assertMatch({ok, _}, ozw_test_rpc:call(atm_inventory_logic, add_user, [
        ?ROOT, AtmInventoryId, UserId, Privileges
    ])),
    ok.


-spec create_workflow_schema(od_atm_inventory:name()) -> od_atm_workflow_schema:id().
create_workflow_schema(Data) ->
    {ok, AtmWorkflowSchemaId} = ?assertMatch({ok, _}, ozw_test_rpc:call(atm_workflow_schema_logic, create, [
        ?ROOT, Data
    ])),
    AtmWorkflowSchemaId.


-spec get_workflow_schema(od_atm_workflow_schema:id()) -> od_atm_workflow_schema:record().
get_workflow_schema(AtmWorkflowSchemaId) ->
    {ok, AtmWorkflowSchema} = ?assertMatch({ok, _}, ozw_test_rpc:call(atm_workflow_schema_logic, get, [
        ?ROOT, AtmWorkflowSchemaId
    ])),
    AtmWorkflowSchema.

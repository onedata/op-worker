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
    add_user_to_inventory/2,

    create_workflow_schema/1
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
    ?assertMatch({ok, _}, ozw_test_rpc:call(atm_inventory_logic, add_user, [
        ?ROOT, AtmInventoryId, UserId, privileges:atm_inventory_member()
    ])),
    ok.


-spec create_workflow_schema(od_atm_inventory:name()) -> od_atm_workflow_schema:id().
create_workflow_schema(Data) ->
    {ok, AtmWorkflowSchemaId} = ?assertMatch({ok, _}, ozw_test_rpc:call(atm_workflow_schema_logic, create, [
        ?ROOT, Data
    ])),
    AtmWorkflowSchemaId.
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

-export([
    create_inventory_for_user/2,
    add_user_to_inventory/3,
    create_workflow_schema/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_inventory_for_user(od_user:id(), od_atm_inventory:name()) ->
    od_atm_inventory:id().
create_inventory_for_user(UserId, Name) ->
    AtmInventoryId = ozw_test_rpc:create_inventory_for_user(
        ?ROOT, UserId, #{<<"name">> => Name}
    ),
    opt:invalidate_cache(od_user, UserId),
    AtmInventoryId.


-spec add_user_to_inventory(
    od_user:id(),
    od_atm_inventory:id(),
    [privileges:atm_inventory_privilege()]
) ->
    ok.
add_user_to_inventory(UserId, AtmInventoryId, Privileges) ->
    ozw_test_rpc:add_user_to_inventory(?ROOT, AtmInventoryId, UserId, Privileges),
    opt:invalidate_cache(od_user, UserId).


-spec create_workflow_schema(od_atm_inventory:name()) -> od_atm_workflow_schema:id().
create_workflow_schema(#{<<"atmInventoryId">> := AtmInventoryId} = Data) ->
    AtmWorkflowSchemaId = ozw_test_rpc:create_workflow_schema(?ROOT, Data),
    opt:invalidate_cache(od_atm_inventory, AtmInventoryId),
    AtmWorkflowSchemaId.

%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to spaces operations in Oneprovider to be 
%%% used in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_spaces).
-author("Michal Stanisz").

-include_lib("ctool/include/test/assertions.hrl").

-export([
    get_privileges/3,
    get_storage_id/2
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_privileges(oct_background:node_selector(), od_space:id(), od_user:id()) -> 
    privileges:privileges(privileges:space_privilege()).
get_privileges(ProviderSelector, SpaceId, UserId) ->
    {ok, Privs} = ?assertMatch({ok, _}, test_rpc:call(
        op_worker, ProviderSelector, space_logic, get_eff_privileges, [SpaceId, UserId])),
    Privs.


-spec get_storage_id(oct_background:node_selector(), od_space:id()) -> od_storage:id().
get_storage_id(ProviderSelector, SpaceId) ->
    {ok, StorageId} = ?assertMatch({ok, _}, test_rpc:call(op_worker, ProviderSelector, space_logic,
        get_local_supporting_storage, [SpaceId])),
    StorageId.

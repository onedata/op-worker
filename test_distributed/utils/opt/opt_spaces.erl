%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021-2026 Onedata (onedata.org)
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
    get_storage_id/2,
    get_support_size/2,
    get_occupancy/2, set_occupancy/3
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_privileges(oct_background:node_selector(), od_space:id(), od_user:id()) -> 
    privileges:privileges(privileges:space_privilege()).
get_privileges(ProviderSelector, SpaceId, UserId) ->
    {ok, Privs} = ?assertMatch({ok, _}, opw_test_rpc:call(
        ProviderSelector, space_logic, get_eff_privileges, [SpaceId, UserId])),
    Privs.


-spec get_storage_id(oct_background:node_selector(), od_space:id()) -> od_storage:id().
get_storage_id(ProviderSelector, SpaceId) ->
    {ok, StorageId} = ?assertMatch({ok, _}, opw_test_rpc:call(
        ProviderSelector, space_logic, get_local_supporting_storage, [SpaceId])),
    StorageId.


%% @doc Size (in bytes) the provider has granted the space as its support.
-spec get_support_size(oct_background:node_selector(), od_space:id()) -> non_neg_integer().
get_support_size(ProviderSelector, SpaceId) ->
    {ok, SupportSize} = ?assertMatch({ok, _}, opw_test_rpc:call(
        ProviderSelector, provider_logic, get_support_size, [SpaceId])),
    SupportSize.


%% @doc How much of the support the space currently occupies on the provider.
-spec get_occupancy(oct_background:node_selector(), od_space:id()) -> non_neg_integer().
get_occupancy(ProviderSelector, SpaceId) ->
    opw_test_rpc:call(ProviderSelector, space_quota, current_size, [SpaceId]).


%%--------------------------------------------------------------------
%% @doc
%% Fakes the occupancy by applying the difference between the current and the
%% target one - e.g. to bring the space to the brink of its quota without
%% actually writing that many bytes. Note that the occupancy keeps being
%% updated by whatever the test does afterwards.
%% @end
%%--------------------------------------------------------------------
-spec set_occupancy(oct_background:node_selector(), od_space:id(), non_neg_integer()) -> ok.
set_occupancy(ProviderSelector, SpaceId, TargetOccupancy) ->
    CurrentOccupancy = get_occupancy(ProviderSelector, SpaceId),
    opw_test_rpc:call(ProviderSelector, space_quota, apply_size_change, [
        SpaceId, TargetOccupancy - CurrentOccupancy
    ]),
    ok.

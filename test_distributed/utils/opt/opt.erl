%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions to be used in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt).
-author("Michal Stanisz").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-type entity() :: od_space | od_user.

-export([init_per_suite/2]).

-export([
    force_fetch_entity/2,
    force_fetch_entity/3,
    invalidate_cache/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_per_suite(test_config:config(), oct_background:onenv_test_config()) ->
    test_config:config().
init_per_suite(Config, OnenvTestConfig) ->
    ModulesToLoad = ?config(?LOAD_MODULES, Config, []),
    Posthook = OnenvTestConfig#onenv_test_config.posthook,
    oct_background:init_per_suite(
        [{?LOAD_MODULES, [space_setup_utils | ModulesToLoad]} | Config],
        OnenvTestConfig#onenv_test_config{
            posthook = fun(NewConfig) ->
                % mock existence of a dummy unhealthy storage (to check whether all works fine when one exists)
                space_setup_utils:mock_existence_of_unhealthy_storage(?config(op_worker_nodes, NewConfig)),
                Posthook(NewConfig)
            end
        }
    ).


-spec force_fetch_entity(entity(), binary()) -> ok.
force_fetch_entity(Entity, Id) ->
    force_fetch_entity(Entity, Id, oct_background:get_provider_ids()).


-spec force_fetch_entity(entity(), binary(), [oct_background:entity_selector()]) -> ok.
force_fetch_entity(Entity, Id, ProviderSelectors) ->
    GRI = #gri{type = Entity, id = Id, aspect = instance},
    lists:foreach(fun(ProviderSelector) ->
        SessionId = get_session_id_required_to_force_fetch_entity(Entity, Id, ProviderSelector),
        ?assertMatch({ok, _}, opw_test_rpc:call(ProviderSelector, gs_client_worker, force_fetch_entity, [
            SessionId, GRI
        ]))
    end, ProviderSelectors).


-spec invalidate_cache(gs_protocol:entity_type(), binary()) -> ok.
invalidate_cache(Entity, Id) ->
    lists:foreach(fun(ProviderId) ->
        ?assertEqual(ok, opw_test_rpc:call(ProviderId, Entity, invalidate_cache, [Id]))
    end, oct_background:get_provider_ids()).



%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
get_session_id_required_to_force_fetch_entity(od_user, UserId, ProviderId) ->
    oct_background:get_user_session_id(UserId, ProviderId);
get_session_id_required_to_force_fetch_entity(od_space, _, _) ->
    ?ROOT_SESS_ID.

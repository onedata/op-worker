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

-include_lib("ctool/include/test/assertions.hrl").

-type entity() :: od_space.

-export([
    force_fetch_entity/2,
    invalidate_cache/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec force_fetch_entity(entity(), binary()) -> ok.
force_fetch_entity(Entity, Id) ->
    Providers = oct_background:get_provider_ids(),
    lists:foreach(fun(P) ->
        ?assertMatch({ok, _}, opw_test_rpc:call(P, entity_to_logic_module(Entity), force_fetch, [Id]))
    end, Providers).


-spec invalidate_cache(gs_protocol:entity_type(), binary()) -> ok.
invalidate_cache(Entity, Id) ->
    lists:foreach(fun(ProviderId) ->
        ?assertEqual(ok, opw_test_rpc:call(ProviderId, Entity, invalidate_cache, [Id]))
    end, oct_background:get_provider_ids()).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec entity_to_logic_module(entity()) -> module().
entity_to_logic_module(od_space) -> space_logic.

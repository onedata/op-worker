%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to spaces to be used in in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ozt_spaces).
-author("Michal Stanisz").

-include_lib("ctool/include/test/assertions.hrl").

-export([set_privileges/3]).

set_privileges(SpaceId, UserId, SpacePrivs) ->
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId, SpacePrivs),
    Providers = oct_background:get_provider_ids(),
    lists:foreach(fun(P) ->
        ?assertMatch({ok, _}, rpc:call(oct_background:get_random_provider_node(P), space_logic, force_fetch, [SpaceId]))
    end, Providers).

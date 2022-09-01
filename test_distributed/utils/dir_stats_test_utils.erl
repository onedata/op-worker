%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2011 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Test utils for dir stats collecting.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_stats_test_utils).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/test/test_utils.hrl").


%% API
-export([disable_stats_counting_ct_posthook/1, disable_stats_counting/1, enable_stats_counting/1]).


-type config() :: proplists:proplist().

%%%===================================================================
%%% API
%%%===================================================================

-spec disable_stats_counting_ct_posthook(config()) -> config().
disable_stats_counting_ct_posthook(Config) ->
    disable_stats_counting(Config),
    Config.


-spec disable_stats_counting(config()) -> ok.
disable_stats_counting(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, [dir_stats_service_state]),
    ok = test_utils:mock_expect(Workers, dir_stats_service_state, is_active, fun(_SpaceId) -> false end),
    ok = test_utils:mock_expect(
        Workers, dir_stats_service_state, get_extended_status, fun(_SpaceId) -> disabled end).


-spec enable_stats_counting(config()) -> ok.
enable_stats_counting(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dir_stats_service_state]).
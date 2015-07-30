%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests for helpers module.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

-define(call(N, M, A), ?call(N, helpers, M, A)).
-define(call(N, Mod, M, A), rpc:call(N, Mod, M, A)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([mkdir_test/1]).

all() ->
    [mkdir_test].


%%%===================================================================
%%% Tests
%%%===================================================================

mkdir_test(Config) ->
    CTX = ?config(ctx, Config),

    ?assertMatch(ok, helpers:mkdir(CTX, <<"test">>, 8#755)),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    lists:keystore(ctx, 1, Config, {ctx, helpers:new_handle(<<"DirectIO">>, [list_to_binary(?config(data_dir, Config))])}).

end_per_testcase(_, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
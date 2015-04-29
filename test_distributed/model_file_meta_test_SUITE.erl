%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(model_file_meta_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-define(call(N, M, A), ?call(N, file_meta, M, A)).
-define(call(N, Mod, M, A), rpc:call(N, Mod, M, A)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([basic_operations_test/1]).

-performance({test_cases, []}).
all() ->
    [basic_operations_test].

%%%===================================================================
%%% Tests
%%%===================================================================

basic_operations_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    ?assertMatch({ok, _}, ?call(Worker1, create, [<<"/">>, #file_meta{name = <<"spaces">>, is_scope = true}])),
    ?assertMatch({ok, _}, ?call(Worker1, create, [<<"/spaces">>, #file_meta{name = <<"Space 1">>, is_scope = true}])),
    ?assertMatch({ok, _}, ?call(Worker1, create, [<<"/spaces/Space 1">>, #file_meta{name = <<"dir1">>}])),

    ok.



%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================
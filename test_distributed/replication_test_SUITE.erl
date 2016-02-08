%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Replication tests.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([sample_test/1]).


-performance({test_cases, []}).
all() ->
    [
        sample_test
    ].

-define(REMOTE_APPLIER, applier).


%%%===================================================================
%%% Test functions
%%%===================================================================

sample_test(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),
    ct:print("~p, ~p", [Workers, lfm_proxy:ls(W1, SessId, {path, <<"/">>}, 10, 0)]).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    initializer:setup_storage(ConfigWithNodes).
    %todo mock dbsync_utils:get_providers_for_space/1

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    tracer:start(Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================


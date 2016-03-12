%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(subscriptions_test_SUITE).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([registers_for_updates/1]).

-define(MESSAGES_WAIT_TIMEOUT, timer:seconds(3)).
-define(MESSAGES_RECEIVE_ATTEMPTS, 30).

all() -> ?ALL([
    registers_for_updates
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

registers_for_updates(_Config) ->
    expect_message([], 1, []),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config1) ->
    ?TEST_INIT(Config1, ?TEST_FILE(Config1, "env_desc.json")).

end_per_suite(Config) ->
    ok.

init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Self = self(),

    test_utils:mock_new(Nodes, subscription_wss),
    test_utils:mock_expect(Nodes, subscription_wss, healthcheck,
        fun() -> ok end),
    test_utils:mock_expect(Nodes, subscription_wss, push,
        fun(Message) -> Self ! Message end),
    test_utils:mock_expect(Nodes, subscription_wss, start_link, fun() ->
        {ok, Self}
    end),

    flush(),
    Config.

end_per_testcase(_, Config) ->

    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, subscription_wss),
    test_node_starter:clean_environment(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

expectation(Users, ResumeAt, Missing) ->
    json_utils:encode([
        {users, lists:usort(Users)},
        {resume_at, ResumeAt},
        {missing, Missing}
    ]).

expect_message(Users, ResumeAt, Missing) ->
    Match = expectation(Users, ResumeAt, Missing),
    ct:print("expect ~p", [Match]),
    receive Match -> ok
    after ?MESSAGES_WAIT_TIMEOUT -> ?assertMatch(timeout, Match) end.

flush() ->
    receive _ -> ok
    after 0 -> ok end.
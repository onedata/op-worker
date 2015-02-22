%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This test checks requests routing inside OP cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(requests_routing_test_SUITE).
-author("Michal Wrzeszcz").

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("cluster_elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([simple_call_test/1, direct_cast_test/1, redirect_cast_test/1, mixed_cast_test/1]).

all() -> [simple_call_test, direct_cast_test, redirect_cast_test, mixed_cast_test].

-define(REQUEST_TIMEOUT, timer:seconds(10)).

%%%===================================================================
%%% Test function
%% ====================================================================

%% -perf_test(). % to execute test once
-perf_test({repeats, 100}).
simple_call_test(Config) ->
    [Ccm] = ?config(op_ccm_nodes, Config),
    [Worker] = ?config(op_worker_nodes, Config),

    ?assertEqual(pong, rpc:call(Ccm, worker_proxy, call, [http_worker, ping, ?REQUEST_TIMEOUT, random])),
    ?assertEqual(pong, rpc:call(Worker, worker_proxy, call, [http_worker, ping, ?REQUEST_TIMEOUT, random])),
    ?assertEqual(pong, rpc:call(Ccm, worker_proxy, call, [http_worker, ping, ?REQUEST_TIMEOUT, prefer_local])),
    ?assertEqual(pong, rpc:call(Worker, worker_proxy, call, [http_worker, ping, ?REQUEST_TIMEOUT, prefer_local])).

%%%===================================================================

-perf_test([
    {repeats, 100},
    {perf_config, [{proc_num, 100}, {proc_repeats, 100}]},
    {ct_config, [{proc_num, 10}, {proc_repeats, 10}]}
]).
direct_cast_test(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    ProcSendNum = ?config(proc_repeats, Config),
    ProcNum = ?config(proc_num, Config),

    TestProc = fun() ->
        Self = self(),
        SendReq = fun(MsgId) ->
            ?assertEqual(ok, rpc:call(Worker, worker_proxy, cast, [http_worker, ping, {proc, Self}, MsgId, prefer_local]))
        end,
        for(1, ProcSendNum, SendReq),
        count_answers(ProcSendNum)
    end,

    ?assertEqual(ok, spawn_and_check(TestProc, ProcNum)).

%%%===================================================================

-perf_test([
    {repeats, 100},
    {perf_config, [{proc_num, 100}, {proc_repeats, 100}]},
    {ct_config, [{proc_num, 10}, {proc_repeats, 10}]}
]).
redirect_cast_test(Config) ->
    [Ccm] = ?config(op_ccm_nodes, Config),
    ProcSendNum = ?config(proc_repeats, Config),
    ProcNum = ?config(proc_num, Config),

    TestProc = fun() ->
        Self = self(),
        SendReq = fun(MsgId) ->
            ?assertEqual(ok, rpc:call(Ccm, worker_proxy, cast, [http_worker, ping, {proc, Self}, MsgId, random]))
        end,
        for(1, ProcSendNum, SendReq),
        count_answers(ProcSendNum)
    end,

    ?assertEqual(ok, spawn_and_check(TestProc, ProcNum)).

%%%===================================================================

-perf_test([
    {repeats, 100},
    {perf_configs, [
        [{proc_num, 100}, {proc_repeats, 1}],
        [{proc_num, 1}, {proc_repeats, 100}],
        [{proc_num, 100}, {proc_repeats, 100}]
    ]},
    {ct_config, [{proc_num, 10}, {proc_repeats, 10}]}
]).
mixed_cast_test(Config) ->
    [Ccm] = ?config(op_ccm_nodes, Config),
    [Worker] = ?config(op_worker_nodes, Config),
    ProcSendNum = ?config(proc_repeats, Config),
    ProcNum = ?config(proc_num, Config),

    TestProc = fun() ->
        Self = self(),
        SendReq = fun(MsgId) ->
            ?assertEqual(ok, rpc:call(Ccm, worker_proxy, cast, [http_worker, ping, {proc, Self}, 2*MsgId-1, random])),
            ?assertEqual(ok, rpc:call(Worker, worker_proxy, cast, [http_worker, ping, {proc, Self}, 2*MsgId, prefer_local]))
        end,
        for(1, ProcSendNum, SendReq),
        count_answers(2*ProcSendNum)
    end,

    ?assertEqual(ok, spawn_and_check(TestProc, ProcNum)).

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

spawn_and_check(_Fun, 0) ->
    ok;

spawn_and_check(Fun, Num) ->
    Master = self(),
    spawn_link(fun() ->
        Fun(),
        Master ! ok
    end),
    case spawn_and_check(Fun, Num - 1) of
        ok ->
            receive
                ok -> ok
            after ?REQUEST_TIMEOUT ->
                {error, timeout}
            end;
        E ->
            E
    end.

for(N, N, F) -> [F(N)];
for(I, N, F) -> [F(I)|for(I+1, N, F)].

count_answers(Exp) ->
    count_answers(0, Exp).

count_answers(Exp, Exp) ->
    ok;

count_answers(Num, Exp) ->
    Ans = receive
              #worker_answer{id = Num, response = Response} -> Response
          after ?REQUEST_TIMEOUT ->
              {error, timeout}
          end,
    ?assertEqual(pong, Ans),
    count_answers(Num + 1, Exp).
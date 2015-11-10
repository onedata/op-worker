%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test creates many Erlang virtual machines and uses them
%% to test basic elements of oneprovider Erlang cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_elements_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("cluster/worker/elements/task_manager/task_manager.hrl").
-include("cluster/worker/modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("annotations/include/annotations.hrl").

-define(DICT_KEY, transactions_list).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([ccm_and_worker_test/1, task_pool_test/1, task_manager_repeats_test/1, task_manager_rerun_test/1,
    transaction_test/1, transaction_rollback_test/1, transaction_rollback_stop_test/1,
    multi_transaction_test/1, transaction_retry_test/1, transaction_error_test/1]).
-export([transaction_retry_test_base/0, transaction_error_test_base/0]).

-performance({test_cases, []}).
all() -> [ccm_and_worker_test, task_pool_test, task_manager_repeats_test, task_manager_rerun_test,
    transaction_test, transaction_rollback_test, transaction_rollback_stop_test,
    multi_transaction_test, transaction_retry_test, transaction_error_test].

%%%===================================================================
%%% Test functions
%%%===================================================================

ccm_and_worker_test(Config) ->
    % given
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    % then
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [dns_worker, ping])).

task_pool_test(Config) ->
    % given
    [W1, W2] = ?config(op_worker_nodes, Config),

    Tasks = [{#task_pool{task = t1}, ?NON_LEVEL, W1}, {#task_pool{task = t2}, ?NODE_LEVEL, W1},
        {#task_pool{task = t3}, ?CLUSTER_LEVEL, W1}, {#task_pool{task = t4}, ?CLUSTER_LEVEL, W2},
        {#task_pool{task = t5}, ?PERSISTENT_LEVEL, W1}, {#task_pool{task = t6}, ?PERSISTENT_LEVEL, W2},
        {#task_pool{task = t7}, ?PERSISTENT_LEVEL, W1}
    ],

    % then
    CreateAns = lists:foldl(fun({Task, Level, Worker}, Acc) ->
        {A1, A2} = rpc:call(Worker, task_pool, create, [Level, #document{value = Task#task_pool{owner = self()}}]),
        ?assertMatch({ok, _}, {A1, A2}),
        [{A2, Level} | Acc]
    end, [], Tasks),
    [_ | Keys] = lists:reverse(CreateAns),

    NewNames = [t2_2, t3_2, t4_2, t5_2, t6_2, t7_2],
    ToUpdate = lists:zip(Keys, NewNames),

    lists:foreach(fun({{Key, Level}, NewName}) ->
        ?assertMatch({ok, _}, rpc:call(W1, task_pool, update, [Level, Key, #{task => NewName}]))
    end, ToUpdate),

    ListTest = [{?NODE_LEVEL, [t2_2]}, {?CLUSTER_LEVEL, [t3_2, t4_2]}],
    %TODO - add PERSISTENT_LEVEL checking when list on db will be added
%%     ListTest = [{?NODE_LEVEL, [t2_2]}, {?CLUSTER_LEVEL, [t3_2, t4_2]}, {?PERSISTENT_LEVEL, [t5_2, t6_2, t7_2]}],
    lists:foreach(fun({Level, Names}) ->
        ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
        {A1, ListedTasks} = rpc:call(W1, task_pool, list, [Level]),
        ?assertMatch({ok, _}, {A1, ListedTasks}),
        ?assertEqual(length(Names), length(ListedTasks)),
        lists:foreach(fun(T) ->
            V = T#document.value,
            ?assert(lists:member(V#task_pool.task, Names))
        end, ListedTasks)
    end, ListTest),

    lists:foreach(fun({{Key, Level}, _}) ->
        ?assertMatch(ok, rpc:call(W1, task_pool, delete, [Level, Key]))
    end, ToUpdate).

task_manager_repeats_test(Config) ->
    task_manager_repeats_test_base(Config, ?NON_LEVEL, 0),
    task_manager_repeats_test_base(Config, ?NODE_LEVEL, 3),
    task_manager_repeats_test_base(Config, ?CLUSTER_LEVEL, 5).
% TODO Uncomment when list on db will be added (without list, task cannot be repeted)
%%     task_manager_repeats_test_base(Config, ?PERSISTENT_LEVEL, 5).

task_manager_repeats_test_base(Config, Level, FirstCheckNum) ->
    [W1, W2] = ?config(op_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    ControllerPid = start_tasks(Level, Workers, 5),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(FirstCheckNum, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    timer:sleep(timer:seconds(1)),
    ?assertEqual(0, count_answers()),
    timer:sleep(timer:seconds(6)),
    ?assertEqual(5, count_answers()),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    ControllerPid ! kill.

task_manager_rerun_test(Config) ->
    task_manager_rerun_test_base(Config, ?NON_LEVEL, 0),
    task_manager_rerun_test_base(Config, ?NODE_LEVEL, 3),
    task_manager_rerun_test_base(Config, ?CLUSTER_LEVEL, 5).
% TODO Uncomment when list on db will be added (without list, task cannot be repeted)
%%     task_manager_rerun_test_base(Config, ?PERSISTENT_LEVEL, 5).

task_manager_rerun_test_base(Config, Level, FirstCheckNum) ->
    [W1, W2] = WorkersList = ?config(op_worker_nodes, Config),
    Workers = [W1, W2, W1, W2, W1],

    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, application, set_env, [?APP_NAME, task_fail_sleep_time_ms, 100]))
    end, WorkersList),

    ControllerPid = start_tasks(Level, Workers, 25),
    {A1, A2} = rpc:call(W1, task_pool, list, [Level]),
    ?assertMatch({ok, _}, {A1, A2}),
    ?assertEqual(FirstCheckNum, length(A2)),
    ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),

    timer:sleep(timer:seconds(5)),
    ?assertEqual(0, count_answers()),

    case Level of
        ?NON_LEVEL ->
            ok;
        _ ->
            lists:foreach(fun(W) ->
                gen_server:cast({?NODE_MANAGER_NAME, W}, check_tasks)
            end, WorkersList),
            timer:sleep(timer:seconds(2)),
            ?assertEqual(5, count_answers()),
            ?assertEqual({ok, []}, rpc:call(W1, task_pool, list, [Level])),
            ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level]))
    end,

    ControllerPid ! kill.

start_tasks(Level, Workers, Num) ->
    ControllerPid = spawn(fun() -> task_controller([]) end),
    Master = self(),
    {Funs, _} = lists:foldl(fun(_W, {Acc, Counter}) ->
        NewAcc = [fun() ->
            ControllerPid ! {get_num, Counter, self()},
            receive
                {value, MyNum} ->
                    case MyNum of
                        Num ->
                            Master ! task_done,
                            ok;
                        _ ->
                            error
                    end
            end
        end | Acc],
        {NewAcc, Counter + 1}
    end, {[], 1}, Workers),

    lists:foreach(fun({Fun, W}) ->
        ?assertMatch(ok, rpc:call(W, task_manager, start_task, [Fun, Level]))
    end, lists:zip(Funs, Workers)),

    ControllerPid.

transaction_test(_Config) ->
    ?assertEqual(undefined, get(?DICT_KEY)),
    ?assertEqual(ok, transaction:start()),
    ?assertEqual(1, length(get(?DICT_KEY))),

    ?assertEqual(ok, transaction:commit()),
    ?assertEqual(0, length(get(?DICT_KEY))),
    ok.

transaction_rollback_test(_Config) ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),
    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:rollback(1)),
    ?assertEqual(0, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(2)),
    ?assertEqual(ok, get_rollback_ans(3)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

transaction_rollback_stop_test(_Config) ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),
    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    Rollback2 = fun(_Num) ->
        {ok, stop}
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:rollback(1)),
    ?assertEqual(0, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(2)),
    ?assertEqual(ok, get_rollback_ans(3)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

multi_transaction_test(_Config) ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),
    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:start()),
    ?assertEqual(2, length(get(?DICT_KEY))),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(ok, transaction:rollback(1)),
    ?assertEqual(1, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(non, get_rollback_ans()),

    ?assertEqual(ok, transaction:rollback(11)),
    ?assertEqual(0, length(get(?DICT_KEY))),

    ?assertEqual(ok, get_rollback_ans(11)),
    ?assertEqual(ok, get_rollback_ans(12)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

transaction_retry_test(Config) ->
    [Worker1, _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker1, ?MODULE, transaction_retry_test_base, [])),
    ok.

transaction_retry_test_base() ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),

    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    Rollback2 = fun(Num) ->
        Pid = self(),
        case Pid of
            Self ->
                {retry, Num + 100, some_reason};
            _ ->
                Self ! {rollback, Num},
                {ok, Num + 1}
        end
    end,

    Rollback3 = fun(Num) ->
        Pid = self(),
        case Pid of
            Self ->
                {retry, Num + 100, some_reason};
            _ when Num < 1000 ->
                {retry, Num + 1000, some_reason};
            _ ->
                Self ! {rollback, Num},
                {ok, Num + 1}
        end
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback3)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual(task_sheduled, transaction:rollback(1)),
    timer:sleep(1000), % task is asynch
    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(102)),
    ?assertEqual(ok, get_rollback_ans(303)),
    ?assertEqual(ok, get_rollback_ans(1304)),
    ?assertEqual(ok, get_rollback_ans(1305)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

transaction_error_test(Config) ->
    [Worker1, _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker1, ?MODULE, transaction_error_test_base, [])),
    ok.

transaction_error_test_base() ->
    ?assertEqual(ok, transaction:start()),

    Self = self(),

    Rollback1 = fun(Num) ->
        Self ! {rollback, Num},
        {ok, Num + 1}
    end,

    Rollback2 = fun(Num) ->
        Pid = self(),
        case Pid of
            Self ->
                {error, some_error};
            _ ->
                Self ! {rollback, Num},
                {ok, Num + 10}
        end
    end,

    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),
    ?assertEqual(ok, transaction:rollback_point(Rollback2)),
    ?assertEqual(ok, transaction:rollback_point(Rollback1)),

    ?assertEqual({rollback_fun_error,  {error, some_error}}, transaction:rollback(1)),
    timer:sleep(1000), % task is asynch
    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(1)),
    ?assertEqual(ok, get_rollback_ans(2)),
    ?assertEqual(ok, get_rollback_ans(12)),
    ?assertEqual(ok, get_rollback_ans(13)),
    ?assertEqual(non, get_rollback_ans()),

    ok.

get_rollback_ans() ->
    receive
        Other -> {error, Other}
    after
        0 -> non
    end.

get_rollback_ans(Num) ->
    receive
        {rollback, Num} -> ok;
        Other -> {error, Other}
    after
        0 -> non
    end.

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

task_controller(State) ->
    receive
        kill ->
            ok;
        {get_num, Sender, AnsPid} ->
            Num = proplists:get_value(Sender, State, 0),
            State2 = [{Sender, Num + 1} | State -- [{Sender, Num}]],
            AnsPid ! {value, Num + 1},
            task_controller(State2)
    end.

count_answers() ->
    receive
        task_done ->
            count_answers() + 1
    after
        0 ->
            0
    end.
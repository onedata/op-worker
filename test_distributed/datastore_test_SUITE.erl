%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore main API based on 'some_record' model.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("annotations/include/annotations.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").

-define(call_store(N, M, A), ?call_store(N, datastore, M, A)).
-define(call_store(N, Mod, M, A), rpc:call(N, Mod, M, A)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    local_cache_test/1, global_cache_test/1, global_cache_atomic_update_test/1,
    global_cache_list_test/1, persistance_test/1, local_cache_list_test/1,
    disk_only_links_test/1, global_only_links_test/1, globally_cached_links_test/1, link_walk_test/1,
    cache_monitoring_test/1, old_keys_cleaning_test/1, cache_clearing_test/1]).
-export([utilize_memory/2]).

-performance({test_cases, []}).
all() ->
    [local_cache_test, global_cache_test, global_cache_atomic_update_test,
     global_cache_list_test, persistance_test, local_cache_list_test,
     disk_only_links_test, global_only_links_test, globally_cached_links_test, link_walk_test,
     cache_monitoring_test, old_keys_cleaning_test, cache_clearing_test].

%%%===================================================================
%%% Test functions
%%%===================================================================

% checks if cache is monitored
cache_monitoring_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),
    disable_cache_control_and_set_dump_delay(Workers, timer:seconds(5)), % Automatic cleaning may influence results
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, application, set_env, [?APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(10)]))
    end, Workers),

    Key = <<"key">>,
    ?assertMatch({ok, _}, ?call_store(Worker1, some_record, create, [
        #document{
            key = Key,
            value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
        }])),

    timer:sleep(1000), % Posthook is async
    Uuid = caches_controller:get_cache_uuid(Key, some_record),
    ?assertMatch(true, ?call_store(Worker1, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
    ?assertMatch(true, ?call_store(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),

    ?assertMatch({ok, false}, ?call_store(Worker2, exists, [disk_only, some_record, Key])),
    timer:sleep(5000),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, Key])),

    % Check dump delay
    Key2 = <<"key2">>,
    for(2, fun() ->
        ?assertMatch({ok, _}, ?call_store(Worker1, some_record, save, [
            #document{
                key = Key2,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),
        timer:sleep(3500)
    end),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists, [disk_only, some_record, Key2])),
    timer:sleep(5000),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, Key2])),

    % Check forced dump
    Key3 = <<"key3">>,
    for(4, fun() ->
        ?assertMatch({ok, _}, ?call_store(Worker1, some_record, save, [
            #document{
                key = Key3,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),
        timer:sleep(3500)
    end),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, Key3])),

    ok.

% checks if caches controller clears caches
old_keys_cleaning_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),
    disable_cache_control(Workers), % Automatic cleaning may influence results

    Times = [timer:hours(7*24), timer:hours(24), timer:hours(1), timer:minutes(10), 0],
    {_, KeysWithTimes} = lists:foldl(fun(T, {Num, Ans}) ->
        K = list_to_binary("old_keys_cleaning_test"++integer_to_list(Num)),
        {Num + 1, [{K, T} | Ans]}
    end, {1, []}, Times),

    lists:foreach(fun({K, _T}) ->
        ?assertMatch({ok, _}, ?call_store(Worker1, some_record, create, [
            #document{
                key = K,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }]))
    end, KeysWithTimes),
    timer:sleep(1000), % Posthook is async
    ?assertMatch(ok, rpc:call(Worker1, caches_controller, wait_for_cache_dump, [])),
    check_clearing(lists:reverse(KeysWithTimes), Worker1, Worker2),

    CorruptedKey = list_to_binary("old_keys_cleaning_test_c"),
    ?assertMatch({ok, _}, ?call_store(Worker1, some_record, create, [
        #document{
            key = CorruptedKey,
            value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
        }])),
    timer:sleep(1000), % Posthook is async
    ?assertMatch(ok, rpc:call(Worker1, caches_controller, wait_for_cache_dump, [])),
    CorruptedUuid = caches_controller:get_cache_uuid(CorruptedKey, some_record),
    ?assertMatch(ok, ?call_store(Worker2, cache_controller, delete, [?GLOBAL_ONLY_LEVEL, CorruptedUuid])),

    ?assertMatch(ok, ?call_store(Worker1, caches_controller, delete_old_keys, [globally_cached, 1])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [global_only, some_record, CorruptedKey])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, CorruptedKey])),

    ?assertMatch(ok, ?call_store(Worker1, caches_controller, delete_old_keys, [globally_cached, 0])),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists, [global_only, some_record, CorruptedKey])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, CorruptedKey])),
    ok.

% helper fun used by old_keys_cleaning_test
check_clearing([], _Worker1, _Worker2) ->
    ok;
check_clearing([{K, TimeWindow} | R] = KeysWithTimes, Worker1, Worker2) ->
    lists:foreach(fun({K2, T}) ->
        Uuid = caches_controller:get_cache_uuid(K2, some_record),
        UpdateFun = fun
            (Record) ->
                Record#cache_controller{timestamp = to_timestamp(from_timestamp(os:timestamp()) - T - timer:minutes(5))}
        end,
        ?assertMatch({ok, _}, ?call_store(Worker1, cache_controller, update, [?GLOBAL_ONLY_LEVEL, Uuid, UpdateFun]))
    end, KeysWithTimes),

    ?assertMatch(ok, ?call_store(Worker1, caches_controller, delete_old_keys, [globally_cached, TimeWindow])),

    Uuid = caches_controller:get_cache_uuid(K, some_record),
    ?assertMatch(false, ?call_store(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid])),
    ?assertMatch({ok, false}, ?call_store(Worker2, exists, [global_only, some_record, K])),
    ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, K])),
    ?assertMatch({ok, _}, ?call_store(Worker1, some_record, get, [K])),
    ?assertMatch(true, ?call_store(Worker2, some_record, exists, [K])),

    lists:foreach(fun({K2, _}) ->
        Uuid2 = caches_controller:get_cache_uuid(K2, some_record),
        ?assertMatch(true, ?call_store(Worker2, cache_controller, exists, [?GLOBAL_ONLY_LEVEL, Uuid2])),
        ?assertMatch({ok, true}, ?call_store(Worker2, exists, [global_only, some_record, K2])),
        ?assertMatch({ok, true}, ?call_store(Worker2, exists, [disk_only, some_record, K2]))
    end, R),
    timer:sleep(1000), % TODO - rewrite checking to disable hooks during tests

    check_clearing(R, Worker1, Worker2).

% checks if node_managaer clears memory correctly
cache_clearing_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),
    disable_cache_control(Workers), % Automatic cleaning may influence results

    [{_, Mem0}] = monitoring:get_memory_stats(),
    Mem0Node = rpc:call(Worker2, erlang, memory, [total]),
    ct:print("Mem0 ~p xxx ~p", [Mem0, Mem0Node]),
    FreeMem = 100 - Mem0,
    ToAdd = min(20, FreeMem/2),
    MemTarget = Mem0 + ToAdd/2,
    MemUsage = Mem0 + ToAdd,

    ?assertMatch(ok, rpc:call(Worker1, ?MODULE, utilize_memory, [MemUsage, MemTarget])),
    ?assertMatch(ok, rpc:call(Worker2, application, set_env, [?APP_NAME, mem_to_clear_cache, MemTarget])),
    [{_, Mem1}] = monitoring:get_memory_stats(),
    Mem1Node = rpc:call(Worker2, erlang, memory, [total]),
    ct:print("Mem1 ~p xxx ~p", [Mem1, Mem1Node]),
    ?assert(Mem1 > MemTarget),

    timer:sleep(1000), % Posthook is async
    ?assertMatch(ok, rpc:call(Worker2, caches_controller, wait_for_cache_dump, [])),
    % TODO Add answer checking when DB nodes will be run at separate machine
    CheckMemAns = gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, 60000),
%%     ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, Worker2}, check_mem_synch, 60000)),
    [{_, Mem2}] = monitoring:get_memory_stats(),
    Mem2Node = rpc:call(Worker2, erlang, memory, [total]),
    ct:print("Mem2 ~p xxx ~p, check_mem: ~p", [Mem2, Mem2Node, CheckMemAns]),
    % TODO Change to node memory checking when DB nodes will be run at separate machine
    ?assert(Mem2Node < (Mem0Node + Mem1Node)/2),
%%     ?assert(Mem2 < MemTarget),

    ok.

% helper fun used by cache_clearing_test
utilize_memory(MemUsage, MemTarget) ->
    application:set_env(?APP_NAME, mem_to_clear_cache, MemTarget),

    OneDoc = list_to_binary(prepare_list(256 * 1024)),

    Add100MB = fun(_KeyBeg) ->
        for(1, 100 * 4, fun(I) ->
            some_record:create(
                #document{
                    value = #some_record{field1 = I, field2 = binary:copy(OneDoc), field3 = {test, tuple}}
                })
        end)
    end,
    Guard = fun() ->
        [{_, Current}] = monitoring:get_memory_stats(),
        Current >= MemUsage
    end,
    while(Add100MB, Guard),
    ok.

%% Simple usage of get/update/create/exists/delete on local cache driver (on several nodes)
local_cache_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = local_only,

    local_access_only(Worker1, Level),
    local_access_only(Worker2, Level),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = some_other_key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch({ok, false},
        ?call_store(Worker2, exists, [Level,
            some_record, some_other_key])),

    ?assertMatch({ok, true},
        ?call_store(Worker1, exists, [Level,
            some_record, some_other_key])),

    ok.


%% Simple usage of get/update/create/exists/delete on global cache driver (on several nodes)
global_cache_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = ?GLOBAL_ONLY_LEVEL,

    local_access_only(Worker1, Level),
    local_access_only(Worker2, Level),

    global_access(Config, Level),

    ok.


%% Simple usage of get/update/create/exists/delete on persistamce driver (on several nodes)
persistance_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = ?DISK_ONLY_LEVEL,

    local_access_only(Worker1, Level),
    local_access_only(Worker2, Level),

    global_access(Config, Level),

    ok.


%% Atomic update on global cache driver (on several nodes)
global_cache_atomic_update_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = ?GLOBAL_ONLY_LEVEL,
    Key = rand_key(),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 0, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    Pid = self(),
    ?assertMatch({ok, Key},
        ?call_store(Worker2, update, [Level,
            some_record, Key,
            fun(#some_record{field1 = 0} = Record) ->
                Record#some_record{field2 = Pid}
            end])),

    ?assertMatch({ok, #document{value = #some_record{field1 = 0, field2 = Pid}}},
        ?call_store(Worker1, get, [Level,
            some_record, Key])),

    UpdateFun = fun(#some_record{field1 = Value} = Record) ->
        Record#some_record{field1 = Value + 1}
    end,

    Self = self(),
    Timeout = timer:seconds(30),
    utils:pforeach(fun(Node) ->
        ?call_store(Node, update, [Level, some_record, Key, UpdateFun]),
        Self ! done
    end, lists:duplicate(100, Worker1) ++ lists:duplicate(100, Worker2)),
    [receive done -> ok after Timeout -> ok end || _ <- lists:seq(1, 200)],

    ?assertMatch({ok, #document{value = #some_record{field1 = 200}}},
        ?call_store(Worker1, get, [Level,
            some_record, Key])),

    ok.


%% list operation on global cache driver (on several nodes)
global_cache_list_test(Config) ->
    generic_list_test(?config(op_worker_nodes, Config), ?GLOBAL_ONLY_LEVEL).


%% list operation on local cache driver (on several nodes)
local_cache_list_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    generic_list_test([Worker1], ?LOCAL_ONLY_LEVEL),
    generic_list_test([Worker2], ?LOCAL_ONLY_LEVEL),
    ok.

%% Simple usege of link_walk
link_walk_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),
    
    Level = ?DISK_ONLY_LEVEL,
    Key1 = rand_key(),
    Key2 = rand_key(),
    Key3 = rand_key(),

    Doc1 = #document{
        key = Key1,
        value = #some_record{field1 = 1}
    },

    Doc2 = #document{
        key = Key2,
        value = #some_record{field1 = 2}
    },

    Doc3 = #document{
        key = Key3,
        value = #some_record{field1 = 3}
    },

    %% Create some documents and links
    ?assertMatch({ok, _},
        ?call_store(Worker1, some_record, create, [Doc1])),

    ?assertMatch({ok, _},
        ?call_store(Worker1, some_record, create, [Doc2])),

    ?assertMatch({ok, _},
        ?call_store(Worker1, some_record, create, [Doc3])),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [Level, Doc1, [{some, Doc2}, {other, Doc1}]])),
    ?assertMatch(ok, ?call_store(Worker1, add_links, [Level, Doc2, [{link, Doc3}, {parent, Doc1}]])),

    Res0 = ?call_store(Worker1, link_walk, [Level, Doc1, [some, link], get_leaf]),
    Res1 = ?call_store(Worker2, link_walk, [Level, Doc1, [some, parent], get_leaf]),

    ?assertMatch({ok, {#document{key = Key3, value = #some_record{field1 = 3}}, [Key2, Key3]}}, Res0),
    ?assertMatch({ok, {#document{key = Key1, value = #some_record{field1 = 1}}, [Key2, Key1]}}, Res1),

    ok.


%% Simple usege of (add/fetch/delete/foreach)_link
disk_only_links_test(Config) ->
    generic_links_test(Config, ?DISK_ONLY_LEVEL).


%% Simple usege of (add/fetch/delete/foreach)_link
global_only_links_test(Config) ->
    generic_links_test(Config, ?GLOBAL_ONLY_LEVEL).


%% Simple usege of (add/fetch/delete/foreach)_link
globally_cached_links_test(Config) ->
    generic_links_test(Config, ?GLOBALLY_CACHED_LEVEL).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= persistance_test;
    Case =:= disk_only_links_test;
    Case =:= global_only_links_test;
    Case =:= link_walk_test ->
    Workers = ?config(op_worker_nodes, Config),

    Methods = [save, get, exists, delete, update, create, fetch_link, delete_links],
    ModelConfig = lists:map(fun(Method) ->
        {some_record, Method}
    end, Methods),

    lists:foreach(fun(W) ->
        lists:foreach(fun(MC) ->
            ?assert(rpc:call(W, ets, delete_object, [datastore_local_state, {MC, cache_controller}]))
        end, ModelConfig)
    end, Workers),
    Config;

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(Case, Config) when
    Case =:= persistance_test;
    Case =:= disk_only_links_test;
    Case =:= global_only_links_test;
    Case =:= link_walk_test ->
    Workers = ?config(op_worker_nodes, Config),

    Methods = [save, get, exists, delete, update, create, fetch_link, delete_links],
    ModelConfig = lists:map(fun(Method) ->
        {some_record, Method}
    end, Methods),

    lists:foreach(fun(W) ->
        lists:foreach(fun(MC) ->
            ?assert(rpc:call(W, ets, insert, [datastore_local_state, {MC, cache_controller}]))
        end, ModelConfig)
    end, Workers);

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    [W | _] = Workers,
    ?assertMatch(ok, rpc:call(W, caches_controller, wait_for_cache_dump, [])),
    ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, W}, clear_mem_synch, 60000)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec local_access_only(Node :: atom(), Level :: datastore:store_level()) -> ok.
local_access_only(Node, Level) ->
    Key = rand_key(),

    ?assertMatch({ok, Key},
        ?call_store(Node, create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch({error, already_exists},
        ?call_store(Node, create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch({ok, true},
        ?call_store(Node, exists, [Level,
            some_record, Key])),

    ?assertMatch({ok, #document{value = #some_record{field3 = {test, tuple}}}},
        ?call_store(Node, get, [Level,
            some_record, Key])),

    Pid = self(),
    ?assertMatch({ok, Key},
        ?call_store(Node, update, [Level,
            some_record, Key, #{field2 => Pid}])),

    ?assertMatch({ok, #document{value = #some_record{field2 = Pid}}},
        ?call_store(Node, get, [Level,
            some_record, Key])),

    ?assertMatch(ok,
        ?call_store(Node, delete, [Level,
            some_record, Key, fun() -> false end])),

    ?assertMatch({ok, #document{value = #some_record{field2 = Pid}}},
        ?call_store(Node, get, [Level,
            some_record, Key])),

    ?assertMatch(ok,
        ?call_store(Node, delete, [Level,
            some_record, Key])),

    ?assertMatch({error, {not_found, _}},
        ?call_store(Node, get, [Level,
            some_record, Key])),

    ?assertMatch({error, {not_found, _}},
        ?call_store(Node, update, [Level,
            some_record, Key, #{field2 => self()}])),

    ok.


-spec global_access(Config :: term(), Level :: datastore:store_level()) -> ok.
global_access(Config, Level) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Key = rand_key(),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch({ok, true},
        ?call_store(Worker2, exists, [Level,
            some_record, Key])),

    ?assertMatch({ok, true},
        ?call_store(Worker1, exists, [Level,
            some_record, Key])),

    ?assertMatch({error, already_exists},
        ?call_store(Worker2, create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch({ok, #document{value = #some_record{field1 = 1, field3 = {test, tuple}}}},
        ?call_store(Worker1, get, [Level,
            some_record, Key])),

    ?assertMatch({ok, #document{value = #some_record{field1 = 1, field3 = {test, tuple}}}},
        ?call_store(Worker2, get, [Level,
            some_record, Key])),

    ?assertMatch({ok, _},
        ?call_store(Worker1, update, [Level,
            some_record, Key, #{field1 => 2}])),

    ?assertMatch({ok, #document{value = #some_record{field1 = 2}}},
        ?call_store(Worker2, get, [Level,
            some_record, Key])),

    ok.


generic_links_test(Config, Level) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Key1 = rand_key(),
    Key2 = rand_key(),
    Key3 = rand_key(),

    Doc1 = #document{
        key = Key1,
        value = #some_record{field1 = 1}
    },

    Doc2 = #document{
        key = Key2,
        value = #some_record{field1 = 2}
    },

    Doc3 = #document{
        key = Key3,
        value = #some_record{field1 = 3}
    },


    %% Create some documents and links
    ?assertMatch({ok, _},
        ?call_store(Worker1, some_record, create, [Doc1])),

    ?assertMatch({ok, _},
        ?call_store(Worker2, some_record, create, [Doc2])),

    ?assertMatch({ok, _},
        ?call_store(Worker1, some_record, create, [Doc3])),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [Level, Doc1, [{link2, Doc2}, {link3, Doc3}]])),

    %% Fetch all links and theirs targets
    Ret0 = ?call_store(Worker2, fetch_link_target, [Level, Doc1, link2]),
    Ret1 = ?call_store(Worker1, fetch_link_target, [Level, Doc1, link3]),
    Ret2 = ?call_store(Worker2, fetch_link, [Level, Doc1, link2]),
    Ret3 = ?call_store(Worker1, fetch_link, [Level, Doc1, link3]),

    ?assertMatch({ok, {Key2, some_record}}, Ret2),
    ?assertMatch({ok, {Key3, some_record}}, Ret3),
    ?assertMatch({ok, #document{key = Key2, value = #some_record{field1 = 2}}}, Ret0),
    ?assertMatch({ok, #document{key = Key3, value = #some_record{field1 = 3}}}, Ret1),

    ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc1, [link2, link3]])),
    timer:sleep(1000), % wait for hooks

    Ret4 = ?call_store(Worker2, fetch_link_target, [Level, Doc1, link2]),
    Ret5 = ?call_store(Worker1, fetch_link_target, [Level, Doc1, link3]),
    Ret6 = ?call_store(Worker2, fetch_link, [Level, Doc1, link2]),
    Ret7 = ?call_store(Worker1, fetch_link, [Level, Doc1, link3]),

    ?assertMatch({error, link_not_found}, Ret6),
    ?assertMatch({error, link_not_found}, Ret7),
    ?assertMatch({error, link_not_found}, Ret4),
    ?assertMatch({error, link_not_found}, Ret5),

    ?assertMatch(ok, ?call_store(Worker2, add_links, [Level, Doc1, [{link2, Doc2}, {link3, Doc3}]])),
    ?assertMatch(ok,
        ?call_store(Worker1, some_record, delete, [Key2])),
    timer:sleep(1000), % wait for hooks
    ?assertMatch(ok, ?call_store(Worker1, delete_links, [Level, Doc1, link3])),
    timer:sleep(1000), % wait for hooks

    Ret8 = ?call_store(Worker1, fetch_link_target, [Level, Doc1, link2]),
    Ret9 = ?call_store(Worker2, fetch_link_target, [Level, Doc1, link3]),
    Ret10 = ?call_store(Worker1, fetch_link, [Level, Doc1, link2]),
    Ret11 = ?call_store(Worker2, fetch_link, [Level, Doc1, link3]),

    ?assertMatch({ok, {Key2, some_record}}, Ret10),
    ?assertMatch({error, link_not_found}, Ret11),
    ?assertMatch({error, link_not_found}, Ret9),
    ?assertMatch({error, {not_found, _}}, Ret8),

    %% Delete on document shall delete all its links
    ?assertMatch(ok,
        ?call_store(Worker1, some_record, delete, [Key1])),
    timer:sleep(timer:seconds(1)),

    Ret12 = ?call_store(Worker2, fetch_link, [Level, Doc1, link2]),
    ?assertMatch({error, link_not_found}, Ret12),

    ok = ?call_store(Worker2, delete, [Level, some_record, Key1]),
    ok = ?call_store(Worker2, delete, [Level, some_record, Key2]),
    ok = ?call_store(Worker2, delete, [Level, some_record, Key3]),

    ok.


%% generic list operation (on several nodes)
generic_list_test(Nodes, Level) ->
    
    Ret0 = ?call_store(rand_node(Nodes), list, [Level, some_record, ?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret0),
    {ok, Objects0} = Ret0,

    ObjCount = 3424,
    Keys = [rand_key() || _ <- lists:seq(1, ObjCount)],

    CreateDocFun = fun(Key) ->
        ?call_store(rand_node(Nodes), create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])
        end,

    [?assertMatch({ok, _}, CreateDocFun(Key)) || Key <- Keys],

    Ret1 = ?call_store(rand_node(Nodes), list, [Level, some_record, ?GET_ALL, []]),
    ?assertMatch({ok, _}, Ret1),
    {ok, Objects1} = Ret1,
    ReceivedKeys = lists:map(fun(#document{key = Key}) -> Key end, Objects1 -- Objects0),

    ?assertMatch(ObjCount, erlang:length(Objects1) - erlang:length(Objects0)),
    ?assertMatch([], ReceivedKeys -- Keys),

    ok.

rand_key() ->
    base64:encode(crypto:rand_bytes(8)).

rand_node(Nodes) when is_list(Nodes) ->
    lists:nth(crypto:rand_uniform(1, length(Nodes) + 1), Nodes);
rand_node(Node) when is_atom(Node) ->
    Node.

prepare_list(1) ->
    "x";

prepare_list(Size) ->
    "x" ++ prepare_list(Size - 1).

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

while(F, Guard) ->
    while(0, F, Guard).

while(Counter, F, Guard) ->
    case Guard() of
        true ->
            ok;
        _ ->
            F(Counter),
            while(Counter+1, F, Guard)
    end.

from_timestamp({Mega,Sec,Micro}) ->
    (Mega*1000000 + Sec)*1000 + Micro/1000.

to_timestamp(T) ->
    {trunc(T/1000000000), trunc(T/1000) rem 1000000, trunc(T*1000) rem 1000000}.

disable_cache_control(Workers) ->
    disable_cache_control_and_set_dump_delay(Workers, 1000).

disable_cache_control_and_set_dump_delay(Workers, Delay) ->
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control)),
        ?assertEqual(ok, rpc:call(W, application, set_env, [?APP_NAME, cache_to_disk_delay_ms, Delay]))
    end, Workers).

for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).
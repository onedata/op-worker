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

-include("workers/datastore/datastore_models.hrl").

-define(call_store(N, M, A), rpc:call(N, datastore, M, A)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([local_cache_test/1, global_cache_test/1, global_cache_atomic_update_test/1]).

all() -> [local_cache_test, global_cache_test, global_cache_atomic_update_test].

%%%===================================================================
%%% Test function
%% ====================================================================

local_cache_test(Config) ->
    [CCM] = ?config(op_ccm_nodes, Config),
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = local_only,

    local_access_only(Worker1, Level),
    local_access_only(Worker2, Level),
    local_access_only(CCM, Level),

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = some_other_key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch(false,
        ?call_store(Worker2, exists, [Level,
            some_record, some_other_key])),

    ?assertMatch(false,
        ?call_store(CCM, exists, [Level,
            some_record, some_other_key])),

    ?assertMatch(true,
        ?call_store(Worker1, exists, [Level,
            some_record, some_other_key])),

    ok.


global_cache_test(Config) ->
    [CCM] = ?config(op_ccm_nodes, Config),
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = global_only,

    local_access_only(CCM, Level),
    local_access_only(Worker1, Level),
    local_access_only(Worker2, Level),

    global_access(Config, Level),

    ok.


global_cache_atomic_update_test(Config) ->
    [_CCM] = ?config(op_ccm_nodes, Config),
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Level = global_only,
    Key = some_key_atomic,

    %% Load this module into oneprovider nodes so that update fun() will be available
    {Mod, Bin, File} = code:get_object_code(?MODULE),
    {_Replies, _} = rpc:multicall([Worker1, Worker2], code, load_binary,
        [Mod, File, Bin]),

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

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).


-spec local_access_only(Node :: atom(), Level :: datastore:store_level()) -> ok.
local_access_only(Node, Level) ->
    Key = some_key,

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

    ?assertMatch(true,
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
    [CCM] = ?config(op_ccm_nodes, Config),
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Key = some_other_key,

    ?assertMatch({ok, _},
        ?call_store(Worker1, create, [Level,
            #document{
                key = Key,
                value = #some_record{field1 = 1, field2 = <<"abc">>, field3 = {test, tuple}}
            }])),

    ?assertMatch(true,
        ?call_store(Worker2, exists, [Level,
            some_record, Key])),

    ?assertMatch(true,
        ?call_store(CCM, exists, [Level,
            some_record, Key])),

    ?assertMatch(true,
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
            some_record, some_other_key])),

    ?assertMatch({ok, #document{value = #some_record{field1 = 1, field3 = {test, tuple}}}},
        ?call_store(CCM, get, [Level,
            some_record, Key])),

    ?assertMatch({ok, _},
        ?call_store(Worker1, update, [Level,
            some_record, Key, #{field1 => 2}])),

    ?assertMatch({ok, #document{value = #some_record{field1 = 2}}},
        ?call_store(Worker2, get, [Level,
            some_record, Key])),

    ok.
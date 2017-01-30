%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests for file_meta model.
%%% @end
%%%-------------------------------------------------------------------
-module(model_file_meta_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(call(N, F, A), ?call(N, file_meta, F, A)).
-define(call(N, M, F, A), rpc:call(N, M, F, A)).

%% export for ct
-export([all/0, init_per_suite/1]).
%% tests
-export([basic_operations_test/1, rename_test/1]).
%% test_bases
-export([basic_operations_test_base/1]).

all() ->
    ?ALL([basic_operations_test, rename_test], [basic_operations_test]).

-define(REPEATS, 100).
-define(SUCCESS_RATE, 99).

%%%===================================================================
%%% Test functions
%%%===================================================================

basic_operations_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, last_level}, {value, 10}, {description, "Depth of last level"}]
        ]},
        {description, "Performs operations on file meta model"},
        {config, [{name, basic_config},
            {parameters, [
                [{name, last_level}, {value, 50}]
            ]},
            {description, "Basic config for test"}
        ]}
    ]
    ).
basic_operations_test_base(Config) ->
    LastLevel = ?config(last_level, Config),
    model_file_meta_test_base:basic_operations_test_core(Config, LastLevel).

rename_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    {A2, U2} = ?call(Worker2, create, [{path, <<"/">>}, #file_meta{name = <<"Space 1">>, is_scope = true}]),
    {A3, U3} = ?call(Worker2, create, [{path, <<"/">>}, #file_meta{name = <<"Space 2">>, is_scope = true}]),
    {A4, U4} = ?call(Worker1, create, [{path, <<"/Space 1">>}, #file_meta{name = <<"d1">>}]),
    {A5, U5} = ?call(Worker1, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f1">>}]),
    {A20, U20} = ?call(Worker1, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f2">>}]),
    {A21, U21} = ?call(Worker1, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f3">>}]),
    {A22, U22} = ?call(Worker1, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f4">>}]),
    {A23, U23} = ?call(Worker1, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"dd1">>}]),
    {A24, U24} = ?call(Worker1, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"dd2">>}]),
    {A25, U25} = ?call(Worker1, create, [{path, <<"/Space 1/d1/dd1">>}, #file_meta{name = <<"f1">>}]),
    {A26, U26} = ?call(Worker1, create, [{path, <<"/Space 1/d1/dd1">>}, #file_meta{name = <<"f2">>}]),
    {A27, U27} = ?call(Worker1, create, [{path, <<"/Space 1/d1/dd2">>}, #file_meta{name = <<"f1">>}]),
    {A28, U28} = ?call(Worker1, create, [{path, <<"/Space 1/d1/dd2">>}, #file_meta{name = <<"f2">>}]),
    ?assertMatch({ok, _}, {A2, U2}),
    ?assertMatch({ok, _}, {A3, U3}),
    ?assertMatch({ok, _}, {A4, U4}),
    ?assertMatch({ok, _}, {A5, U5}),

    ?assertMatch({ok, _}, {A20, U20}),
    ?assertMatch({ok, _}, {A21, U21}),
    ?assertMatch({ok, _}, {A22, U22}),
    ?assertMatch({ok, _}, {A23, U23}),
    ?assertMatch({ok, _}, {A24, U24}),
    ?assertMatch({ok, _}, {A25, U25}),
    ?assertMatch({ok, _}, {A26, U26}),
    ?assertMatch({ok, _}, {A27, U27}),
    ?assertMatch({ok, _}, {A28, U28}),

    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/f1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/f2">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/f3">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/f4">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/dd1/f1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/dd1/f2">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/dd2/f1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d1/dd2/f2">>}])),


    {A8, U8} = ?call(Worker2, get, [{path, <<"/Space 1/d1">>}]),
    ?assertMatch({ok, _}, {A8, U8}),

    ?assertMatch(ok, ?call(Worker2, rename, [U8, {name, <<"d2">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/Space 1/d1">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d2">>}}}, ?call(Worker2, get, [{path, <<"/Space 1/d2">>}])),

    ?assertMatch(ok, ?call(Worker2, rename, [{path, <<"/Space 1/d2">>}, {name, <<"d3">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/Space 1/d2">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d3">>}}}, ?call(Worker2, get, [{path, <<"/Space 1/d3">>}])),

    ?assertMatch({ok, _}, ?call(Worker2, get, [{path, <<"/Space 1/d3/f1">>}])),

    ?assertMatch(ok, ?call(Worker2, rename, [{path, <<"/Space 1/d3">>}, {path, <<"/Space 1/d2">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/Space 1/d3">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d2">>}}}, ?call(Worker2, get, [{path, <<"/Space 1/d2">>}])),

    ?assertMatch(ok, ?call(Worker2, rename, [{path, <<"/Space 1/d2">>}, {path, <<"/Space 1/d1">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/Space 1/d2">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d1">>}}}, ?call(Worker2, get, [{path, <<"/Space 1/d1">>}])),

    ?assertMatch(ok, ?call(Worker2, rename, [{path, <<"/Space 1/d1">>}, {name, <<"d4">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/Space 1/d1">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d4">>}}}, ?call(Worker2, get, [{path, <<"/Space 1/d4">>}])),

    %% Inter-space rename
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/Space 1/d4">>}])),
    ?assertMatch(ok, ?call(Worker2, rename, [{path, <<"/Space 1/d4">>}, {path, <<"/Space 2/d1">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/Space 1/d4">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d1">>}}}, ?call(Worker2, get, [{path, <<"/Space 2/d1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/f1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/f2">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/f3">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/f4">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/dd1/f1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/dd1/f2">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/dd2/f1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/Space 2/d1/dd2/f2">>}])),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [model_file_meta_test_base]} | Config].

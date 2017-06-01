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

    % create file tree
    {ok, Space1DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker2, file_meta, create, [{path, <<"/">>}, #file_meta{name = <<"Space 1">>, is_scope = true}])),
    {ok, Space2DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker2, file_meta, create, [{path, <<"/">>}, #file_meta{name = <<"Space 2">>, is_scope = true}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1">>}, #file_meta{name = <<"d1">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f1">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f2">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f3">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"f4">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"dd1">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1">>}, #file_meta{name = <<"dd2">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1/dd1">>}, #file_meta{name = <<"f1">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1/dd1">>}, #file_meta{name = <<"f2">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1/dd2">>}, #file_meta{name = <<"f1">>}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{path, <<"/Space 1/d1/dd2">>}, #file_meta{name = <<"f2">>}])),

    % assert that scope is correct for each file in the tree
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/f1">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/f2">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/f3">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/f4">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/dd1/f1">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/dd1/f2">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/dd2/f1">>}])),
    ?assertMatch({ok, #document{key = Space1DirUuid}}, rpc:call(Worker2, file_meta, get_scope, [{path, <<"/Space 1/d1/dd2/f2">>}])),

    % rename tree root
    {ok, Space1DirDoc} = ?assertMatch({ok, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1">>}])),
    {ok, D1Doc} = ?assertMatch({ok, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d1">>}])),
    ?assertMatch(ok, rpc:call(Worker2, file_meta, rename, [D1Doc, Space1DirDoc, Space1DirDoc, <<"d2">>])),
    ?assertMatch({error, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d1">>}])),
    {ok, D2Doc} = ?assertMatch({ok, #document{value = #file_meta{name = <<"d2">>}}}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d2">>}])),

    % rename tree root again
    ?assertMatch(ok, rpc:call(Worker2, file_meta, rename, [D2Doc, Space1DirDoc, Space1DirDoc, <<"d3">>])),
    ?assertMatch({error, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d2">>}])),
    {ok, D3Doc} = ?assertMatch({ok, #document{value = #file_meta{name = <<"d3">>}}}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d3">>}])),
    ?assertMatch({ok, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d3/f1">>}])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [model_file_meta_test_base]} | Config].

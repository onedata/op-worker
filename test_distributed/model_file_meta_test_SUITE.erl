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
-export([all/0, init_per_suite/1, end_per_suite/1]).
%% tests
-export([basic_operations_test/1, rename_test/1, list_test/1]).
%% test_bases
-export([basic_operations_test_base/1]).

all() ->
    ?ALL([basic_operations_test, rename_test, list_test], [basic_operations_test]).

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
    RootUuid = <<>>,
    {ok, Space1DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker2, file_meta, create, [{uuid, RootUuid}, #document{value = #file_meta{name = <<"Space 1">>, is_scope = true}}])),
    {ok, _Space2DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker2, file_meta, create, [{uuid, RootUuid}, #document{value = #file_meta{name = <<"Space 2">>, is_scope = true}}])),
    {ok, D1DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Space1DirUuid}, #document{value = #file_meta{name = <<"d1">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f1">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f2">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f3">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f4">>}}])),
    {ok, Dd1DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"dd1">>}}])),
    {ok, Dd2DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"dd2">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dd1DirUuid}, #document{value = #file_meta{name = <<"f1">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dd1DirUuid}, #document{value = #file_meta{name = <<"f2">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dd2DirUuid}, #document{value = #file_meta{name = <<"f1">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dd2DirUuid}, #document{value = #file_meta{name = <<"f2">>}}])),

    % assert that scope is correct for each file in the tree
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/f1">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/f2">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/f3">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/f4">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/dd1/f1">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/dd1/f2">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/dd2/f1">>}])),
    ?assertEqual({ok, Space1DirUuid}, rpc:call(Worker2, file_meta, get_scope_id, [{path, <<"/Space 1/d1/dd2/f2">>}])),

    % rename tree root
    {ok, Space1DirDoc} = ?assertMatch({ok, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1">>}])),
    {ok, D1Doc} = ?assertMatch({ok, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d1">>}])),
    ?assertMatch(ok, rpc:call(Worker2, file_meta, rename, [D1Doc, Space1DirDoc, Space1DirDoc, <<"d2">>])),
    ?assertMatch({error, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d1">>}])),
    {ok, D2Doc} = ?assertMatch({ok, #document{value = #file_meta{name = <<"d2">>}}}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d2">>}])),

    % rename tree root again
    ?assertMatch(ok, rpc:call(Worker2, file_meta, rename, [D2Doc, Space1DirDoc, Space1DirDoc, <<"d3">>])),
    ?assertMatch({error, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d2">>}])),
    {ok, _D3Doc} = ?assertMatch({ok, #document{value = #file_meta{name = <<"d3">>}}}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d3">>}])),
    ?assertMatch({ok, _}, rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/d3/f1">>}])).

list_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),

    % create file tree
    RootUuid = <<>>,
    {ok, Space1DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker2, file_meta, create, [{uuid, RootUuid}, #document{value = #file_meta{name = <<"Space list 1">>, is_scope = true}}])),
    {ok, D1DirUuid} = ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Space1DirUuid}, #document{value = #file_meta{name = <<"list_test_d1">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f1">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f2">>}}])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f3">>}}])),

    ?assertMatch({ok, [#child_link_uuid{name = <<"f1">>}, #child_link_uuid{name = <<"f2">>}, #child_link_uuid{name = <<"f3">>}], #{}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 0, 100])),

    ?assertMatch({ok, [#child_link_uuid{name = <<"f1">>}, #child_link_uuid{name = <<"f2">>}, #child_link_uuid{name = <<"f3">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 100])),

    {ok, _, #{token := T1}} = ?assertMatch({ok, [#child_link_uuid{name = <<"f1">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 1])),
    {ok, _, #{token := T2}} = ?assertMatch({ok, [#child_link_uuid{name = <<"f2">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 0, 1, T1])),
    ?assertMatch({ok, [#child_link_uuid{name = <<"f3">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children_by_key, [{path, <<"/Space list 1/list_test_d1">>}, <<>>, <<>>, 1, T2])),

    test_utils:set_env(Workers, ?CLUSTER_WORKER_APP_NAME, fold_cache_timeout, 0),
    {ok, _, #{token := T3}} = ?assertMatch({ok, [#child_link_uuid{name = <<"f1">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 1])),
    timer:sleep(timer:seconds(10)),
    ?assertMatch({ok, [#child_link_uuid{name = <<"f2">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 1, 1, T3])),

    {ok, _, #{token := T4, last_name := LN, last_tree := LT}} = ?assertMatch({ok, [#child_link_uuid{name = <<"f1">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 1])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f0">>}}])),
    timer:sleep(timer:seconds(10)),
    ?assertMatch({ok, [#child_link_uuid{name = <<"f2">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children_by_key, [{path, <<"/Space list 1/list_test_d1">>}, LN, LT, 1, T4])),

    {ok, _, #{token := T5, last_name := LN2, last_tree := LT2}} = ?assertMatch({ok, [#child_link_uuid{name = <<"f0">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 1])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f02">>}}])),
    timer:sleep(timer:seconds(10)),
    ?assertMatch({ok, [#child_link_uuid{name = <<"f02">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children_by_key, [{path, <<"/Space list 1/list_test_d1">>}, LN2, LT2, 1, T5])),

    {ok, _, #{token := T6, last_name := LN3, last_tree := LT3}} = ?assertMatch({ok, [#child_link_uuid{name = <<"f0">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space list 1/list_test_d1">>}, 1])),
    ?assertMatch({ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, D1DirUuid}, #document{value = #file_meta{name = <<"f01">>}}])),
    ?assertMatch({ok, [#child_link_uuid{name = <<"f02">>}], #{token := _}},
        rpc:call(Worker1, file_meta, list_children_by_key, [{path, <<"/Space list 1/list_test_d1">>}, LN3, LT3, 1, T6])),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        Workers = ?config(op_worker_nodes, NewConfig),
        test_utils:mock_new(Workers, [dbsync_utils]),
        test_utils:mock_expect(Workers, dbsync_utils, get_providers,
            fun(_) -> [] end),
        NewConfig,
        initializer:mock_provider_id(
            Workers, <<"provider1">>, <<"auth-macaroon">>, <<"identity-macaroon">>
        ),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [model_file_meta_test_base]} | Config].


end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers),
    test_utils:mock_unload(Workers, [dbsync_utils]).

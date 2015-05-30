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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-define(call(N, M, A), ?call(N, file_meta, M, A)).
-define(call(N, Mod, M, A), rpc:call(N, Mod, M, A)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([basic_operations_test/1, rename_test/1]).

-performance({test_cases, []}).
all() ->
    [basic_operations_test, rename_test].

%%%===================================================================
%%% Tests
%%%===================================================================

basic_operations_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    {A1, U1} = ?call(Worker1, create, [{path, <<"/">>}, #file_meta{name = <<"spaces">>, is_scope = true}]),
    {A2, U2} = ?call(Worker2, create, [{path, <<"/spaces">>}, #file_meta{name = <<"Space 1">>, is_scope = true}]),
    {A3, U3} = ?call(Worker1, create, [{path, <<"/spaces/Space 1">>}, #file_meta{name = <<"dir1">>}]),
    {A4, U4} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/dir1">>}, #file_meta{name = <<"file1">>}]),
    {A20, U20} = ?call(Worker1, create, [{path, <<"/spaces/Space 1">>}, #file_meta{name = <<"dir2">>}]),
    {A21, U21} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/dir2">>}, #file_meta{name = <<"file1">>}]),
    {A22, U22} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/dir2">>}, #file_meta{name = <<"file2">>}]),
    {A23, U23} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/dir2">>}, #file_meta{name = <<"file3">>}]),
    ?assertMatch({ok, _}, {A1, U1}),
    ?assertMatch({ok, _}, {A2, U2}),
    ?assertMatch({ok, _}, {A3, U3}),
    ?assertMatch({ok, _}, {A4, U4}),
    ?assertMatch({ok, _}, {A20, U20}),
    ?assertMatch({ok, _}, {A21, U21}),
    ?assertMatch({ok, _}, {A22, U22}),
    ?assertMatch({ok, _}, {A23, U23}),


    Bench1 =
        fun Loop(File) when File < 100 ->
            ?assertMatch({ok, _}, ?call(Worker1, create, [{path, <<"/spaces/Space 1/dir1">>}, #file_meta{name = integer_to_binary(1000 + File)}])),
            Loop(File + 1);
            Loop(_) ->
                ok
        end,

    S1 = now(),
    Bench1(0),
    E1 = now(),

    {A14, U14} = ?call(Worker1, get, [{path, <<"/">>}]),
    {A5, U5} = ?call(Worker1, get, [{path, <<"/spaces">>}]),
    {A6, U6} = ?call(Worker2, get, [{path, <<"/spaces/Space 1">>}]),
    {A7, U7} = ?call(Worker1, get, [{path, <<"/spaces/Space 1/dir1">>}]),
    {A8, U8} = ?call(Worker2, get, [{path, <<"/spaces/Space 1/dir1/file1">>}]),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"">>}}},        {A14, U14}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"spaces">>}}},  {A5, U5}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"Space 1">>}}}, {A6, U6}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"dir1">>}}},    {A7, U7}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"file1">>}}},   {A8, U8}),

    {A30, U30} = ?call(Worker1, gen_path, [{path, <<"/spaces/Space 1/dir2/file1">>}]),
    {A31, U31} = ?call(Worker2, gen_path, [{path, <<"/spaces/Space 1/dir2/file2">>}]),
    {A32, U32} = ?call(Worker2, gen_path, [{path, <<"/spaces/Space 1/dir2/file3">>}]),
    ?assertMatch({ok, <<"/spaces/Space 1/dir2/file1">>}, {A30, U30}),
    ?assertMatch({ok, <<"/spaces/Space 1/dir2/file2">>}, {A31, U31}),
    ?assertMatch({ok, <<"/spaces/Space 1/dir2/file3">>}, {A32, U32}),


    {A9, U9} =   ?call(Worker1, get_scope, [U14]),
    {A10, U10} = ?call(Worker1, get_scope, [U5]),
    {A11, U11} = ?call(Worker2, get_scope, [U6]),
    {A12, U12} = ?call(Worker1, get_scope, [U7]),
    {A13, U13} = ?call(Worker2, get_scope, [U8]),
    ?assertMatch({ok, #document{key = <<"">>}},         {A9, U9}),
    ?assertMatch({ok, #document{key = U1}},             {A10, U10}),
    ?assertMatch({ok, #document{key = U2}},             {A11, U11}),
    ?assertMatch({ok, #document{key = U2}},             {A12, U12}),
    ?assertMatch({ok, #document{key = U2}},             {A13, U13}),


    ?assertMatch({ok, [U2]}, ?call(Worker1, list_uuids, [{path, <<"/spaces">>}, 0, 10])),
    ?assertMatch({ok, []}, ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir2/file3">>}, 0, 10])),

    {A15, U15} = ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 0, 20]),
    {A16, U16} = ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 5, 10]),

    ?assertMatch({ok, _}, {A15, U15}),
    ?assertMatch({ok, _}, {A16, U16}),

    ?assertMatch(20, length(U15)),
    ?assertMatch(U16, lists:sublist(U15, 6, 10)),

    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file4">>}])),
    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 2/dir2/file1">>}])),
    ?assertMatch(true, ?call(Worker1, exists, [{path, <<"/">>}])),
    ?assertMatch(true, ?call(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file1">>}])),
    ?assertMatch({ok, [_, _, _]}, ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir2">>}, 0, 10])),

    ?assertMatch(ok, ?call(Worker1, delete, [{path, <<"/spaces/Space 1/dir2/file1">>}])),
    ?assertMatch(ok, ?call(Worker1, delete, [{uuid, U22}])),
    ?assertMatch({error, _}, ?call(Worker1, delete, [{path, <<"/spaces/Space 1/dir2/file4">>}])),

    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file1">>}])),
    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file2">>}])),

    ?assertMatch({ok, [U23]}, ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir2">>}, 0, 10])),


    [?call(Worker1, delete, [{uuid, D}]) || D <- [U1, U2, U3, U4, U20, U21, U22, U23]],

    ok.


rename_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    {A1, U1} = ?call(Worker1, create, [{path, <<"/">>}, #file_meta{name = <<"spaces">>, is_scope = true}]),
    {A2, U2} = ?call(Worker2, create, [{path, <<"/spaces">>}, #file_meta{name = <<"Space 1">>, is_scope = true}]),
    {A3, U3} = ?call(Worker2, create, [{path, <<"/spaces">>}, #file_meta{name = <<"Space 2">>, is_scope = true}]),
    {A4, U4} = ?call(Worker1, create, [{path, <<"/spaces/Space 1">>}, #file_meta{name = <<"d1">>}]),
    {A5, U5} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1">>}, #file_meta{name = <<"f1">>}]),
    {A20, U20} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1">>}, #file_meta{name = <<"f2">>}]),
    {A21, U21} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1">>}, #file_meta{name = <<"f3">>}]),
    {A22, U22} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1">>}, #file_meta{name = <<"f4">>}]),
    {A23, U23} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1">>}, #file_meta{name = <<"dd1">>}]),
    {A24, U24} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1">>}, #file_meta{name = <<"dd2">>}]),
    {A25, U25} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1/dd1">>}, #file_meta{name = <<"f1">>}]),
    {A26, U26} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1/dd1">>}, #file_meta{name = <<"f2">>}]),
    {A27, U27} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1/dd2">>}, #file_meta{name = <<"f1">>}]),
    {A28, U28} = ?call(Worker1, create, [{path, <<"/spaces/Space 1/d1/dd2">>}, #file_meta{name = <<"f2">>}]),
    ?assertMatch({ok, _}, {A1, U1}),
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

    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/f1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/f2">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/f3">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/f4">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/dd1/f1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/dd1/f2">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/dd2/f1">>}])),
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d1/dd2/f2">>}])),


    {A8, U8} = ?call(Worker2, get, [{path, <<"/spaces/Space 1/d1">>}]),
    ?assertMatch({ok, _}, {A8, U8}),

    ?assertMatch({ok, _}, ?call(Worker2, rename, [U8, {name, <<"d2">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d1">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d2">>}}}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d2">>}])),

    ?assertMatch({ok, _}, ?call(Worker2, rename, [{path, <<"/spaces/Space 1/d2">>}, {name, <<"d3">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d2">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d3">>}}}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d3">>}])),

    ?assertMatch({ok, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d3/f1">>}])),

    ?assertMatch({ok, _}, ?call(Worker2, rename, [{path, <<"/spaces/Space 1/d3">>}, {path, <<"/spaces/Space 1/d2">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d3">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d2">>}}}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d2">>}])),

    ?assertMatch({ok, _}, ?call(Worker2, rename, [{path, <<"/spaces/Space 1/d2">>}, {path, <<"/spaces/Space 1/d1">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d2">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d1">>}}}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d1">>}])),

    ?assertMatch({ok, _}, ?call(Worker2, rename, [{path, <<"/spaces/Space 1/d1">>}, {name, <<"d4">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d1">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d4">>}}}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d4">>}])),

    %% Inter-space rename
    ?assertMatch({ok, #document{key = U2}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 1/d4">>}])),
    ?assertMatch({ok, _}, ?call(Worker2, rename, [{path, <<"/spaces/Space 1/d4">>}, {path, <<"/spaces/Space 2/d1">>}])),
    ?assertMatch({error, _}, ?call(Worker2, get, [{path, <<"/spaces/Space 1/d4">>}])),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"d1">>}}}, ?call(Worker2, get, [{path, <<"/spaces/Space 2/d1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/f1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/f2">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/f3">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/f4">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/dd1/f1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/dd1/f2">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/dd2/f1">>}])),
    ?assertMatch({ok, #document{key = U3}}, ?call(Worker2, get_scope, [{path, <<"/spaces/Space 2/d1/dd2/f2">>}])),

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
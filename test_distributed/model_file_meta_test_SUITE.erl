%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
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

-define(call_with_time(N, M, A), ?call_with_time(N, file_meta, M, A)).
-define(call_with_time(N, Mod, M, A), rpc:call(N, ?MODULE, exec_and_check_time, [Mod, M, A])).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, exec_and_check_time/3]).
-export([basic_operations_test/1]).

-performance({test_cases, []}).
all() ->
    [basic_operations_test].

%%%===================================================================
%%% Tests
%%%===================================================================

basic_operations_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    {{A1, U1}, CreateLevel0} = ?call_with_time(Worker1, create, [{path, <<"/">>}, #file_meta{name = <<"spaces">>, is_scope = true}]),
    {{A2, U2}, CreateLevel1} = ?call_with_time(Worker2, create, [{path, <<"/spaces">>}, #file_meta{name = <<"Space 1">>, is_scope = true}]),
    {{A3, U3}, CreateLevel2} = ?call_with_time(Worker1, create, [{path, <<"/spaces/Space 1">>}, #file_meta{name = <<"dir1">>}]),
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

    {Level20Path, CreateLevel20} = create_deep_tree(Worker2),

    BigDir =
        fun Loop(File) when File < 99 ->
            ?assertMatch({ok, _}, ?call(Worker1, create, [{path, <<"/spaces/Space 1/dir1">>}, #file_meta{name = integer_to_binary(1000 + File)}])),
            Loop(File + 1);
            Loop(_) ->
                ok
        end,
    BigDir(0),

    {{A14, U14}, GetLevel0} = ?call_with_time(Worker1, get, [{path, <<"/">>}]),
    {{A5, U5}, GetLevel1} = ?call_with_time(Worker1, get, [{path, <<"/spaces">>}]),
    {{A6, U6}, GetLevel2} = ?call_with_time(Worker2, get, [{path, <<"/spaces/Space 1">>}]),
    {A7, U7} = ?call(Worker1, get, [{path, <<"/spaces/Space 1/dir1">>}]),
    {A8, U8} = ?call(Worker2, get, [{path, <<"/spaces/Space 1/dir1/file1">>}]),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"">>}}},        {A14, U14}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"spaces">>}}},  {A5, U5}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"Space 1">>}}}, {A6, U6}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"dir1">>}}},    {A7, U7}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"file1">>}}},   {A8, U8}),

    {{AL20, UL20}, GetLevel20} = ?call_with_time(Worker1, get, [{path, Level20Path}]),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"1">>}}},   {AL20, UL20}),

%%     @RS - jaki sens ma pobieranie sciezki po sciezce? Zmien na inny klucz
    {{A30, U30}, GetPathLevel1} = ?call_with_time(Worker1, gen_path, [{path, <<"/spaces">>}]),
    {{A31, U31}, GetPathLevel2} = ?call_with_time(Worker2, gen_path, [{path, <<"/spaces/Space 1">>}]),
    {{A32, U32}, GetPathLevel3} = ?call_with_time(Worker2, gen_path, [{path, <<"/spaces/Space 1/dir2">>}]),
    ?assertMatch({ok, <<"/spaces">>}, {A30, U30}),
    ?assertMatch({ok, <<"/spaces/Space 1">>}, {A31, U31}),
    ?assertMatch({ok, <<"/spaces/Space 1/dir2">>}, {A32, U32}),

    {{AL20_2, UL20_2}, GetPathLevel20} = ?call_with_time(Worker2, gen_path, [{path, Level20Path}]),
    ?assertMatch({ok, Level20Path}, {AL20_2, UL20_2}),

    {{A9, U9}, GetScopeLevel0} =   ?call_with_time(Worker1, get_scope, [U14]),
    {{A10, U10}, GetScopeLevel1} = ?call_with_time(Worker1, get_scope, [U5]),
    {{A11, U11}, GetScopeLevel2} = ?call_with_time(Worker2, get_scope, [U6]),
    {A12, U12} = ?call(Worker1, get_scope, [U7]),
    {A13, U13} = ?call(Worker2, get_scope, [U8]),
    ?assertMatch({ok, #document{key = <<"">>}},         {A9, U9}),
    ?assertMatch({ok, #document{key = U1}},             {A10, U10}),
    ?assertMatch({ok, #document{key = U2}},             {A11, U11}),
    ?assertMatch({ok, #document{key = U2}},             {A12, U12}),
    ?assertMatch({ok, #document{key = U2}},             {A13, U13}),

    {{AL20_3, UL20_3}, GetScopeLevel20} = ?call_with_time(Worker2, get_scope, [UL20]),
    ?assertMatch({ok, #document{key = U2}},             {AL20_3, UL20_3}),

    ?assertMatch({ok, [U2]}, ?call(Worker1, list_uuids, [{path, <<"/spaces">>}, 0, 10])),

    {{A15, U15}, ListUuids20_100} = ?call_with_time(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 0, 20]),
    {{A15_2, U15_2}, ListUuids100_100} = ?call_with_time(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 0, 100]),
    {{A15_3, U15_3}, ListUuids1000_100} = ?call_with_time(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 0, 1000]),
    {{A15_4, U15_4}, ListUuids1_100} = ?call_with_time(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 0, 1]),
    {{A16, U16}, ListUuids50_60_100} = ?call_with_time(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir1">>}, 50, 10]),
    {{AL20_4, UL20_4}, ListUuidsLevel20} = ?call_with_time(Worker1, list_uuids, [{path, Level20Path}, 0, 1]),

    ?assertMatch({ok, _}, {A15, U15}),
    ?assertMatch({ok, _}, {A15_2, U15_2}),
    ?assertMatch({ok, _}, {A15_3, U15_3}),
    ?assertMatch({ok, _}, {A15_4, U15_4}),
    ?assertMatch({ok, _}, {A16, U16}),
    ?assertMatch({ok, _}, {AL20_4, UL20_4}),

    ?assertMatch(20, length(U15)),
    ?assertMatch(100, length(U15_2)),
    ?assertMatch(100, length(U15_3)),
    ?assertMatch(1, length(U15_4)),
    ct:print("~p~n~p", [U16, lists:sublist(U15_2, 51, 10)]),
    ?assertMatch(U16, lists:sublist(U15_2, 51, 10)),
    ?assertMatch(0, length(UL20_4)),

    {AE1, ExistsFalseLevel4} = ?call_with_time(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file4">>}]),
    ?assertMatch(false, AE1),
    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 2/dir2/file1">>}])),
    {AE2, ExistsTrueLevel1} = ?call_with_time(Worker1, exists, [{path, <<"/">>}]),
    ?assertMatch(true, AE2),
    {AE3, ExistsTrueLevel4} = ?call_with_time(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file1">>}]),
    ?assertMatch(true, AE3),
    {AE4, ExistsTrueLevel20} = ?call_with_time(Worker1, exists, [{path, Level20Path}]),
    ?assertMatch(true, AE4),
    ?assertMatch({ok, [_, _, _]}, ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir2">>}, 0, 10])),

    {AD1, DeleteOkPathLevel4} = ?call_with_time(Worker1, delete, [{path, <<"/spaces/Space 1/dir2/file1">>}]),
    ?assertMatch(ok, AD1),
    {AD2, DeleteOkUuidLevel4} = ?call_with_time(Worker1, delete, [{uuid, U22}]),
    ?assertMatch(ok, AD2),
    {AD3, DeleteErrorPathLevel4} = ?call_with_time(Worker1, delete, [{path, <<"/spaces/Space 1/dir2/file4">>}]),
    ?assertMatch({error, _}, AD3),
    {AD4, DeleteOkPathLevel20} = ?call_with_time(Worker1, delete, [{path, Level20Path}]),
    ?assertMatch(ok, AD4),

    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file1">>}])),
    ?assertMatch(false, ?call(Worker1, exists, [{path, <<"/spaces/Space 1/dir2/file2">>}])),

    ?assertMatch({ok, [U23]}, ?call(Worker1, list_uuids, [{path, <<"/spaces/Space 1/dir2">>}, 0, 10])),

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

exec_and_check_time(Mod, M, A) ->
    BeforeProcessing = os:timestamp(),
    Ans = erlang:apply(Mod, M, A),
    AfterProcessing = os:timestamp(),
    {Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}.

create_deep_tree(Worker) ->
    create_deep_tree(Worker, "/spaces/Space 1", 18).

create_deep_tree(Worker, Prefix, 1) ->
    {{A, U}, Time} = ?call_with_time(Worker, create, [{path, list_to_binary(Prefix)}, #file_meta{name = <<"1">>}]),
    ?assertMatch({ok, _}, {A, U}),
    {list_to_binary(Prefix++"/1"), Time};

create_deep_tree(Worker, Prefix, Num) ->
    StringNum = integer_to_list(Num),
    {A, U} = ?call(Worker, create, [{path, list_to_binary(Prefix)}, #file_meta{name = list_to_binary(StringNum)}]),
    ?assertMatch({ok, _}, {A, U}),
    create_deep_tree(Worker, Prefix++"/"++StringNum, Num-1).

%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helper for file_meta model test.
%%% @end
%%%-------------------------------------------------------------------
-module(model_file_meta_test_base).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(call_with_time(N, F, A), ?call_with_time(N, file_meta, F, A)).
-define(call_with_time(N, M, F, A), rpc:call(N, ?MODULE, exec_and_check_time, [M, F, A])).

-export([basic_operations_test_core/2, exec_and_check_time/3]).

%%%===================================================================
%%% Functions cores (to be reused in standard and stress tests)
%%%===================================================================

basic_operations_test_core(Config, LastLevel) ->
    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),

    % Clear for stress test (if previous run crashed)
    BigDirDel =
        fun Loop(File) when File < 99 ->
            rpc:call(Worker1, file_meta, delete, [{path, list_to_binary("/Space 1/dir1/" ++ integer_to_list(1000 + File))}]),
            Loop(File + 1);
            Loop(_) ->
                ok
        end,
    BigDirDel(0),

    delete_deep_tree(Worker2, LastLevel),
    [rpc:call(Worker1, file_meta, delete, [{path, D}]) || D <- ["/Space 1", "/Space 1/dir1", "/Space 1/dir1/file1",
        "/Space 1/dir2", "/Space 1/dir2/file1", "/Space 1/dir2/file2", "/Space 1/dir2/file3"]],

    % Test
    RootUuid = <<>>,
    {{ok, Space1Uuid}, CreateLevel1} = ?assertMatch(
        {{ok, _}, _},
        ?call_with_time(Worker2, create, [{uuid, RootUuid}, #document{value = #file_meta{name = <<"Space 1">>, is_scope = true}}])
    ),
    {{ok, Dir1Uuid}, CreateLevel2} = ?assertMatch(
        {{ok, _}, _},
        ?call_with_time(Worker1, create, [{uuid, Space1Uuid}, #document{value = #file_meta{name = <<"dir1">>}}])
    ),
    {ok, Dir1File1Uuid} = ?assertMatch(
        {ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dir1Uuid}, #document{value = #file_meta{name = <<"file1">>}}])
    ),
    {ok, Dir2Uuid} = ?assertMatch(
        {ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Space1Uuid}, #document{value = #file_meta{name = <<"dir2">>}}])
    ),
    {ok, Dir2File1Uuid} = ?assertMatch(
        {ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dir2Uuid}, #document{value = #file_meta{name = <<"file1">>}}])
    ),
    {ok, Dir2File2Uuid} = ?assertMatch(
        {ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dir2Uuid}, #document{value = #file_meta{name = <<"file2">>}}])
    ),
    {ok, Dir2File3Uuid} = ?assertMatch(
        {ok, _},
        rpc:call(Worker1, file_meta, create, [{uuid, Dir2Uuid}, #document{value = #file_meta{name = <<"file3">>}}])
    ),

    {Level20Uuid, Level20Path, CreateLevel20} = create_deep_tree(Worker2, <<"/Space 1">>, Space1Uuid, LastLevel - 2),

    BigDir =
        fun Loop(File) when File < 99 ->
            ?assertMatch({ok, _}, rpc:call(Worker1, file_meta, create, [{uuid, Dir1Uuid},
                #document{value = #file_meta{name = integer_to_binary(1000 + File)}}])),
            Loop(File + 1);
            Loop(_) ->
                ok
        end,
    BigDir(0),

    {{A14, U14}, GetLevel0} = ?call_with_time(Worker1, get, [{path, <<"/">>}]),
    {{A6, U6}, GetLevel2} = ?call_with_time(Worker2, get, [{path, <<"/Space 1">>}]),
    {A7, U7} = rpc:call(Worker1, file_meta, get, [{path, <<"/Space 1/dir1">>}]),
    {A8, U8} = rpc:call(Worker2, file_meta, get, [{path, <<"/Space 1/dir1/file1">>}]),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"">>}}}, {A14, U14}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"Space 1">>}}}, {A6, U6}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"dir1">>}}}, {A7, U7}),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"file1">>}}}, {A8, U8}),

    {{AL20, UL20}, GetLevel20} = ?call_with_time(Worker1, get, [{uuid, Level20Uuid}]),
    ?assertMatch({ok, #document{value = #file_meta{name = <<"1">>}}}, {AL20, UL20}),
    #document{key = Level20Key} = UL20,

    space_info_mock(Workers, <<"Space 1">>),
    {U30, GenPathLevel1} = ?call_with_time(Worker1, fslogic_uuid, uuid_to_path, [?ROOT_SESS_ID, Dir2File1Uuid]),
    {U31, GenPathLevel2} = ?call_with_time(Worker2, fslogic_uuid, uuid_to_path, [?ROOT_SESS_ID, Dir2File2Uuid]),
    {U32, GenPathLevel3} = ?call_with_time(Worker2, fslogic_uuid, uuid_to_path, [?ROOT_SESS_ID, Dir2File3Uuid]),
    ?assertMatch(<<"/Space 1/dir2/file1">>, U30),
    ?assertMatch(<<"/Space 1/dir2/file2">>, U31),
    ?assertMatch(<<"/Space 1/dir2/file3">>, U32),

    {{A41, U41}, ResolveLevel2} = ?call_with_time(Worker1, resolve_path, [<<"/Space 1/">>]),
    {{A42, U42}, ResolveLevel3} = ?call_with_time(Worker1, resolve_path, [<<"/Space 1/dir2">>]),
    {{A43, U43}, ResolveLevel20} = ?call_with_time(Worker1, resolve_path, [Level20Path]),
    ?assertMatch({ok, {#document{key = Space1Uuid}, _}}, {A41, U41}),
    ?assertMatch({ok, {#document{key = Dir2Uuid}, _}}, {A42, U42}),
    ?assertMatch({ok, {#document{key = Level20Key}, _}}, {A43, U43}),


    {UL20_2, GenPathLevel20} = ?call_with_time(Worker2, fslogic_uuid, uuid_to_path, [?ROOT_SESS_ID, Level20Key]),
    ?assertMatch(Level20Path, UL20_2),
    test_utils:mock_unload(Workers, [od_space, fslogic_uuid]),

    {{A9, U9}, GetScopeLevel0} = ?call_with_time(Worker1, get_scope, [U14]),
    {{A11, U11}, GetScopeLevel2} = ?call_with_time(Worker2, get_scope, [U6]),
    {A12, U12} = rpc:call(Worker1, file_meta, get_scope, [U7]),
    {A13, U13} = rpc:call(Worker2, file_meta, get_scope, [U8]),
    ?assertMatch({ok, #document{key = <<"">>}}, {A9, U9}),
    ?assertMatch({ok, #document{key = Space1Uuid}}, {A11, U11}),
    ?assertMatch({ok, #document{key = Space1Uuid}}, {A12, U12}),
    ?assertMatch({ok, #document{key = Space1Uuid}}, {A13, U13}),

    {{AL20_3, UL20_3}, GetScopeLevel20} = ?call_with_time(Worker2, get_scope, [UL20]),
    ?assertMatch({ok, #document{key = Space1Uuid}}, {AL20_3, UL20_3}),

    ?assertMatch({ok, [#child_link_uuid{uuid = Space1Uuid}]}, rpc:call(Worker1, file_meta, list_children, [{path, <<"/">>}, 0, 10])),
    ?assertMatch({ok, []}, rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space 1/dir2/file3">>}, 0, 10])),

    {{A15, U15}, ListUuids20_100} = ?call_with_time(Worker1, list_children, [{path, <<"/Space 1/dir1">>}, 0, 20]),
    {{A15_2, U15_2}, ListUuids100_100} = ?call_with_time(Worker1, list_children, [{path, <<"/Space 1/dir1">>}, 0, 100]),
    {{A15_3, U15_3}, ListUuids1000_100} = ?call_with_time(Worker1, list_children, [{path, <<"/Space 1/dir1">>}, 0, 1000]),
    {{A15_4, U15_4}, ListUuids1_100} = ?call_with_time(Worker1, list_children, [{path, <<"/Space 1/dir1">>}, 0, 1]),
    {{A16, U16}, ListUuids50_60_100} = ?call_with_time(Worker1, list_children, [{path, <<"/Space 1/dir1">>}, 50, 10]),
    {{AL20_4, UL20_4}, ListUuidsLevel20} = ?call_with_time(Worker1, list_children, [{path, Level20Path}, 0, 1]),

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
    ?assertMatch(U16, lists:sublist(U15_2, 51, 10)),
    ?assertMatch(0, length(UL20_4)),

    {AE1, ExistsFalseLevel4} = ?call_with_time(Worker1, exists, [{path, <<"/Space 1/dir2/file4">>}]),
    ?assertMatch(false, AE1),
    ?assertMatch(false, rpc:call(Worker1, file_meta, exists, [{path, <<"/Space 2/dir2/file1">>}])),
    {AE2, ExistsTrueLevel1} = ?call_with_time(Worker1, exists, [{path, <<"/">>}]),
    ?assertMatch(true, AE2),
    {AE3, ExistsTrueLevel4} = ?call_with_time(Worker1, exists, [{path, <<"/Space 1/dir2/file1">>}]),
    ?assertMatch(true, AE3),
    {AE4, ExistsTrueLevel20} = ?call_with_time(Worker1, exists, [{path, Level20Path}]),
    ?assertMatch(true, AE4),
    ?assertMatch({ok, [_, _, _]}, rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space 1/dir2">>}, 0, 10])),

    {AD1, DeleteOkPathLevel4} = ?call_with_time(Worker1, delete, [{path, <<"/Space 1/dir2/file1">>}]),
    ?assertMatch(ok, AD1),
    {AD2, DeleteOkUuidLevel4} = ?call_with_time(Worker1, delete, [{uuid, Dir2File2Uuid}]),
    ?assertMatch(ok, AD2),
    {AD3, DeleteErrorPathLevel4} = ?call_with_time(Worker1, delete, [{path, <<"/Space 1/dir2/file4">>}]),
    ?assertMatch({error, _}, AD3),
    {AD4, DeleteOkPathLevel20} = ?call_with_time(Worker1, delete, [{path, Level20Path}]),
    ?assertMatch(ok, AD4),

    ?assertMatch(false, rpc:call(Worker1, file_meta, exists, [{path, <<"/Space 1/dir2/file1">>}])),
    ?assertMatch(false, rpc:call(Worker1, file_meta, exists, [{path, <<"/Space 1/dir2/file2">>}])),

    ?assertMatch({ok, [#child_link_uuid{uuid = Dir2File3Uuid}]}, rpc:call(Worker1, file_meta, list_children, [{path, <<"/Space 1/dir2">>}, 0, 10])),

    BigDirDel(0),

    delete_deep_tree(Worker2, LastLevel),
    [rpc:call(Worker1, file_meta, delete, [{uuid, D}]) || D <- [Space1Uuid, Dir1Uuid, Dir1File1Uuid, Dir2Uuid, Dir2File1Uuid, Dir2File2Uuid, Dir2File3Uuid]],

    [
        #parameter{name = create_level_1, value = CreateLevel1, unit = "us",
            description = "Time of create opertion at level 1 (1 dir above file)"},
        #parameter{name = create_level_2, value = CreateLevel2, unit = "us",
            description = "Time of create opertion at level 2 (2 dirs above file)"},
        #parameter{name = create_level_20, value = CreateLevel20, unit = "us",
            description = "Time of create opertion at level 20 (20 dirs above file)"},
        #parameter{name = get_level_0, value = GetLevel0, unit = "us",
            description = "Time of get opertion at root level"},
        #parameter{name = get_level_1, value = GetLevel2, unit = "us",
            description = "Time of get opertion at level 1 (1 dirs above file)"},
        #parameter{name = get_level_20, value = GetLevel20, unit = "us",
            description = "Time of get opertion at level 20 (20 dirs above file)"},
        #parameter{name = gen_path_level_1, value = GenPathLevel1, unit = "us",
            description = "Time of gen path opertion at level 1 (1 dir above file)"},
        #parameter{name = gen_path_level_2, value = GenPathLevel2, unit = "us",
            description = "Time of gen path opertion at level 2 (2 dirs above file)"},
        #parameter{name = gen_path_level_3, value = GenPathLevel3, unit = "us",
            description = "Time of gen path opertion at level 3 (3 dirs above file)"},
        #parameter{name = genv_pathv_level_20, value = GenPathLevel20, unit = "us",
            description = "Time of gen path opertion at level 20 (20 dirs above file)"},
        #parameter{name = resolve_path_level_1, value = ResolveLevel2, unit = "us",
            description = "Time of resolve path opertion at level 1 (1 dirs above file)"},
        #parameter{name = resolve_path_level_2, value = ResolveLevel3, unit = "us",
            description = "Time of resolve path opertion at level 2 (2 dirs above file)"},
        #parameter{name = resolve_path_level_20, value = ResolveLevel20, unit = "us",
            description = "Time of resolve path opertion at level 20 (20 dirs above file)"},
        #parameter{name = get_scope_level_0, value = GetScopeLevel0, unit = "us",
            description = "Time of get scope opertion at root level"},
        #parameter{name = get_scope_level_1, value = GetScopeLevel2, unit = "us",
            description = "Time of get scope opertion at level 1 (1 dirs above file)"},
        #parameter{name = get_scope_level_20, value = GetScopeLevel20, unit = "us",
            description = "Time of get scope opertion at level 20 (20 dirs above file)"},
        #parameter{name = list_uuids_20_100, value = ListUuids20_100, unit = "us",
            description = "Time of listing 20 uuids in 100file dir at level 4 (4 dirs above file)"},
        #parameter{name = list_uuids_100_100, value = ListUuids100_100, unit = "us",
            description = "Time of listing 100 uuids in 100file dir at level 4 (4 dirs above file)"},
        #parameter{name = list_uuids_1000_100, value = ListUuids1000_100, unit = "us",
            description = "Time of listing 1000 uuids in 100file dir at level 4 (4 dirs above file)"},
        #parameter{name = list_uuids_1_100, value = ListUuids1_100, unit = "us",
            description = "Time of listing 1 uuid in 100file dir at level 4 (4 dirs above file)"},
        #parameter{name = list_uuids_50_60_100, value = ListUuids50_60_100, unit = "us",
            description = "Time of listing uuids from 50 to 60 (100 uuids) in 100file dir at level 4 (4 dirs above file)"},
        #parameter{name = list_uuids_level20, value = ListUuidsLevel20, unit = "us",
            description = "Time of listing 1 uuid in dir with no children at level 20 (20 dirs above file)"},
        #parameter{name = exists_false_level4, value = ExistsFalseLevel4, unit = "us",
            description = "Time of exists opertion at level 4 (4 dirs above file) when file does not exist"},
        #parameter{name = exists_true_level1, value = ExistsTrueLevel1, unit = "us",
            description = "Time of exists opertion at root level when file exists"},
        #parameter{name = exists_true_level4, value = ExistsTrueLevel4, unit = "us",
            description = "Time of exists opertion at level 4 (4 dirs above file) when file exists"},
        #parameter{name = exists_true_level20, value = ExistsTrueLevel20, unit = "us",
            description = "Time of exists opertion at level 4 (4 dirs above file) when file exists"},
        #parameter{name = delete_ok_path_level4, value = DeleteOkPathLevel4, unit = "us",
            description = "Time of delete by path opertion at level 4 (4 dirs above file) when file exists"},
        #parameter{name = delete_ok_uuid_level4, value = DeleteOkUuidLevel4, unit = "us",
            description = "Time of delete by uuid opertion at level 4 (4 dirs above file) when file exists"},
        #parameter{name = delete_error_path_level4, value = DeleteErrorPathLevel4, unit = "us",
            description = "Time of delete by path opertion at level 4 (4 dirs above file) when file does not exist"},
        #parameter{name = delete_ok_path_level20, value = DeleteOkPathLevel20, unit = "us",
            description = "Time of delete by path opertion at level 20 (20 dirs above file) when file exists"}
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

space_info_mock(Workers, SpaceName) ->
    test_utils:mock_new(Workers, [od_space, fslogic_uuid]),
    test_utils:mock_expect(Workers, od_space, get, fun(_, _) ->
        {ok, #document{value = #od_space{name = SpaceName}}}
    end),
    test_utils:mock_expect(Workers, fslogic_uuid, space_dir_uuid_to_spaceid, fun(_) ->
        SpaceName %% Just return space name since space info mock ignores space id anyway
    end).

exec_and_check_time(Mod, M, A) ->
    BeforeProcessing = os:timestamp(),
    Ans = erlang:apply(Mod, M, A),
    AfterProcessing = os:timestamp(),
    {Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}.

create_deep_tree(Worker, ParentPath, ParentUuid, 1) ->
    {{ok, FileUuid}, Time} = ?assertMatch(
        {{ok, _}, _},
        ?call_with_time(Worker, create, [{uuid, ParentUuid}, #document{value = #file_meta{name = <<"1">>}}])
    ),
    {FileUuid, <<ParentPath/binary, "/", "1">>, Time};
create_deep_tree(Worker, ParentPath, ParentUuid, Num) ->
    BinaryNum = integer_to_binary(Num),

    {ok, FileUuid} = ?assertMatch(
        {ok, _},
        rpc:call(Worker, file_meta, create, [{uuid, ParentUuid}, #document{value = #file_meta{name = BinaryNum}}])
    ),
    create_deep_tree(Worker, <<ParentPath/binary, "/", BinaryNum/binary>>, FileUuid, Num - 1).

delete_deep_tree(Worker, Level) ->
    delete_deep_tree(Worker, "/Space 1", Level - 2).

delete_deep_tree(Worker, Prefix, 1) ->
    rpc:call(Worker, file_meta, delete, [{path, list_to_binary(Prefix)}]);

delete_deep_tree(Worker, Prefix, Num) ->
    StringNum = integer_to_list(Num),
    delete_deep_tree(Worker, Prefix ++ "/" ++ StringNum, Num - 1),
    rpc:call(Worker, file_meta, delete, [{path, list_to_binary(Prefix)}]).
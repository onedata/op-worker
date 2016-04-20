%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of sequencer manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_test_SUITE).
-author("Krzysztof Trzepla").
-author("Rafal Slota").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    fslogic_get_file_attr_test/1,
    fslogic_mkdir_and_rmdir_test/1,
    fslogic_read_dir_test/1,
    chmod_test/1,
    simple_rename_test/1,
    update_times_test/1,
    default_permissions_test/1
]).

all() ->
    ?ALL([
        fslogic_get_file_attr_test,
        fslogic_mkdir_and_rmdir_test,
        fslogic_read_dir_test,
        chmod_test,
        simple_rename_test,
        update_times_test,
        default_permissions_test
    ]).

-define(TIMEOUT, timer:seconds(5)).

-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}])).

%%%===================================================================
%%% Test functions
%%%===================================================================

fslogic_get_file_attr_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    lists:foreach(fun({SessId, Name, Mode, UID, Path}) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_attr{
                name = Name, type = ?DIRECTORY_TYPE, mode = Mode,
                uid = UID
            }
        }, ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}))
    end, [
        {SessId1, UserId1, 8#1770, 0, <<"/">>},
        {SessId2, UserId2, 8#1770, 0, <<"/">>},
        {SessId1, <<"spaces">>, 8#1755, 0, <<"/spaces">>},
        {SessId2, <<"spaces">>, 8#1755, 0, <<"/spaces">>},
        {SessId1, <<"space_id1">>, 8#1770, 0, <<"/spaces/space_name1">>},
        {SessId2, <<"space_id2">>, 8#1770, 0, <<"/spaces/space_name2">>},
        {SessId1, <<"space_id3">>, 8#1770, 0, <<"/spaces/space_name3">>},
        {SessId2, <<"space_id4">>, 8#1770, 0, <<"/spaces/space_name4">>}
    ]),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, ?req(Worker,
        SessId1, #get_file_attr{entry = {path, <<"/spaces/space_name1/t1_dir">>}}
    )).

fslogic_mkdir_and_rmdir_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    RootFileAttr1 = ?req(Worker, SessId1, #get_file_attr{entry = {path, <<"/">>}}),
    RootFileAttr2 = ?req(Worker, SessId2, #get_file_attr{entry = {path, <<"/">>}}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr2),

    #fuse_response{fuse_response = #file_attr{uuid = RootUUID1}} = RootFileAttr1,
    #fuse_response{fuse_response = #file_attr{uuid = RootUUID2}} = RootFileAttr2,

    MakeTree = fun(Leaf, {SessId, DefaultSpaceName, Path, ParentUUID, FileUUIDs}) ->
        NewPath = <<Path/binary, "/", Leaf/binary>>,
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(Worker, SessId,
            #create_dir{parent_uuid = ParentUUID, name = Leaf, mode = 8#755}
        )),

        FileAttr = ?req(Worker, SessId, #get_file_attr{entry = {path, NewPath}}),
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
        ?assertEqual(FileAttr, ?req(Worker, SessId, #get_file_attr{entry = {path, <<"/spaces/", DefaultSpaceName/binary, NewPath/binary>>}})),
        #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,

        {SessId, DefaultSpaceName, NewPath, FileUUID, [FileUUID | FileUUIDs]}
    end,

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(Worker, SessId1,
        #create_dir{parent_uuid = RootUUID1, name = <<"t2_double">>, mode = 8#755}
    )),
    ?assertMatch(#fuse_response{status = #status{code = ?EEXIST}}, ?req(Worker, SessId1,
        #create_dir{parent_uuid = RootUUID1, name = <<"t2_double">>, mode = 8#755}
    )),


    {_, _, _, _, UUIDs1} = lists:foldl(MakeTree, {SessId1, <<"space_name1">>, <<>>, RootUUID1, []},
        [<<"t2_dir1">>, <<"t2_dir2">>, <<"t2_dir3">>]),
    {_, _, _, _, UUIDs2} = lists:foldl(MakeTree, {SessId2, <<"space_name2">>, <<>>, RootUUID2, []},
        [<<"t2_dir4">>, <<"t2_dir5">>, <<"t2_dir6">>]),

    TestPath1 = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME, <<"space_name2">>,
        <<"t2_dir4">>, <<"t2_dir5">>, <<"t2_dir6">>]),
    FileAttr = ?req(Worker, SessId1, #get_file_attr{entry = {path, TestPath1}}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
    ?assertEqual(FileAttr, ?req(Worker, SessId2, #get_file_attr{entry = {path, TestPath1}})),

    lists:foreach(fun(UUID) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            ?req(Worker, SessId1, #delete_file{uuid = UUID}))
    end, lists:reverse(tl(UUIDs1))),

    lists:foreach(fun(UUID) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            ?req(Worker, SessId2, #delete_file{uuid = UUID}))
    end, lists:reverse(tl(UUIDs2))).

fslogic_read_dir_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},
    {SessId3, _UserId3} = {?config({session_id, 3}, Config), ?config({user_id, 3}, Config)},
    {SessId4, _UserId4} = {?config({session_id, 4}, Config), ?config({user_id, 4}, Config)},

    ValidateReadDir = fun({SessId, Path, NameList}) ->
        FileAttr = ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}),
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
        #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,

        ExpectedNames = lists:sort(NameList),

        lists:foreach( %% Size
            fun(Size) ->
                lists:foreach( %% Offset step
                    fun(OffsetStep) ->
                        {_, Names} = lists:foldl( %% foreach Offset
                            fun(_, {Offset, CurrentChildren}) ->
                                Response = ?req(Worker, SessId, #get_file_children{uuid = FileUUID, offset = Offset, size = Size}),

                                ?assertMatch(#fuse_response{status = #status{code = ?OK}}, Response),
                                #fuse_response{fuse_response = #file_children{child_links = Links}} = Response,

                                RespNames = lists:map(
                                    fun(#child_link{uuid = _, name = Name}) ->
                                        Name
                                    end, Links),

                                ?assertEqual(min(max(0, length(ExpectedNames) - Offset), Size), length(RespNames)),

                                {Offset + OffsetStep, lists:usort(RespNames ++ CurrentChildren)}
                            end, {0, []}, lists:seq(1, 2 * round(length(ExpectedNames) / OffsetStep))),

                        ?assertMatch(ExpectedNames, lists:sort(lists:flatten(Names)))
                    end, lists:seq(1, Size))

            end, lists:seq(1, length(ExpectedNames) + 1))
    end,

    lists:foreach(ValidateReadDir, [
        {SessId1, <<"/spaces">>, [<<"space_name1">>, <<"space_name2">>, <<"space_name3">>, <<"space_name4">>]},
        {SessId2, <<"/spaces">>, [<<"space_name2">>, <<"space_name3">>, <<"space_name4">>]},
        {SessId3, <<"/spaces">>, [<<"space_name3">>, <<"space_name4">>]},
        {SessId4, <<"/spaces">>, [<<"space_name4">>]},
        {SessId1, <<"/">>, [?SPACES_BASE_DIR_NAME]},
        {SessId2, <<"/">>, [?SPACES_BASE_DIR_NAME]},
        {SessId3, <<"/">>, [?SPACES_BASE_DIR_NAME]},
        {SessId4, <<"/">>, [?SPACES_BASE_DIR_NAME]}
    ]),

    RootUUID1 = get_uuid_privileged(Worker, SessId1, <<"/">>),
    RootUUID2 = get_uuid_privileged(Worker, SessId2, <<"/">>),

    lists:foreach(fun({SessId, RootUUID, Dirs}) ->
        lists:foreach(fun(Name) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(
                Worker, SessId, #create_dir{parent_uuid = RootUUID, name = Name, mode = 8#755}
            ))
        end, Dirs)
    end, [
        {SessId1, RootUUID1, [<<"t3_dir11">>, <<"t3_dir12">>, <<"t3_dir13">>, <<"t3_dir14">>, <<"t3_dir15">>]},
        {SessId2, RootUUID2, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]}
    ]),

    lists:foreach(ValidateReadDir, [
        {SessId1, <<"/spaces/space_name1">>, [<<"t3_dir11">>, <<"t3_dir12">>, <<"t3_dir13">>, <<"t3_dir14">>, <<"t3_dir15">>]},
        {SessId1, <<"/spaces/space_name2">>, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]},
        {SessId2, <<"/spaces/space_name2">>, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]},
        {SessId1, <<"/">>, [?SPACES_BASE_DIR_NAME, <<"t3_dir11">>, <<"t3_dir12">>, <<"t3_dir13">>, <<"t3_dir14">>, <<"t3_dir15">>]},
        {SessId2, <<"/">>, [?SPACES_BASE_DIR_NAME, <<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]}
    ]),

    ok.

chmod_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},
    {SessId3, _UserId3} = {?config({session_id, 3}, Config), ?config({user_id, 3}, Config)},
    {SessId4, _UserId4} = {?config({session_id, 4}, Config), ?config({user_id, 4}, Config)},

    lists:foreach(
        fun(SessId) ->
            Path = <<"/t4_test">>,
            ParentUUID = get_uuid_privileged(Worker, SessId, <<"/">>),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #create_dir{parent_uuid = ParentUUID, name = <<"t4_test">>, mode = 8#000})),
            UUID = get_uuid(Worker, SessId, Path),

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #change_mode{uuid = UUID, mode = 8#123})),

            FileAttr = ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            #fuse_response{fuse_response = #file_attr{uuid = UUID, mode = 8#123}} = FileAttr

        end, [SessId1, SessId2, SessId3, SessId4]).


default_permissions_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},
    {SessId3, _UserId3} = {?config({session_id, 3}, Config), ?config({user_id, 3}, Config)},
    {SessId4, _UserId4} = {?config({session_id, 4}, Config), ?config({user_id, 4}, Config)},

    lists:foreach(
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EACCES}}, ?req(Worker, SessId, #delete_file{uuid = UUID}))
                end, SessIds)

        end, [
            {<<"/">>, [SessId1, SessId2, SessId3, SessId4]},
            {<<"/spaces">>, [SessId1, SessId2, SessId3, SessId4]},
            {<<"/spaces/space_name1">>, [SessId1]},
            {<<"/spaces/space_name2">>, [SessId1, SessId2]},
            {<<"/spaces/space_name3">>, [SessId1, SessId2, SessId3]},
            {<<"/spaces/space_name4">>, [SessId1, SessId2, SessId3, SessId4]}
        ]),


    lists:foreach(
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),

                    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, ?req(Worker, SessId, #delete_file{uuid = UUID}))
                end, SessIds)

        end, [
            {<<"/spaces/space_name1">>, [SessId2, SessId3, SessId4]},
            {<<"/spaces/space_name2">>, [SessId3, SessId4]},
            {<<"/spaces/space_name3">>, [SessId4]}
        ]),

    lists:foreach( %% File
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EACCES}},
                        ?req(Worker, SessId, #create_dir{parent_uuid = UUID, mode = 8#777, name = <<"test">>}))
                end, SessIds)

        end, [
            {<<"/spaces">>, [SessId1, SessId2, SessId3, SessId4]}
        ]),

    lists:foreach( %% File
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}},
                        ?req(Worker, SessId, #create_dir{parent_uuid = UUID, mode = 8#777, name = <<"test">>}))
                end, SessIds)

        end, [
            {<<"/spaces/space_name1">>, [SessId2, SessId3, SessId4]},
            {<<"/spaces/space_name2">>, [SessId3, SessId4]},
            {<<"/spaces/space_name3">>, [SessId4]},
            {<<"/spaces/space_name4">>, []}
        ]),

    lists:foreach(fun %% File
        ({mkdir, Parent, Name, Mode, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Parent),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, #create_dir{parent_uuid = UUID, mode = Mode, name = Name}))
                end, SessIds);
        ({delete, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, #delete_file{uuid = UUID}))
                end, SessIds);
        ({get_attr, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}))
                end, SessIds);
        ({readdir, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, #get_file_children{uuid = UUID}))
                end, SessIds);
        ({chmod, Path, Mode, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    UUID = get_uuid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, #change_mode{uuid = UUID, mode = Mode}))
                end, SessIds)
    end,
        [
            {mkdir, <<"/spaces/space_name1">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/spaces/space_name1/test">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/spaces/space_name1/test/test">>, <<"test">>, 8#777, [SessId1], ?OK},
            {get_attr, <<"/spaces/space_name1/test/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/spaces/space_name1/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/spaces/space_name1/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/spaces/space_name1">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/spaces/space_name1/test/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/spaces/space_name1/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/spaces/space_name1/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/spaces/space_name1">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {mkdir, <<"/spaces/space_name4">>, <<"test">>, 8#740, [SessId4], ?OK},
            {mkdir, <<"/spaces/space_name4/test">>, <<"test">>, 8#1770, [SessId4], ?OK},
            {mkdir, <<"/spaces/space_name4/test/test">>, <<"test">>, 8#730, [SessId4], ?OK},
            {readdir, <<"/spaces/space_name4/test/test/test">>, [SessId4], ?OK},
            {readdir, <<"/spaces/space_name4/test/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/spaces/space_name4/test/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/spaces/space_name4/test/test/test">>, [SessId4], ?OK},
            {delete, <<"/spaces/space_name4/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/spaces/space_name4/test/test">>, [SessId4], ?OK},
            {delete, <<"/spaces/space_name4/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/spaces/space_name4/test">>, [SessId4], ?OK},
            {chmod, <<"/">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EACCES},
            {chmod, <<"/spaces">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EACCES},
            {chmod, <<"/spaces/space_name1">>, 8#123, [SessId1], ?EACCES},
            {chmod, <<"/spaces/space_name2">>, 8#123, [SessId1, SessId2], ?EACCES},
            {chmod, <<"/spaces/space_name3">>, 8#123, [SessId1, SessId2, SessId3], ?EACCES},
            {chmod, <<"/spaces/space_name4">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EACCES},
            {mkdir, <<"/spaces/space_name4">>, <<"test">>, 8#740, [SessId3], ?OK},
            {chmod, <<"/spaces/space_name4/test">>, 8#123, [SessId1, SessId2], ?EACCES},
            {chmod, <<"/spaces/space_name4/test">>, 8#123, [SessId4], ?EACCES},
            {chmod, <<"/spaces/space_name4/test">>, 8#123, [SessId3], ?OK}
        ]).


simple_rename_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},
    {_SessId3, _UserId3} = {?config({session_id, 3}, Config), ?config({user_id, 3}, Config)},
    {_SessId4, _UserId4} = {?config({session_id, 4}, Config), ?config({user_id, 4}, Config)},

    test_utils:mock_expect(Worker, oz_spaces, get_providers,
        fun
            (provider, _SpaceId) ->
                {ok, [oneprovider:get_provider_id()]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    RootFileAttr1 = ?req(Worker, SessId1, #get_file_attr{entry = {path, <<"/">>}}),
    RootFileAttr2 = ?req(Worker, SessId2, #get_file_attr{entry = {path, <<"/">>}}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr2),

    #fuse_response{fuse_response = #file_attr{uuid = RootUUID1}} = RootFileAttr1,
    #fuse_response{fuse_response = #file_attr{uuid = _RootUUID2}} = RootFileAttr2,

    MakeTree =
        fun(Leaf, {SessId, DefaultSpaceName, Path, ParentUUID, FileUUIDs}) ->
            NewPath = <<Path/binary, "/", Leaf/binary>>,
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(Worker, SessId,
                #create_dir{parent_uuid = ParentUUID, name = Leaf, mode = 8#755}
            )),

            FileAttr = ?req(Worker, SessId, #get_file_attr{entry = {path, NewPath}}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            ?assertEqual(FileAttr, ?req(Worker, SessId,
                #get_file_attr{entry = {path, <<"/spaces/", DefaultSpaceName/binary, NewPath/binary>>}})),
            #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,

            {SessId, DefaultSpaceName, NewPath, FileUUID, [FileUUID | FileUUIDs]}
        end,

    {_, _, _, _, UUIDs1} = lists:foldl(MakeTree, {SessId1, <<"space_name1">>, <<>>, RootUUID1, []},
        [<<"t6_dir1">>, <<"t6_dir2">>, <<"t6_dir3">>]),
    [_, ToMove | _] = lists:reverse(UUIDs1),

    RenameResp1 = ?req(Worker, SessId1, #rename{uuid = ToMove, target_path = <<"/spaces/space_name2/t6_dir4">>}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RenameResp1),

    MovedFileAttr1 = ?req(Worker, SessId2, #get_file_attr{entry = {path, <<"/spaces/space_name2/t6_dir4">>}}),
    MovedFileAttr2 = ?req(Worker, SessId2, #get_file_attr{entry = {path, <<"/spaces/space_name2/t6_dir4/t6_dir3">>}}),
    MovedFileAttr3 = ?req(Worker, SessId2, #get_file_attr{entry = {path, <<"/spaces/space_name1/t6_dir1/t6_dir2">>}}),
    MovedFileAttr4 = ?req(Worker, SessId2, #get_file_attr{entry = {path, <<"/spaces/space_name1/t6_dir1/t6_dir2/t6_dir3">>}}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, MovedFileAttr1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, MovedFileAttr2),

    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, MovedFileAttr3),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, MovedFileAttr4).


update_times_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},
    {SessId3, _UserId3} = {?config({session_id, 3}, Config), ?config({user_id, 3}, Config)},
    {SessId4, _UserId4} = {?config({session_id, 4}, Config), ?config({user_id, 4}, Config)},

    GetTimes =
        fun(Entry, SessId) ->
            FileAttr = ?req(Worker, SessId, #get_file_attr{entry = Entry}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            #fuse_response{fuse_response = #file_attr{atime = ATime, mtime = MTime, ctime = CTime}} = FileAttr,
            {ATime, MTime, CTime}
        end,

    lists:foreach(
        fun(SessId) ->
            Path = <<"/t7_test">>,
            ParentUUID = get_uuid_privileged(Worker, SessId, <<"/">>),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #create_dir{parent_uuid = ParentUUID, name = <<"t7_test">>, mode = 8#000})),
            UUID = get_uuid(Worker, SessId, Path),

            {_OldATime, OldMTime, OldCTime} = GetTimes({uuid, UUID}, SessId),

            NewATime = 1234565,
            NewMTime = 9275629,
            NewCTime = 7837652,

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #update_times{uuid = UUID, atime = NewATime})),

            ?assertMatch({NewATime, OldMTime, OldCTime}, GetTimes({uuid, UUID}, SessId)),

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #update_times{uuid = UUID, mtime = NewMTime, ctime = NewCTime})),

            ?assertMatch({NewATime, NewMTime, NewCTime}, GetTimes({uuid, UUID}, SessId))

        end, [SessId1, SessId2, SessId3, SessId4]).


%% Get uuid of given by path file. Possible as a space member to bypass permissions checks.
get_uuid_privileged(Worker, SessId, Path) ->
    SessId1 = case Path of
                  <<"/">> ->
                      SessId;
                  <<"/spaces">> ->
                      SessId;
                  _ ->
                      {ok, [_, _, SpaceName | _]} = fslogic_path:verify_file_path(Path),
                      hd(get(SpaceName))
              end,
    get_uuid(Worker, SessId1, Path).


get_uuid(Worker, SessId, Path) ->
    RootFileAttr = ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr),
    #fuse_response{fuse_response = #file_attr{uuid = UUID}} = RootFileAttr,
    UUID.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, luma_utils),
    test_utils:mock_expect(Workers, luma_utils, get_storage_type, fun(_) -> ?DIRECTIO_HELPER_NAME end),
    initializer:communicator_mock(Workers),
    initializer:create_test_users_and_spaces(Config).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:clean_test_users_and_spaces(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator, luma_utils]).
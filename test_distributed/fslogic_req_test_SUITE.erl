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

-define(req(W, SessId, ContextEntry, FuseRequest), rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{
        context_entry = ContextEntry, fuse_request = FuseRequest}}])).

%%%===================================================================
%%% Test functions
%%%===================================================================

fslogic_get_file_attr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    lists:foreach(fun({SessId, Name, Mode, UID, Path}) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_attr{
                name = Name, type = ?DIRECTORY_TYPE, mode = Mode,
                uid = UID
            }
        }, ?req(Worker, SessId, {path, Path}, #get_file_attr{}))
    end, [
        {SessId1, UserId1, 8#1755, 0, <<"/">>},
        {SessId2, UserId2, 8#1755, 0, <<"/">>},
        {SessId1, <<"space_id1">>, 8#1770, 0, <<"/space_name1">>},
        {SessId2, <<"space_id2">>, 8#1770, 0, <<"/space_name2">>},
        {SessId1, <<"space_id3">>, 8#1770, 0, <<"/space_name3">>},
        {SessId2, <<"space_id4">>, 8#1770, 0, <<"/space_name4">>}
    ]),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, ?req(Worker,
        SessId1, {path, <<"/space_name1/t1_dir">>}, #get_file_attr{}
    )).

fslogic_mkdir_and_rmdir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    RootFileAttr1 = ?req(Worker, SessId1, {path, <<"/space_name1">>}, #get_file_attr{}),
    RootFileAttr2 = ?req(Worker, SessId2, {path, <<"/space_name2">>}, #get_file_attr{}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr2),

    #fuse_response{fuse_response = #file_attr{uuid = RootUUID1}} = RootFileAttr1,
    #fuse_response{fuse_response = #file_attr{uuid = RootUUID2}} = RootFileAttr2,

    MakeTree = fun(Leaf, {SessId, DefaultSpaceName, Path, ParentUUID, FileUUIDs}) ->
        NewPath = <<Path/binary, "/", Leaf/binary>>,
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(Worker, SessId,
            {guid, ParentUUID}, #create_dir{name = Leaf, mode = 8#755}
        )),

        FileAttr = ?req(Worker, SessId, {path, NewPath}, #get_file_attr{}),
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
        #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,

        {SessId, DefaultSpaceName, NewPath, FileUUID, [FileUUID | FileUUIDs]}
    end,

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(Worker, SessId1,
        {guid, RootUUID1}, #create_dir{name = <<"t2_double">>, mode = 8#755}
    )),
    ?assertMatch(#fuse_response{status = #status{code = ?EEXIST}}, ?req(Worker, SessId1,
        {guid, RootUUID1}, #create_dir{name = <<"t2_double">>, mode = 8#755}
    )),


    {_, _, _, _, UUIDs1} = lists:foldl(MakeTree, {SessId1, <<"space_name1">>, <<"/space_name1">>, RootUUID1, []},
        [<<"t2_dir1">>, <<"t2_dir2">>, <<"t2_dir3">>]),
    {_, _, _, _, UUIDs2} = lists:foldl(MakeTree, {SessId2, <<"space_name2">>, <<"/space_name2">>, RootUUID2, []},
        [<<"t2_dir4">>, <<"t2_dir5">>, <<"t2_dir6">>]),

    TestPath1 = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, <<"space_name2">>,
        <<"t2_dir4">>, <<"t2_dir5">>, <<"t2_dir6">>]),
    FileAttr = ?req(Worker, SessId1, {path, TestPath1}, #get_file_attr{}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
    ?assertEqual(FileAttr, ?req(Worker, SessId2, {path, TestPath1}, #get_file_attr{})),

    lists:foreach(fun(GUID) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            ?req(Worker, SessId1, {guid, GUID}, #delete_file{}))
    end, lists:reverse(tl(UUIDs1))),

    lists:foreach(fun(GUID) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            ?req(Worker, SessId2, {guid, GUID}, #delete_file{}))
    end, lists:reverse(tl(UUIDs2))).

fslogic_read_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    ValidateReadDir = fun({SessId, Path, NameList}) ->
        FileAttr = ?req(Worker, SessId, {path, Path}, #get_file_attr{}),
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
        #fuse_response{fuse_response = #file_attr{uuid = FileGUID}} = FileAttr,

        ExpectedNames = lists:sort(NameList),

        lists:foreach( %% Size
            fun(Size) ->
                lists:foreach( %% Offset step
                    fun(OffsetStep) ->
                        {_, Names} = lists:foldl( %% foreach Offset
                            fun(_, {Offset, CurrentChildren}) ->
                                Response = ?req(Worker, SessId, {guid, FileGUID}, #get_file_children{offset = Offset, size = Size}),

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
        {SessId1, <<"/">>, [<<"space_name1">>, <<"space_name2">>, <<"space_name3">>, <<"space_name4">>]},
        {SessId2, <<"/">>, [<<"space_name2">>, <<"space_name3">>, <<"space_name4">>]},
        {SessId3, <<"/">>, [<<"space_name3">>, <<"space_name4">>]},
        {SessId4, <<"/">>, [<<"space_name4">>]}
    ]),

    RootGUID1 = get_guid_privileged(Worker, SessId1, <<"/space_name1">>),
    RootGUID2 = get_guid_privileged(Worker, SessId2, <<"/space_name2">>),

    lists:foreach(fun({SessId, RootGUID, Dirs}) ->
        lists:foreach(fun(Name) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(
                Worker, SessId, {guid, RootGUID}, #create_dir{name = Name, mode = 8#755}
            ))
        end, Dirs)
    end, [
        {SessId1, RootGUID1, [<<"t3_dir11">>, <<"t3_dir12">>, <<"t3_dir13">>, <<"t3_dir14">>, <<"t3_dir15">>]},
        {SessId2, RootGUID2, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]}
    ]),

    lists:foreach(ValidateReadDir, [
        {SessId1, <<"/space_name1">>, [<<"t3_dir11">>, <<"t3_dir12">>, <<"t3_dir13">>, <<"t3_dir14">>, <<"t3_dir15">>]},
        {SessId1, <<"/space_name2">>, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]},
        {SessId2, <<"/space_name2">>, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]}
    ]),

    ok.

chmod_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    lists:foreach(
        fun(SessId) ->
            Path = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, <<"space_name4">>, SessId]),
            ParentGUID = get_guid_privileged(Worker, SessId, <<"/space_name4">>),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, {guid, ParentGUID}, #create_dir{name = SessId, mode = 8#000})),
            GUID = get_guid(Worker, SessId, Path),

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, {guid, GUID}, #change_mode{mode = 8#123})),

            FileAttr = ?req(Worker, SessId, {path, Path}, #get_file_attr{}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            #fuse_response{fuse_response = #file_attr{uuid = GUID, mode = 8#123}} = FileAttr

        end, [SessId1, SessId2, SessId3, SessId4]).


default_permissions_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    lists:foreach(
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EACCES}}, ?req(Worker, SessId, {guid, GUID}, #delete_file{}))
                end, SessIds)

        end, [
            {<<"/">>, [SessId1, SessId2, SessId3, SessId4]},
            {<<"/space_name1">>, [SessId1]},
            {<<"/space_name2">>, [SessId1, SessId2]},
            {<<"/space_name3">>, [SessId1, SessId2, SessId3]},
            {<<"/space_name4">>, [SessId1, SessId2, SessId3, SessId4]}
        ]),


    lists:foreach(
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),

                    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, ?req(Worker, SessId, {guid, GUID}, #delete_file{}))
                end, SessIds)

        end, [
            {<<"/space_name1">>, [SessId2, SessId3, SessId4]},
            {<<"/space_name2">>, [SessId3, SessId4]},
            {<<"/space_name3">>, [SessId4]}
        ]),

    lists:foreach( %% File
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EACCES}},
                        ?req(Worker, SessId, {guid, GUID}, #create_dir{mode = 8#777, name = <<"test">>}))
                end, SessIds)

        end, [
            {<<"/">>, [SessId1, SessId2, SessId3, SessId4]}
        ]),

    lists:foreach( %% File
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}},
                        ?req(Worker, SessId, {guid, GUID}, #create_dir{mode = 8#777, name = <<"test">>}))
                end, SessIds)

        end, [
            {<<"/space_name1">>, [SessId2, SessId3, SessId4]},
            {<<"/space_name2">>, [SessId3, SessId4]},
            {<<"/space_name3">>, [SessId4]},
            {<<"/space_name4">>, []}
        ]),

    lists:foreach(fun %% File
        ({mkdir, Parent, Name, Mode, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Parent),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, {guid, GUID}, #create_dir{mode = Mode, name = Name}))
                end, SessIds);
        ({delete, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, {guid, GUID}, #delete_file{}))
                end, SessIds);
        ({get_attr, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, {path, Path}, #get_file_attr{}))
                end, SessIds);
        ({readdir, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, {guid, GUID}, #get_file_children{}))
                end, SessIds);
        ({chmod, Path, Mode, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    GUID = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, {guid, GUID}, #change_mode{mode = Mode}))
                end, SessIds)
    end,
        [
            {mkdir, <<"/space_name1">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/space_name1/test">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/space_name1/test/test">>, <<"test">>, 8#777, [SessId1], ?OK},
            {get_attr, <<"/space_name1/test/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/space_name1/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/space_name1/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/space_name1">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1/test/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {mkdir, <<"/space_name4">>, <<"test">>, 8#740, [SessId4], ?OK},
            {mkdir, <<"/space_name4/test">>, <<"test">>, 8#1770, [SessId4], ?OK},
            {mkdir, <<"/space_name4/test/test">>, <<"test">>, 8#730, [SessId4], ?OK},
            {readdir, <<"/space_name4/test/test/test">>, [SessId4], ?OK},
            {readdir, <<"/space_name4/test/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test/test/test">>, [SessId4], ?OK},
            {delete, <<"/space_name4/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test/test">>, [SessId4], ?OK},
            {delete, <<"/space_name4/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test">>, [SessId4], ?OK},
            {chmod, <<"/">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EACCES},
            {chmod, <<"/space_name1">>, 8#123, [SessId1], ?EACCES},
            {chmod, <<"/space_name2">>, 8#123, [SessId1, SessId2], ?EACCES},
            {chmod, <<"/space_name3">>, 8#123, [SessId1, SessId2, SessId3], ?EACCES},
            {chmod, <<"/space_name4">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EACCES},
            {mkdir, <<"/space_name4">>, <<"test">>, 8#740, [SessId3], ?OK},
            {chmod, <<"/space_name4/test">>, 8#123, [SessId1, SessId2], ?EACCES},
            {chmod, <<"/space_name4/test">>, 8#123, [SessId4], ?EACCES},
            {chmod, <<"/space_name4/test">>, 8#123, [SessId3], ?OK}
        ]).


simple_rename_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {_SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {_SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    test_utils:mock_expect(Worker, oz_spaces, get_providers,
        fun
            (provider, _SpaceId) ->
                {ok, [oneprovider:get_provider_id()]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    RootFileAttr1 = ?req(Worker, SessId1, {path, <<"/space_name1">>}, #get_file_attr{}),
    RootFileAttr2 = ?req(Worker, SessId2, {path, <<"/space_name2">>}, #get_file_attr{}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr2),

    #fuse_response{fuse_response = #file_attr{uuid = RootUUID1}} = RootFileAttr1,
    #fuse_response{fuse_response = #file_attr{uuid = _RootUUID2}} = RootFileAttr2,

    MakeTree =
        fun(Leaf, {SessId, DefaultSpaceName, Path, ParentUUID, FileUUIDs}) ->
            NewPath = <<Path/binary, "/", Leaf/binary>>,
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?req(Worker, SessId,
                {guid, ParentUUID}, #create_dir{name = Leaf, mode = 8#755}
            )),

            FileAttr = ?req(Worker, SessId, {path, NewPath}, #get_file_attr{}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,

            {SessId, DefaultSpaceName, NewPath, FileUUID, [FileUUID | FileUUIDs]}
        end,

    {_, _, _, _, UUIDs1} = lists:foldl(MakeTree, {SessId1, <<"space_name1">>, <<"/space_name1">>, RootUUID1, []},
        [<<"t6_dir1">>, <<"t6_dir2">>, <<"t6_dir3">>]),
    [_, ToMove | _] = lists:reverse(UUIDs1),

    RenameResp1 = ?req(Worker, SessId1, {guid, ToMove}, #rename{target_path = <<"/space_name2/t6_dir4">>}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RenameResp1),

    MovedFileAttr1 = ?req(Worker, SessId2, {path, <<"/space_name2/t6_dir4">>}, #get_file_attr{}),
    MovedFileAttr2 = ?req(Worker, SessId2, {path, <<"/space_name2/t6_dir4/t6_dir3">>}, #get_file_attr{}),
    MovedFileAttr3 = ?req(Worker, SessId2, {path, <<"/space_name1/t6_dir1/t6_dir2">>}, #get_file_attr{}),
    MovedFileAttr4 = ?req(Worker, SessId2, {path, <<"/space_name1/t6_dir1/t6_dir2/t6_dir3">>}, #get_file_attr{}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, MovedFileAttr1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, MovedFileAttr2),

    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, MovedFileAttr3),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, MovedFileAttr4).


update_times_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    GetTimes =
        fun(Entry, SessId) ->
            FileAttr = ?req(Worker, SessId, Entry, #get_file_attr{}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            #fuse_response{fuse_response = #file_attr{atime = ATime, mtime = MTime, ctime = CTime}} = FileAttr,
            {ATime, MTime, CTime}
        end,

    lists:foreach(
        fun(SessId) ->
            FileName = <<"file_", SessId/binary>>,
            Path = <<"/space_name4/", FileName/binary>>,
            ParentGUID = get_guid_privileged(Worker, SessId, <<"/space_name4">>),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, {guid, ParentGUID}, #create_dir{name = FileName, mode = 8#000})),
            GUID = get_guid(Worker, SessId, Path),

            {_OldATime, OldMTime, OldCTime} = GetTimes({guid, GUID}, SessId),

            NewATime = 1234565,
            NewMTime = 9275629,
            NewCTime = 7837652,

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, {guid, GUID}, #update_times{atime = NewATime})),

            ?assertMatch({NewATime, OldMTime, OldCTime}, GetTimes({guid, GUID}, SessId)),

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, {guid, GUID}, #update_times{mtime = NewMTime, ctime = NewCTime})),

            ?assertMatch({NewATime, NewMTime, NewCTime}, GetTimes({guid, GUID}, SessId))

        end, [SessId1, SessId2, SessId3, SessId4]).


%% Get uuid of given by path file. Possible as a space member to bypass permissions checks.
get_guid_privileged(Worker, SessId, Path) ->
    SessId1 = case Path of
                  <<"/">> ->
                      SessId;
                  _ ->
                      {ok, [_, SpaceName | _]} = fslogic_path:verify_file_path(Path),
                      hd(get(SpaceName))
              end,
    get_guid(Worker, SessId1, Path).


get_guid(Worker, SessId, Path) ->
    RootFileAttr = ?req(Worker, SessId, {path, Path}, #get_file_attr{}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr),
    #fuse_response{fuse_response = #file_attr{uuid = GUID}} = RootFileAttr,
    GUID.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Config0 = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(Config0).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, luma_utils),
    test_utils:mock_expect(Workers, luma_utils, get_storage_type, fun(_) -> ?DIRECTIO_HELPER_NAME end),
    initializer:communicator_mock(Workers),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator, luma_utils]).
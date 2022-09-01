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

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    fslogic_get_file_attr_test/1,
    fslogic_get_file_attr_with_replication_status_not_requested_test/1,
    fslogic_get_file_attr_with_replication_status_requested_test/1,
    fslogic_get_file_children_attrs_test/1,
    fslogic_get_file_children_attrs_with_replication_status_test/1,
    fslogic_get_child_attr_test/1,
    fslogic_mkdir_and_rmdir_test/1,
    fslogic_read_dir_test/1,
    chmod_test/1,
    simple_rename_test/1,
    update_times_test/1,
    default_permissions_test/1,
    creating_handle_in_create_test/1,
    creating_handle_in_open_test/1
]).

all() ->
    ?ALL([
        fslogic_get_file_attr_test,
        fslogic_get_file_attr_with_replication_status_not_requested_test,
        fslogic_get_file_attr_with_replication_status_requested_test,
        fslogic_get_file_children_attrs_test,
        fslogic_get_file_children_attrs_with_replication_status_test,
        fslogic_get_child_attr_test,
        fslogic_mkdir_and_rmdir_test,
        fslogic_read_dir_test,
        chmod_test,
        simple_rename_test,
        update_times_test,
        default_permissions_test,
        creating_handle_in_create_test,
        creating_handle_in_open_test
    ]).

-define(TIMEOUT, timer:seconds(5)).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(file_req(W, SessId, ContextGuid, FileRequest), ?req(W, SessId,
    #file_request{context_guid = ContextGuid, file_request = FileRequest})).

%%%===================================================================
%%% Test functions
%%%===================================================================

fslogic_get_file_attr_test(Config) ->
    fslogic_get_file_attr_test_base(Config, undefined).

fslogic_get_file_attr_with_replication_status_not_requested_test(Config) ->
    fslogic_get_file_attr_test_base(Config, false).

fslogic_get_file_attr_with_replication_status_requested_test(Config) ->
    fslogic_get_file_attr_test_base(Config, true).

fslogic_get_file_attr_test_base(Config, CheckReplicationStatus) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    UserRootGuid1 = fslogic_file_id:user_root_dir_guid(UserId1),
    UserRootGuid2 = fslogic_file_id:user_root_dir_guid(UserId2),

    FileName =  generator:gen_name(),
    FilePath = <<"/space_name1/", FileName/binary>>,
    {ok, FileGuid} = ?assertMatch({ok, _},
        lfm_proxy:create(Worker, SessId1, FilePath)),
    Space1Guid = client_simulation_test_base:get_guid(Worker, SessId1, <<"/space_name1">>),
    {{FileUid, _}, _} = rpc:call(Worker, file_ctx, get_display_credentials, [file_ctx:new_by_guid(FileGuid)]),

    lists:foreach(fun({SessId, Name, Mode, UID, Path, ParentGuid, Type}) ->
        #fuse_response{fuse_response = #guid{guid = Guid}} =
            ?assertMatch(
                #fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #resolve_guid{path = Path})
            ),

        FullyReplicated = case {CheckReplicationStatus, Type} of
            {true, ?REGULAR_FILE_TYPE} -> true;
            _ -> undefined
        end,

        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_attr{
                guid = Guid, name = Name, type = Type, mode = Mode,
                uid = UID, parent_guid = ParentGuid, fully_replicated = FullyReplicated
            }
        }, ?file_req(Worker, SessId, Guid, #get_file_attr{include_replication_status = CheckReplicationStatus})),

        case ParentGuid =/= undefined of
            true ->
                ?assertMatch(#fuse_response{status = #status{code = ?OK},
                    fuse_response = #file_attr{
                        name = Name, type = Type, mode = Mode,
                        uid = UID, parent_guid = ParentGuid, fully_replicated = FullyReplicated
                    }
                }, ?file_req(Worker, SessId, ParentGuid,
                    #get_child_attr{name = Name, include_replication_status = CheckReplicationStatus}));
            false ->
                ok
        end
    end, [
        {SessId1, UserId1, 8#1755, 0, <<"/">>, undefined, ?DIRECTORY_TYPE},
        {SessId2, UserId2, 8#1755, 0, <<"/">>, undefined, ?DIRECTORY_TYPE},
        {SessId1, <<"space_name1">>, ?DEFAULT_DIR_MODE, 0, <<"/space_name1">>, UserRootGuid1, ?DIRECTORY_TYPE},
        {SessId2, <<"space_name2">>, ?DEFAULT_DIR_MODE, 0, <<"/space_name2">>, UserRootGuid2, ?DIRECTORY_TYPE},
        {SessId1, <<"space_name3">>, ?DEFAULT_DIR_MODE, 0, <<"/space_name3">>, UserRootGuid1, ?DIRECTORY_TYPE},
        {SessId2, <<"space_name4">>, ?DEFAULT_DIR_MODE, 0, <<"/space_name4">>, UserRootGuid2, ?DIRECTORY_TYPE},
        {SessId1, FileName, ?DEFAULT_FILE_PERMS, FileUid, FilePath, Space1Guid, ?REGULAR_FILE_TYPE}
    ]),
    ?assertMatch(
        #fuse_response{status = #status{code = ?ENOENT}},
        ?req(Worker, SessId1, #resolve_guid{path = <<"/space_name1/t1_dir">>}
    )),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId1, {path, FilePath})).

fslogic_get_file_children_attrs_with_replication_status_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config),
        ?config({user_id, <<"user2">>}, Config)},

    FileName =  generator:gen_name(),
    FilePath = <<"/space_name4/", FileName/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, FilePath)),
    SpaceGuid = client_simulation_test_base:get_guid(Worker, SessId, <<"/space_name4">>),

    #fuse_response{fuse_response = #file_children_attrs{child_attrs = ChildrenAttrs}} =
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?file_req(Worker, SessId, SpaceGuid,
            #get_file_children_attrs{
                listing_options = #{offset => 0, limit => 1000, tune_for_large_continuous_listing => false}, 
                include_replication_status = true
            })),
    ?assertMatch([_ | _], ChildrenAttrs),

    lists:foreach(fun
        (#file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs) ->
            ?assertMatch(#file_attr{fully_replicated = true}, FileAttrs);
        (FileAttrs) ->
            ?assertMatch(#file_attr{fully_replicated = undefined}, FileAttrs)
    end, ChildrenAttrs),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {path, FilePath})).

fslogic_get_file_children_attrs_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ProviderID = rpc:call(Worker, oneprovider, get_id, []),

    ?assertMatch(ok, test_utils:set_env(Worker, ?APP_NAME,
        max_read_dir_plus_procs, 2)),

    {SessId1, UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    UserRootGuid1 = fslogic_file_id:user_root_dir_guid(UserId1),
    UserRootGuid2 = fslogic_file_id:user_root_dir_guid(UserId2),
    UserRootGuid3 = fslogic_file_id:user_root_dir_guid(UserId3),
    UserRootGuid4 = fslogic_file_id:user_root_dir_guid(UserId4),

    ValidateReadDirPlus = fun({SessId, Path, AttrsList}) ->
        #fuse_response{fuse_response = #guid{guid = FileGuid}} =
            ?assertMatch(
                #fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #resolve_guid{path = Path})
            ),

        lists:foreach( %% Size
            fun(Size) ->
                lists:foreach( %% Offset step
                    fun(OffsetStep) ->
                        {_, Attrs} = lists:foldl( %% foreach Offset
                            fun(_, {Offset, CurrentChildren}) ->
                                Response = ?file_req(Worker, SessId, FileGuid,
                                    #get_file_children_attrs{
                                        listing_options = #{offset => Offset, limit => Size, tune_for_large_continuous_listing => false}
                                    }),

                                ?assertMatch(#fuse_response{status = #status{code = ?OK}}, Response),
                                #fuse_response{fuse_response = #file_children_attrs{
                                    child_attrs = ChildrenAttrs}} = Response,


                                ?assertEqual(min(max(0, length(AttrsList) - Offset), Size), length(ChildrenAttrs)),

                                {Offset + OffsetStep, lists:usort(ChildrenAttrs ++ CurrentChildren)}
                            end, {0, []}, lists:seq(1, 2 * round(length(AttrsList) / OffsetStep))),

                        lists:foreach(fun({A1, A2}) ->
                            #file_attr{
                                guid = Guid, name = Name, type = Type, mode = Mode,
                                uid = UID, parent_guid = ParentGuid, provider_id = ProviderID,
                                owner_id = OwnerID
                            } = A1,
                            #file_attr{
                                guid = Guid2, name = Name2, type = Type2, mode = Mode2,
                                uid = UID2, parent_guid = ParentGuid2, provider_id = ProviderID2,
                                owner_id = OwnerID2, fully_replicated = ReplicationStatus
                            } = A2,

                            ?assertMatch(Guid, Guid2),
                            ?assertMatch(Name, Name2),
                            ?assertMatch(Type, Type2),
                            ?assertMatch(Mode, Mode2),
                            ?assertMatch(UID, UID2),
                            ?assertMatch(ParentGuid, ParentGuid2),
                            ?assertMatch(ProviderID, ProviderID2),
                            ?assertMatch(OwnerID, OwnerID2),
                            ?assertEqual(undefined, ReplicationStatus)
                        end, lists:zip(AttrsList, Attrs))
                    end, lists:seq(1, Size))

            end, lists:seq(1, length(AttrsList) + 1))
    end,

    TestFun = fun({SessionId, Path, NameList, OwnersList, UserRootGuid}) ->
        Files = lists:map(fun({Name, Owner}) ->
            {SessionId, Name, ?DEFAULT_DIR_MODE, ?ROOT_UID, <<"/", Name/binary>>, Owner, UserRootGuid}
        end, lists:zip(NameList, OwnersList)),

        FilesAttrs = lists:map(fun({SessId, Name, Mode, UID, P, Owner, ParentGuid}) ->
            #fuse_response{fuse_response = #guid{guid = Guid}} =
                ?assertMatch(
                    #fuse_response{status = #status{code = ?OK}},
                    ?req(Worker, SessId, #resolve_guid{path = P})
                ),

            #file_attr{
                guid = Guid, name = Name, type = ?DIRECTORY_TYPE, mode = Mode,
                uid = UID, parent_guid = ParentGuid, provider_id = ProviderID,
                owner_id = Owner
            }
        end, Files),

        ValidateReadDirPlus({SessionId, Path, FilesAttrs})
    end,

    lists:foreach(TestFun, [
        {SessId1, <<"/">>, 
            [<<"space_name1">>, <<"space_name2">>, <<"space_name3">>, <<"space_name4">>], 
            [?SPACE_OWNER_ID(SpaceId) || SpaceId <- [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>]],
            UserRootGuid1
        },
        {SessId2, <<"/">>, 
            [<<"space_name2">>, <<"space_name3">>, <<"space_name4">>],
            [?SPACE_OWNER_ID(SpaceId) || SpaceId <- [<<"space_id2">>, <<"space_id3">>, <<"space_id4">>]],
            UserRootGuid2
        },
        {SessId3, <<"/">>, 
            [<<"space_name3">>, <<"space_name4">>],
            [?SPACE_OWNER_ID(SpaceId) || SpaceId <- [<<"space_id3">>, <<"space_id4">>]],
            UserRootGuid3
        },
        {SessId4, <<"/">>, [<<"space_name4">>], [?SPACE_OWNER_ID(<<"space_id4">>)], UserRootGuid4}
    ]),

    ?assertMatch(ok, test_utils:set_env(Worker, ?APP_NAME,
        max_read_dir_plus_procs, 1000)),

    ok.


fslogic_get_child_attr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    UserRootGuid1 = fslogic_file_id:user_root_dir_guid(UserId1),
    UserRootGuid2 = fslogic_file_id:user_root_dir_guid(UserId2),

    lists:foreach(fun({SessId, Name, Mode, UID, ParentGuid, ChildName}) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_attr{
                name = Name, type = ?DIRECTORY_TYPE, mode = Mode,
                uid = UID, parent_guid = ParentGuid
            }
        }, ?file_req(Worker, SessId, ParentGuid, #get_child_attr{name = ChildName}))
    end, [
        {SessId1, <<"space_name1">>, ?DEFAULT_DIR_MODE, 0, UserRootGuid1, <<"space_name1">>},
        {SessId2, <<"space_name2">>, ?DEFAULT_DIR_MODE, 0, UserRootGuid2, <<"space_name2">>},
        {SessId1, <<"space_name3">>, ?DEFAULT_DIR_MODE, 0, UserRootGuid1, <<"space_name3">>},
        {SessId2, <<"space_name4">>, ?DEFAULT_DIR_MODE, 0, UserRootGuid2, <<"space_name4">>}
    ]),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}},
        ?file_req(Worker, SessId1, UserRootGuid1, #get_child_attr{name = <<"no such child">>})).


fslogic_mkdir_and_rmdir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    #fuse_response{fuse_response = #guid{guid = RootGuid1}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId1, #resolve_guid{path = <<"/space_name1">>})
        ),
    #fuse_response{fuse_response = #guid{guid = RootGuid2}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId2, #resolve_guid{path = <<"/space_name2">>})
        ),

    MakeTree = fun(Leaf, {SessId, DefaultSpaceName, Path, ParentUuid, FileGuids}) ->
        NewPath = <<Path/binary, "/", Leaf/binary>>,
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?file_req(Worker, SessId,
            ParentUuid, #create_dir{name = Leaf, mode = ?DEFAULT_DIR_PERMS}
        )),

        #fuse_response{fuse_response = #guid{guid = FileGuid}} =
            ?assertMatch(
                #fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #resolve_guid{path = NewPath})
            ),

        {SessId, DefaultSpaceName, NewPath, FileGuid, [FileGuid | FileGuids]}
    end,

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?file_req(Worker, SessId1,
        RootGuid1, #create_dir{name = <<"t2_double">>, mode = ?DEFAULT_DIR_PERMS}
    )),
    ?assertMatch(#fuse_response{status = #status{code = ?EEXIST}}, ?file_req(Worker, SessId1,
        RootGuid1, #create_dir{name = <<"t2_double">>, mode = ?DEFAULT_DIR_PERMS}
    )),


    {_, _, _, _, Uuids1} = lists:foldl(MakeTree, {SessId1, <<"space_name1">>, <<"/space_name1">>, RootGuid1, []},
        [<<"t2_dir1">>, <<"t2_dir2">>, <<"t2_dir3">>]),
    {_, _, _, _, Uuids2} = lists:foldl(MakeTree, {SessId2, <<"space_name2">>, <<"/space_name2">>, RootGuid2, []},
        [<<"t2_dir4">>, <<"t2_dir5">>, <<"t2_dir6">>]),

    TestPath1 = filepath_utils:join([<<?DIRECTORY_SEPARATOR>>, <<"space_name2">>,
        <<"t2_dir4">>, <<"t2_dir5">>, <<"t2_dir6">>]),
    FileResolvedGuid = ?req(Worker, SessId1, #resolve_guid{path = TestPath1}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileResolvedGuid),
    ?assertEqual(FileResolvedGuid, ?req(Worker, SessId2, #resolve_guid{path = TestPath1})),

    lists:foreach(fun(Guid) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            ?file_req(Worker, SessId1, Guid, #delete_file{}))
    end, lists:reverse(tl(Uuids1))),

    lists:foreach(fun(Guid) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            ?file_req(Worker, SessId2, Guid, #delete_file{}))
    end, lists:reverse(tl(Uuids2))).

fslogic_read_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    ValidateReadDir = fun({SessId, Path, NameList}) ->
        #fuse_response{fuse_response = #guid{guid = FileGuid}} =
            ?assertMatch(
                #fuse_response{status = #status{code = ?OK}},
                ?req(Worker, SessId, #resolve_guid{path = Path})
            ),

        ExpectedNames = lists:sort(NameList),

        lists:foreach( %% Limit
            fun(Limit) ->
                lists:foreach( %% Offset step
                    fun(OffsetStep) ->
                        {_, Names} = lists:foldl( %% foreach Offset
                            fun(_, {Offset, CurrentChildren}) ->
                                Response = ?file_req(Worker, SessId, FileGuid, #get_file_children{
                                    listing_options = #{offset => Offset, limit => Limit, tune_for_large_continuous_listing => false}
                                }),

                                ?assertMatch(#fuse_response{status = #status{code = ?OK}}, Response),
                                #fuse_response{fuse_response = #file_children{child_links = Links}} = Response,

                                RespNames = lists:map(
                                    fun(#child_link{guid = _, name = Name}) ->
                                        Name
                                    end, Links),

                                ?assertEqual(min(max(0, length(ExpectedNames) - Offset), Limit), length(RespNames)),

                                {Offset + OffsetStep, lists:usort(RespNames ++ CurrentChildren)}
                            end, {0, []}, lists:seq(1, 2 * round(length(ExpectedNames) / OffsetStep))),

                        ?assertMatch(ExpectedNames, lists:sort(lists:flatten(Names)))
                    end, lists:seq(1, Limit))

            end, lists:seq(1, length(ExpectedNames) + 1))
    end,

    lists:foreach(ValidateReadDir, [
        {SessId1, <<"/">>, [<<"space_name1">>, <<"space_name2">>, <<"space_name3">>, <<"space_name4">>]},
        {SessId2, <<"/">>, [<<"space_name2">>, <<"space_name3">>, <<"space_name4">>]},
        {SessId3, <<"/">>, [<<"space_name3">>, <<"space_name4">>]},
        {SessId4, <<"/">>, [<<"space_name4">>]}
    ]),

    RootGuid1 = get_guid_privileged(Worker, SessId1, <<"/space_name1">>),
    RootGuid2 = get_guid_privileged(Worker, SessId2, <<"/space_name2">>),

    lists:foreach(fun({SessId, RootGuid, Dirs}) ->
        lists:foreach(fun(Name) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?file_req(
                Worker, SessId, RootGuid, #create_dir{name = Name, mode = ?DEFAULT_DIR_PERMS}
            ))
        end, Dirs)
    end, [
        {SessId1, RootGuid1, [<<"t3_dir11">>, <<"t3_dir12">>, <<"t3_dir13">>, <<"t3_dir14">>, <<"t3_dir15">>]},
        {SessId2, RootGuid2, [<<"t3_dir21">>, <<"t3_dir22">>, <<"t3_dir23">>, <<"t3_dir24">>, <<"t3_dir25">>]}
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
            Path = filepath_utils:join([<<?DIRECTORY_SEPARATOR>>, <<"space_name4">>, SessId]),
            ParentGuid = get_guid_privileged(Worker, SessId, <<"/space_name4">>),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?file_req(Worker, SessId, ParentGuid, #create_dir{name = SessId, mode = 8#000})),
            Guid = get_guid(Worker, SessId, Path),

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?file_req(Worker, SessId, Guid, #change_mode{mode = 8#123})),

            #fuse_response{fuse_response = #guid{guid = Guid}} =
                ?assertMatch(
                    #fuse_response{status = #status{code = ?OK}},
                    ?req(Worker, SessId, #resolve_guid{path = Path})
                ),

            ?assertMatch(
                #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{mode = 8#123}},
                ?file_req(Worker, SessId, Guid, #get_file_attr{})
            )


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
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EPERM}},
                        ?file_req(Worker, SessId, Guid, #delete_file{}))
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
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EPERM}},
                        ?file_req(Worker, SessId, Guid, #delete_file{}))
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
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?EPERM}},
                        ?file_req(Worker, SessId, Guid, #create_dir{mode = 8#777, name = <<"test">>}))
                end, SessIds)

        end, [
            {<<"/">>, [SessId1, SessId2, SessId3, SessId4]}
        ]),

    lists:foreach( %% File
        fun({Path, SessIds}) ->
            lists:foreach(
                fun(SessId) ->
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}},
                        ?file_req(Worker, SessId, Guid, #create_dir{mode = 8#777, name = <<"test">>}))
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
                    Guid = get_guid_privileged(Worker, SessId, Parent),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?file_req(Worker, SessId, Guid, #create_dir{mode = Mode, name = Name}))
                end, SessIds);
        ({delete, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?file_req(Worker, SessId, Guid, #delete_file{}))
                end, SessIds);
        ({get_attr, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?req(Worker, SessId, #resolve_guid{path = Path}))
                end, SessIds);
        ({readdir, Path, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?file_req(Worker, SessId, Guid, #get_file_children{
                            listing_options = #{offset => 0, tune_for_large_continuous_listing => false}
                        }))
                end, SessIds);
        ({chmod, Path, Mode, SessIds, Code}) ->
            lists:foreach(
                fun(SessId) ->
                    Guid = get_guid_privileged(Worker, SessId, Path),
                    ?assertMatch(#fuse_response{status = #status{code = Code}},
                        ?file_req(Worker, SessId, Guid, #change_mode{mode = Mode}))
                end, SessIds)
    end,
        [
            {mkdir, <<"/space_name1">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/space_name1/test">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/space_name1/test/test">>, <<"test">>, 8#777, [SessId1], ?OK},
            {mkdir, <<"/space_name1">>, ?TRASH_DIR_NAME, 8#777, [SessId1], ?EPERM}, % TODO VFS-7064 change to EEXIST
            {get_attr, <<"/space_name1/test/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/space_name1/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/space_name1/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {get_attr, <<"/space_name1">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1/test/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1/test/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {delete, <<"/space_name1">>, [SessId2, SessId3, SessId4], ?EPERM},
            % TODO VFS-7064 uncomment after adding link to trash directory
            % {delete, filename:join([<<"/space_name1">>, ?TRASH_DIR_NAME]), [SessId1, SessId2, SessId3, SessId4], ?EPERM},
            {mkdir, <<"/space_name4">>, <<"test">>, 8#740, [SessId4], ?OK},
            {mkdir, <<"/space_name4/test">>, <<"test">>, 8#1770, [SessId4], ?OK},
            {mkdir, <<"/space_name4/test/test">>, <<"test">>, 8#730, [SessId4], ?OK},
            {readdir, <<"/space_name4/test/test/test">>, [SessId4], ?OK},
            {readdir, <<"/space_name4/test/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test/test/test">>, [SessId4], ?OK},
            {delete, <<"/space_name4/test/test">>, [SessId1, SessId2, SessId3], ?EACCES},
            {delete, <<"/space_name4/test/test">>, [SessId4], ?OK},
            {delete, <<"/space_name4/test">>, [SessId1], ?OK},
            {get_attr, <<"/space_name4/test">>, [SessId2, SessId3, SessId4], ?ENOENT},
            {chmod, <<"/">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EPERM},
            {chmod, <<"/space_name1">>, 8#123, [SessId1], ?EPERM},
            {chmod, <<"/space_name2">>, 8#123, [SessId1, SessId2], ?EPERM},
            {chmod, <<"/space_name3">>, 8#123, [SessId1, SessId2, SessId3], ?EPERM},
            {chmod, <<"/space_name4">>, 8#123, [SessId1, SessId2, SessId3, SessId4], ?EPERM},
            % TODO VFS-7064 uncomment after adding link to trash directory
            % {chmod, filename:join([<<"/space_name1">>, ?TRASH_DIR_NAME]), 8#777, [SessId1], ?EPERM},
            {mkdir, <<"/space_name4">>, <<"test">>, 8#740, [SessId3], ?OK},
            {chmod, <<"/space_name4/test">>, 8#123, [SessId1, SessId2], ?EACCES},
            {chmod, <<"/space_name4/test">>, 8#123, [SessId4], ?EACCES},
            {chmod, <<"/space_name4/test">>, 8#123, [SessId3], ?OK}
        ]).


simple_rename_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    #fuse_response{fuse_response = #guid{guid = RootGuid1}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId1, #resolve_guid{path = <<"/space_name1">>})
        ),
    #fuse_response{fuse_response = #guid{guid = RootGuid2}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId2, #resolve_guid{path = <<"/space_name2">>})
        ),

    MakeTree =
        fun(Leaf, {SessId, DefaultSpaceName, Path, ParentUuid, FileUuids}) ->
            NewPath = <<Path/binary, "/", Leaf/binary>>,
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, ?file_req(Worker, SessId,
                ParentUuid, #create_dir{name = Leaf, mode = ?DEFAULT_DIR_PERMS}
            )),

            #fuse_response{fuse_response = #guid{guid = FileUuid}} =
                ?assertMatch(
                    #fuse_response{status = #status{code = ?OK}},
                    ?req(Worker, SessId, #resolve_guid{path = NewPath})
                ),

            {SessId, DefaultSpaceName, NewPath, FileUuid, [FileUuid | FileUuids]}
        end,

    {_, _, _, _, Uuids1} = lists:foldl(MakeTree, {SessId1, <<"space_name1">>, <<"/space_name1">>, RootGuid1, []},
        [<<"t6_dir1">>, <<"t6_dir2">>, <<"t6_dir3">>]),
    [_, ToMove | _] = lists:reverse(Uuids1),

    RenameResp1 = ?file_req(Worker, SessId1, ToMove, #rename{target_parent_guid = RootGuid2, target_name = <<"t6_dir4">>}),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RenameResp1),

    ?assertMatch(
        #fuse_response{status = #status{code = ?OK}},
        ?req(Worker, SessId2, #resolve_guid{path = <<"/space_name2/t6_dir4">>})
    ),
    ?assertMatch(
        #fuse_response{status = #status{code = ?OK}},
        ?req(Worker, SessId2, #resolve_guid{path = <<"/space_name2/t6_dir4/t6_dir3">>})
    ),
    ?assertMatch(
        #fuse_response{status = #status{code = ?ENOENT}},
        ?req(Worker, SessId2, #resolve_guid{path = <<"/space_name1/t6_dir1/t6_dir2">>})
    ),
    ?assertMatch(
        #fuse_response{status = #status{code = ?ENOENT}},
        ?req(Worker, SessId2, #resolve_guid{path = <<"/space_name1/t6_dir1/t6_dir2/t6_dir3">>})
    ).


update_times_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},
    {SessId3, _UserId3} = {?config({session_id, {<<"user3">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user3">>}, Config)},
    {SessId4, _UserId4} = {?config({session_id, {<<"user4">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user4">>}, Config)},

    GetTimes =
        fun(Guid, SessId) ->
            FileAttr = ?file_req(Worker, SessId, Guid, #get_file_attr{}),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
            #fuse_response{fuse_response = #file_attr{atime = ATime, mtime = MTime, ctime = CTime}} = FileAttr,
            {ATime, MTime, CTime}
        end,

    lists:foreach(
        fun(SessId) ->
            FileName = <<"file_", SessId/binary>>,
            Path = <<"/space_name4/", FileName/binary>>,
            ParentGuid = get_guid_privileged(Worker, SessId, <<"/space_name4">>),
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?file_req(Worker, SessId, ParentGuid, #create_dir{name = FileName, mode = 8#000})),
            Guid = get_guid(Worker, SessId, Path),

            {_OldATime, OldMTime, OldCTime} = GetTimes(Guid, SessId),

            NewATime = 1234565,
            NewMTime = 9275629,
            NewCTime = 7837652,

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?file_req(Worker, SessId, Guid, #update_times{atime = NewATime})),

            ?assertMatch({NewATime, OldMTime, OldCTime}, GetTimes(Guid, SessId)),

            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                ?file_req(Worker, SessId, Guid, #update_times{mtime = NewMTime, ctime = NewCTime})),

            ?assertMatch({NewATime, NewMTime, NewCTime}, GetTimes(Guid, SessId))

        end, [SessId1, SessId2, SessId3, SessId4]).


creating_handle_in_create_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId, <<"/space_name2/handle_test_dir">>),

    BaseReq = #create_file{mode = 8#777},

    Resp1 = ?file_req(W, SessId, DirGuid, BaseReq#create_file{
        name = <<"file1">>
    }),
    Resp2 = ?file_req(W, ?ROOT_SESS_ID, DirGuid, BaseReq#create_file{
        name = <<"file2">>
    }),

    #fuse_response{fuse_response = #file_created{handle_id = HandleId1}} =
        ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_created{}}, Resp1),
    #fuse_response{fuse_response = #file_created{handle_id = HandleId2}} =
        ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_created{}}, Resp2),

    ?assertMatch(<<_/binary>>, HandleId1),
    ?assertMatch(<<_/binary>>, HandleId2).


creating_handle_in_open_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    {ok, Guid} = lfm_proxy:create(W, SessId, <<"/space_name2/handle_test_file">>),

    Resp1 = ?file_req(W, SessId, Guid, #get_file_location{}),
    Resp2 = ?file_req(W, ?ROOT_SESS_ID, Guid, #get_file_location{}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{}}, Resp1),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{}}, Resp2).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Get uuid of given by path file. Possible as a space member to bypass permissions checks.
get_guid_privileged(Worker, SessId, Path) ->
    SessId1 = case Path of
        <<"/">> ->
            SessId;
        _ ->
            {ok, [_, SpaceName | _]} = filepath_utils:split_and_skip_dots(Path),
            hd(get(SpaceName))
    end,
    get_guid(Worker, SessId1, Path).


get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path})
        ),
    Guid.

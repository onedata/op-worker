%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of logical_file_manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_test_SUITE).
-author("Rafal Slota").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    fslogic_new_file_test/1,
    lfm_create_and_unlink_test/1,
    lfm_create_and_access_test/1,
    lfm_write_test/1,
    lfm_stat_test/1,
    lfm_synch_stat_test/1,
    lfm_truncate_test/1,
    lfm_acl_test/1
]).

all() ->
    ?ALL([
        fslogic_new_file_test,
        lfm_create_and_unlink_test,
        lfm_create_and_access_test,
        lfm_write_test,
        lfm_stat_test,
        lfm_synch_stat_test,
        lfm_truncate_test,
        lfm_acl_test
    ]).

-define(TIMEOUT, timer:seconds(10)).
-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}], ?TIMEOUT)).
-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================

fslogic_new_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    RootUUID1 = get_uuid_privileged(Worker, SessId1, <<"/">>),
    RootUUID2 = get_uuid_privileged(Worker, SessId2, <<"/">>),

    Resp11 = ?req(Worker, SessId1, #get_new_file_location{parent_uuid = RootUUID1, name = <<"test">>}),
    Resp21 = ?req(Worker, SessId2, #get_new_file_location{parent_uuid = RootUUID2, name = <<"test">>}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{}}, Resp11),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{}}, Resp21),

    #fuse_response{fuse_response = #file_location{
        file_id = FileId11,
        storage_id = StorageId11,
        provider_id = ProviderId11}} = Resp11,

    #fuse_response{fuse_response = #file_location{
        file_id = FileId21,
        storage_id = StorageId21,
        provider_id = ProviderId21}} = Resp21,

    ?assertNotMatch(undefined, FileId11),
    ?assertNotMatch(undefined, FileId21),

    TestStorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    ?assertMatch(TestStorageId, StorageId11),
    ?assertMatch(TestStorageId, StorageId21),

    TestProviderId = rpc:call(Worker, oneprovider, get_provider_id, []),
    ?assertMatch(TestProviderId, ProviderId11),
    ?assertMatch(TestProviderId, ProviderId21).

lfm_create_and_access_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    FilePath1 = <<"/spaces/space_name3/", (generator:gen_name())/binary>>,
    FilePath2 = <<"/spaces/space_name3/", (generator:gen_name())/binary>>,
    FilePath3 = <<"/spaces/space_name3/", (generator:gen_name())/binary>>,
    FilePath4 = <<"/spaces/space_name3/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath1, 8#240)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath2, 8#640)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath3, 8#670)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath4, 8#540)),

    %% File #1
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath1}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath1}, read)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath1}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath1}, read)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath1}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath1}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath1}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath1}, 10)),

    %% File #2
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath2}, read)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath2}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath2}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath2}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath2}, 10)),

    %% File #3
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, rdwr)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath3}, 10)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath3}, 10)),

    %% File #4
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath4}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath4}, read)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath4}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath4}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath4}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath4}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId1, {path, FilePath4}, 10)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath4}, 10)).

lfm_create_and_unlink_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    _RootUUID1 = get_uuid_privileged(W, SessId1, <<"/">>),
    _RootUUID2 = get_uuid_privileged(W, SessId2, <<"/">>),

    FilePath1 = <<"/", (generator:gen_name())/binary>>,
    FilePath2 = <<"/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath1, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath2, 8#755)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(W, SessId1, FilePath1, 8#755)),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath1, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath2, 8#755)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(W, SessId2, FilePath1, 8#755)),

    ?assertMatch(ok, lfm_proxy:unlink(W, SessId1, {path, FilePath1})),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId2, {path, FilePath2})),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(W, SessId1, {path, FilePath1})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(W, SessId2, {path, FilePath2})),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath1, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath2, 8#755)).

lfm_write_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    _RootUUID1 = get_uuid_privileged(W, SessId1, <<"/">>),
    _RootUUID2 = get_uuid_privileged(W, SessId2, <<"/">>),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/test3">>, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/test4">>, 8#755)),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/test3">>, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/test4">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/test3">>}, rdwr),
    O12 = lfm_proxy:open(W, SessId1, {path, <<"/test4">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    ?assertMatch({ok, _}, O12),

    {ok, Handle11} = O11,
    {ok, Handle12} = O12,

    WriteAndTest =
        fun(Worker, Handle, Offset, Bytes) ->
            Size = size(Bytes),
            ?assertMatch({ok, Size}, lfm_proxy:write(Worker, Handle, Offset, Bytes)),
            for(Offset, Offset + Size - 1,
                fun(I) ->
                    for(1, Offset + Size - I,
                        fun(J) ->
                            SubBytes = binary:part(Bytes, I - Offset, J),
                            ?assertMatch({ok, SubBytes}, lfm_proxy:read(Worker, Handle, I, J))
                        end)
                end)
        end,

    WriteAndTest(W, Handle11, 0, <<"abc">>),
    WriteAndTest(W, Handle12, 0, <<"abc">>),

    WriteAndTest(W, Handle11, 3, <<"def">>),
    WriteAndTest(W, Handle12, 3, <<"def">>),

    WriteAndTest(W, Handle11, 2, <<"qwerty">>),
    WriteAndTest(W, Handle12, 2, <<"qwerty">>),

    WriteAndTest(W, Handle11, 8, <<"zxcvbnm">>),
    WriteAndTest(W, Handle12, 8, <<"zxcvbnm">>),

    WriteAndTest(W, Handle11, 6, <<"qwerty">>),
    WriteAndTest(W, Handle12, 6, <<"qwerty">>),

    WriteAndTest(W, Handle11, 10, crypto:rand_bytes(100)),
    WriteAndTest(W, Handle12, 10, crypto:rand_bytes(100)).

lfm_stat_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/test5">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/test5">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/test5">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId1, {path, <<"/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 3, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 6}}, lfm_proxy:stat(W, SessId1, {path, <<"/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 2, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 6}}, lfm_proxy:stat(W, SessId1, {path, <<"/test5">>}), 10),

    ?assertMatch({ok, 9}, lfm_proxy:write(W, Handle11, 1, <<"123456789">>)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/test5">>}), 10).

lfm_synch_stat_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/test5">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/test5">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/test5">>})),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 3}}}, lfm_proxy:write_and_check(W, Handle11, 0, <<"abc">>)),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(W, Handle11, 3, <<"abc">>)),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(W, Handle11, 2, <<"abc">>)),

    ?assertMatch({ok, 9, {ok, #file_attr{size = 10}}}, lfm_proxy:write_and_check(W, Handle11, 1, <<"123456789">>)).

lfm_truncate_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/test6">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/test6">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/test6">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId1, {path, <<"/test6">>}), 10),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/test6">>}, 1)),
    ?assertMatch({ok, #file_attr{size = 1}}, lfm_proxy:stat(W, SessId1, {path, <<"/test6">>}), 10),
    ?assertMatch({ok, <<"a">>}, lfm_proxy:read(W, Handle11, 0, 10)),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/test6">>}, 10)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/test6">>}), 10),
    ?assertMatch({ok, <<"a">>}, lfm_proxy:read(W, Handle11, 0, 1)),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 1, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/test6">>}), 10),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/test6">>}, 5)),
    ?assertMatch({ok, #file_attr{size = 5}}, lfm_proxy:stat(W, SessId1, {path, <<"/test6">>}), 10),
    ?assertMatch({ok, <<"aabc">>}, lfm_proxy:read(W, Handle11, 0, 4)).

lfm_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    [{GroupId1, _GroupName1} | _] = ?config({groups, 1}, Config),
    FileName = <<"/test_file_acl">>,
    DirName = <<"/test_dir_acl">>,

    {ok, FileUuid} = lfm_proxy:create(W, SessId1, FileName, 8#755),
    {ok, _} = lfm_proxy:mkdir(W, SessId1, DirName),

    % test setting and getting acl
    Acl = [
        #accesscontrolentity{acetype = ?allow_mask, identifier = UserId1, aceflags = ?no_flags_mask, acemask = ?read_mask bor ?write_mask},
        #accesscontrolentity{acetype = ?deny_mask, identifier = GroupId1, aceflags = ?identifier_group_mask, acemask = ?write_mask}
    ],
    Ans1 = lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, Acl),
    ?assertEqual(ok, Ans1),
    Ans2 = lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, Acl}, Ans2).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Get uuid of given by path file. Possible as root to bypass permissions checks.
get_uuid_privileged(Worker, SessId, Path) ->
    get_uuid(Worker, SessId, Path).

get_uuid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #file_attr{uuid = UUID}} = ?assertMatch(
        #fuse_response{status = #status{code = ?OK}},
        ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}),
        30
    ),
    UUID.

for(From, To, Fun) ->
    for(From, To, 1, Fun).
for(From, To, Step, Fun) ->
    [Fun(I) || I <- lists:seq(From, To, Step)].


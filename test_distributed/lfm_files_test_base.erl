%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains base functions for tests of lfm API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_test_base).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("lfm_files_test_base.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    fslogic_new_file/1,
    lfm_acl/1
]).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(file_req(W, SessId, ContextGuid, FileRequest), ?req(W, SessId,
    #file_request{context_guid = ContextGuid, file_request = FileRequest})).

%%%====================================================================
%%% Test function
%%%====================================================================

fslogic_new_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} =
        {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    RootUuid1 = get_guid_privileged(Worker, SessId1, <<"/space_name1">>),
    RootUuid2 = get_guid_privileged(Worker, SessId2, <<"/space_name2">>),

    Resp11 = ?file_req(Worker, SessId1, RootUuid1, #create_file{name = <<"test">>}),
    Resp21 = ?file_req(Worker, SessId2, RootUuid2, #create_file{name = <<"test">>}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_created{}}, Resp11),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_created{}}, Resp21),

    #fuse_response{fuse_response = #file_created{
        file_location = #file_location{
            file_id = FileId11,
            storage_id = StorageId11,
            provider_id = ProviderId11,
            storage_file_created = true
        }
    }} = Resp11,

    #fuse_response{fuse_response = #file_created{
        file_location = #file_location{
            file_id = FileId21,
            storage_id = StorageId21,
            provider_id = ProviderId21,
            storage_file_created = true
        }
    }} = Resp21,

    ?assertNotMatch(undefined, FileId11),
    ?assertNotMatch(undefined, FileId21),

    TestStorageId1 = initializer:get_supporting_storage_id(Worker, ?SPACE_ID1),
    TestStorageId2 = initializer:get_supporting_storage_id(Worker, ?SPACE_ID2),
    ?assertMatch(TestStorageId1, StorageId11),
    ?assertMatch(TestStorageId2, StorageId21),

    TestProviderId = rpc:call(Worker, oneprovider, get_id, []),
    ?assertMatch(TestProviderId, ProviderId11),
    ?assertMatch(TestProviderId, ProviderId21).

lfm_acl(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    UserName1 = ?config({user_name, <<"user1">>}, Config),
    [{GroupId1, GroupName1} | _] = ?config({groups, <<"user1">>}, Config),
    FileName = <<"/space_name2/test_file_acl">>,
    DirName = <<"/space_name2/test_dir_acl">>,

    {ok, FileGUID} = lfm_proxy:create(W, SessId1, FileName),
    {ok, _} = lfm_proxy:mkdir(W, SessId1, DirName),

    % test setting and getting acl
    Acl = [
        #access_control_entity{acetype = ?allow_mask, identifier = UserId1, name = UserName1, aceflags = ?no_flags_mask, acemask =
        ?read_all_object_mask bor ?write_all_object_mask},
        #access_control_entity{acetype = ?deny_mask, identifier = GroupId1, name = GroupName1, aceflags = ?identifier_group_mask, acemask = ?write_all_object_mask}
    ],
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, ?FILE_REF(FileGUID), Acl)),
    ?assertEqual({ok, Acl}, lfm_proxy:get_acl(W, SessId1, ?FILE_REF(FileGUID))).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_guid_privileged(Worker, SessId, Path) ->
    get_guid(Worker, SessId, Path).

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_auth_manager(NewConfig),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, ?MODULE]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    initializer:unmock_auth_manager(Config).


init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID1, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID2, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID3, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID4, 30),
    lfm_proxy:teardown(Config),
    lfm_ct:clear_context(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).
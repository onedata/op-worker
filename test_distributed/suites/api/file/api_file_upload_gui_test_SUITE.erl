%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file upload via gui API.
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_upload_gui_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").

-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    % parallel tests
    registering_upload_for_directory_should_fail_test/1,
    registering_upload_for_non_empty_file_should_fail_test/1,
    registering_upload_for_not_owned_file_should_fail_test/1,
    not_registered_upload_should_fail_test/1,
    upload_test/1,

    % sequential tests
    stale_upload_file_should_be_deleted_test/1,
    upload_with_time_warps_test/1
]).

% Exported for rpc calls
-export([upload_chunk/5]).

groups() -> [
    {parallel_tests, [parallel], [
        registering_upload_for_directory_should_fail_test,
        registering_upload_for_non_empty_file_should_fail_test,
        registering_upload_for_not_owned_file_should_fail_test,
        not_registered_upload_should_fail_test,
        upload_test
    ]},
    {time_mock_tests, [sequential], [
        stale_upload_file_should_be_deleted_test,
        upload_with_time_warps_test_test
    ]}
].

all() -> [
    {group, parallel_tests},
    {group, time_mock_tests}
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Test functions
%%%===================================================================


registering_upload_for_directory_should_fail_test(_Config) ->
    [#object{guid = DirGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #dir_spec{}
    ),
    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>, <<"not a regular file">>),
        initialize_gui_upload(krakow, user1, DirGuid)
    ).


registering_upload_for_non_empty_file_should_fail_test(_Config) ->
    [#object{guid = FileGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #file_spec{content = crypto:strong_rand_bytes(5)}
    ),
    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>, <<"file is not empty">>),
        initialize_gui_upload(krakow, user1, FileGuid)
    ).


registering_upload_for_not_owned_file_should_fail_test(_Config) ->
    [#object{guid = FileGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #file_spec{content = crypto:strong_rand_bytes(5)}
    ),
    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>, <<"file is not owned by user">>),
        initialize_gui_upload(krakow, user2, FileGuid)
    ).


not_registered_upload_should_fail_test(_Config) ->
    [#object{guid = FileGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #file_spec{content = crypto:strong_rand_bytes(5)}
    ),

    UserId = oct_background:get_user_id(user1),
    UserSessId = oct_background:get_user_session_id(user1, krakow),
    Node = oct_background:get_random_provider_node(krakow),

    ?assertMatch(
        upload_not_authorized,
        rpc:call(Node, page_file_upload, handle_multipart_req, [
            #{size => 20, left => 1},
            ?USER(UserId, UserSessId),
            #{
                <<"guid">> => FileGuid,
                <<"resumableChunkNumber">> => 1,
                <<"resumableChunkSize">> => 20
            }
        ])
    ).


upload_test(_Config) ->
    [#object{guid = FileGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #file_spec{}
    ),

    ?assertMatch({ok, _}, initialize_gui_upload(krakow, user1, FileGuid)),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    do_multipart(krakow, user1, FileGuid, 5, 10, 5),

    ?assertMatch({ok, _}, finalize_gui_upload(krakow, user1, FileGuid)),
    ?assertMatch(false, authorize_chunk_upload(krakow, user1, FileGuid), ?ATTEMPTS),

    assert_file_uploaded(krakow, user1, FileGuid, 250).


stale_upload_file_should_be_deleted_test(_Config) ->
    [#object{guid = FileGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #file_spec{}
    ),

    ?assertMatch({ok, _}, initialize_gui_upload(krakow, user1, FileGuid)),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    [StallProcess | StallProcesses] = spawn_stall_processes(krakow, user1, FileGuid),

    % upload should not be removed as long as there is at least one uploading process alive
    time_test_utils:simulate_seconds_passing(1000),
    force_stale_uploads_removal(krakow),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    lists:foreach(fun stop_stall_process/1, StallProcesses),
    time_test_utils:simulate_seconds_passing(1000),
    force_stale_uploads_removal(krakow),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    % and will be removed if no such process exists for longer than 1 minute
    stop_stall_process(StallProcess),
    time_test_utils:simulate_seconds_passing(59),
    force_stale_uploads_removal(krakow),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    time_test_utils:simulate_seconds_passing(2),
    force_stale_uploads_removal(krakow),
    ?assertMatch(false, authorize_chunk_upload(krakow, user1, FileGuid)),

    Node = oct_background:get_random_provider_node(krakow),
    UserSessId = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Node, UserSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS).


upload_with_time_warps_test(_Config) ->
    [#object{guid = FileGuid}] = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, #file_spec{}
    ),

    ?assertMatch({ok, _}, initialize_gui_upload(krakow, user1, FileGuid)),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    % upload should not be canceled if time warps backward (whether write occurred or not)
    time_test_utils:simulate_seconds_passing(-1000),
    force_stale_uploads_removal(krakow),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    % upload should not be canceled in case of forward time warp if next chunk
    % was written before stale uploads checkup
    time_test_utils:simulate_seconds_passing(3000),
    do_multipart(krakow, user1, FileGuid, 5, 10, 5),
    force_stale_uploads_removal(krakow),
    ?assertMatch(true, authorize_chunk_upload(krakow, user1, FileGuid)),

    % otherwise it will be removed
    time_test_utils:simulate_seconds_passing(2000),
    force_stale_uploads_removal(krakow),
    ?assertMatch(false, authorize_chunk_upload(krakow, user1, FileGuid)),

    Node = oct_background:get_random_provider_node(krakow),
    UserSessId = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Node, UserSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op-2nodes",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(time_mock_tests = Group, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_group(?DEFAULT_CASE(Group), Config);

init_per_group(_Group, Config) ->
    mock_cowboy_multipart(krakow),
    lfm_proxy:init(Config, false).


end_per_group(time_mock_tests = Group, Config) ->
    time_test_utils:unfreeze_time(Config),
    end_per_group(?DEFAULT_CASE(Group), Config);

end_per_group(_Group, Config) ->
    unmock_cowboy_multipart(krakow),
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec initialize_gui_upload(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid()
) ->
    {ok, term()} | errors:error().
initialize_gui_upload(ProviderSelector, UserSelector, FileGuid) ->
    UserId = oct_background:get_user_id(UserSelector),
    UserSessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    Node = oct_background:get_random_provider_node(ProviderSelector),

    rpc:call(Node, gs_rpc, handle, [
        ?USER(UserId, UserSessId), <<"initializeFileUpload">>, #{<<"guid">> => FileGuid}
    ]).


%% @private
-spec finalize_gui_upload(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid()
) ->
    {ok, term()} | errors:error().
finalize_gui_upload(ProviderSelector, UserSelector, FileGuid) ->
    UserId = oct_background:get_user_id(UserSelector),
    UserSessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    Node = oct_background:get_random_provider_node(ProviderSelector),

    rpc:call(Node, gs_rpc, handle, [
        ?USER(UserId, UserSessId), <<"finalizeFileUpload">>, #{<<"guid">> => FileGuid}
    ]).


%% @private
-spec authorize_chunk_upload(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid()
) ->
    boolean().
authorize_chunk_upload(ProviderSelector, UserSelector, FileGuid) ->
    UserId = oct_background:get_user_id(UserSelector),
    Node = oct_background:get_random_provider_node(ProviderSelector),

    rpc:call(Node, file_upload_manager, authorize_chunk_upload, [UserId, FileGuid]).


%% @private
-spec spawn_stall_processes(
    [node()] | oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid()
) ->
    [pid()].
spawn_stall_processes(Nodes, UserSelector, FileGuid) when is_list(Nodes) ->
    UserId = oct_background:get_user_id(UserSelector),
    TestProcessPid = self(),

    StallProcessDef = fun() ->
        file_upload_manager:authorize_chunk_upload(UserId, FileGuid),
        receive
            stop -> TestProcessPid ! ok
        end
    end,

    lists:map(fun(Node) -> spawn(Node, StallProcessDef) end, Nodes);

spawn_stall_processes(ProviderSelector, UserSelector, FileGuid) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    spawn_stall_processes(Nodes, UserSelector, FileGuid).


%% @private
-spec stop_stall_process(pid()) -> ok.
stop_stall_process(Pid) ->
    Pid ! stop,
    receive ok -> ok end,
    % Await some time so that file_upload_manager has time to deregister dead process
    timer:sleep(timer:seconds(2)).


%% @private
-spec force_stale_uploads_removal(oct_background:entity_selector()) -> ok.
force_stale_uploads_removal(ProviderSelector) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    {file_upload_manager, Node} ! check_uploads,  %% TODO fix
    ok.


%% @private
-spec do_multipart(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid(),
    integer(),
    integer(),
    integer()
) ->
    ok.
do_multipart(ProviderSelector, UserSelector, FileGuid, PartsNumber, PartSize, ChunksNumber) ->
    UserId = oct_background:get_user_id(UserSelector),
    UserSessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    Auth = ?USER(UserId, UserSessId),

    ?assertMatch(ok, lists_utils:pforeach(fun(ChunkNo) ->
        Node = oct_background:get_random_provider_node(ProviderSelector),
        rpc:call(Node, ?MODULE, upload_chunk, [Auth, FileGuid, PartsNumber, PartSize, ChunkNo])
    end, lists:seq(1, ChunksNumber))).


-spec upload_chunk(aai:auth(), file_id:file_guid(), integer(), integer(), integer()) ->
    ok.
upload_chunk(?USER(UserId, _) = Auth, FileGuid, PartsNumber, PartSize, ChunkNo) ->
    true = file_upload_manager:authorize_chunk_upload(UserId, FileGuid),

    Req = #{size => PartSize, left => PartsNumber},
    Params = #{
        <<"resumableChunkSize">> => integer_to_binary(PartsNumber * PartSize),
        <<"guid">> => FileGuid,
        <<"resumableChunkNumber">> => integer_to_binary(ChunkNo)
    },
    page_file_upload:handle_multipart_req(Req, Auth, Params),

    ok.


%% @private
-spec assert_file_uploaded(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid(),
    integer()
) ->
    boolean().
assert_file_uploaded(ProviderSelector, UserSelector, FileGuid, ExpSize) ->
    UserSessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    Node = oct_background:get_random_provider_node(ProviderSelector),

    ?assertMatch(
        {ok, #file_attr{size = ExpSize}},
        lfm_proxy:stat(Node, UserSessId, ?FILE_REF(FileGuid)),
        ?ATTEMPTS
    ),
    {ok, FileHandle} = lfm_proxy:open(Node, UserSessId, ?FILE_REF(FileGuid), read),
    {ok, Data} = ?assertMatch({ok, _}, lfm_proxy:read(Node, FileHandle, 0, ExpSize)),
    ?assert(lists:all(fun(X) -> X == true end, [$a == Char || <<Char>> <= Data])),
    lfm_proxy:close(Node, FileHandle).


%% @private
-spec mock_cowboy_multipart(oct_background:entity_selector()) -> ok.
mock_cowboy_multipart(ProviderPlaceholder) ->
    Nodes = oct_background:get_provider_nodes(ProviderPlaceholder),
    ok = test_utils:mock_new(Nodes, cow_multipart),
    ok = test_utils:mock_new(Nodes, cowboy_req),
    ok = test_utils:mock_expect(Nodes, cow_multipart, form_data,
        fun(_) -> {file, ok, ok, ok} end
    ),
    ok = test_utils:mock_expect(Nodes, cowboy_req, read_part,
        fun
            (#{done := true} = Req) ->
                {done, Req};
            (Req) ->
                {ok, [], Req}
        end
    ),
    ok = test_utils:mock_expect(Nodes, cowboy_req, read_part_body,
        fun
            (#{left := 1, size := Size} = Req, _) ->
                {ok, <<<<$a>> || _ <- lists:seq(1, Size)>>, Req#{done => true}};
            (#{left := Left, size := Size} = Req, _) ->
                {more, <<<<$a>> || _ <- lists:seq(1, Size)>>, Req#{left => Left - 1}}
        end
    ).


%% @private
-spec unmock_cowboy_multipart(oct_background:entity_selector()) -> ok.
unmock_cowboy_multipart(ProviderPlaceholder) ->
    Nodes = oct_background:get_provider_nodes(ProviderPlaceholder),
    test_utils:mock_unload(Nodes, cowboy_req),
    test_utils:mock_unload(Nodes, cow_multipart).

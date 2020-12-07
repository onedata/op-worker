%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% GUI file upload tests
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_upload_via_gui_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    registering_upload_for_directory_should_fail/1,
    registering_upload_for_non_empty_file_should_fail/1,
    registering_upload_for_not_owned_file_should_fail/1,
    not_registered_upload_should_fail/1,
    upload_test/1,
    stale_upload_file_should_be_deleted/1,
    finished_upload_file_should_be_left_intact/1,
    upload_with_time_warps_test/1
]).

all() -> [
    registering_upload_for_directory_should_fail,
    registering_upload_for_non_empty_file_should_fail,
    registering_upload_for_not_owned_file_should_fail,
    not_registered_upload_should_fail,
    upload_test,
    stale_upload_file_should_be_deleted,
    finished_upload_file_should_be_left_intact,
    upload_with_time_warps_test
].

-define(SPACE_ID, <<"space1">>).
-define(FILE_PATH, <<"/", ?SPACE_ID/binary, "/", (atom_to_binary(?FUNCTION_NAME, utf8))/binary>>).

-define(ATTEMPTS, 60).


%%%===================================================================
%%% Test functions
%%%===================================================================


registering_upload_for_directory_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, ?FILE_PATH),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>),
        initialize_upload(UserId, DirGuid, WorkerP1, Config)
    ).


registering_upload_for_non_empty_file_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    ?assertMatch({ok, _}, lfm_proxy:write(WorkerP1, FileHandle, 0, crypto:strong_rand_bytes(5))),
    lfm_proxy:fsync(WorkerP1, FileHandle),
    lfm_proxy:close(WorkerP1, FileHandle),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>),
        initialize_upload(UserId, FileGuid, WorkerP1, Config)
    ).


registering_upload_for_not_owned_file_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    User1Id = <<"user1">>,

    User2Id = <<"user2">>,
    Session2Id = ?config({session_id, {User2Id, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, Session2Id, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>),
        initialize_upload(User1Id, FileGuid, WorkerP1, Config)
    ).


not_registered_upload_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    ?assertMatch(
        upload_not_registered,
        rpc:call(WorkerP1, page_file_upload, handle_multipart_req, [
            #{size => 20, left => 1},
            ?USER(UserId, SessionId),
            #{
                <<"guid">> => Guid,
                <<"resumableChunkNumber">> => 1,
                <<"resumableChunkSize">> => 20
            }
        ])
    ).


upload_test(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})),

    ?assertMatch({ok, _}, initialize_upload(UserId, FileGuid, WorkerP1, Config)),
    ?assertMatch(true, is_upload_registered(UserId, FileGuid, WorkerP1)),

    do_multipart(WorkerP1, ?USER(UserId, SessionId), 5, 10, 5, FileGuid),

    ?assertMatch({ok, _}, finalize_upload(UserId, FileGuid, WorkerP1, Config)),
    ?assertMatch(false, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),

    ?assertMatch(
        {ok, #file_attr{size = 250}},
        lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid}),
        ?ATTEMPTS
    ),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, read),
    {ok, Data} = ?assertMatch({ok, _}, lfm_proxy:read(WorkerP1, FileHandle, 0, 250)),
    ?assert(lists:all(fun(X) -> X == true end, [$a == Char || <<Char>> <= Data])),
    lfm_proxy:close(WorkerP1, FileHandle).


stale_upload_file_should_be_deleted(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})),

    ?assertMatch({ok, _}, initialize_upload(UserId, FileGuid, WorkerP1, Config)),
    ?assertMatch(true, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),

    % file being uploaded shouldn't be deleted after only 30s of inactivity
    timer:sleep(timer:seconds(30)),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})),
    ?assertMatch(true, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),

    % but if upload is not resumed or finished before INACTIVITY_PERIOD then file should be deleted
    timer:sleep(timer:seconds(100)),
    ?assertMatch(false, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid}), ?ATTEMPTS).


finished_upload_file_should_be_left_intact(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})),

    ?assertMatch({ok, _}, initialize_upload(UserId, FileGuid, WorkerP1, Config)),
    ?assertMatch(true, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),

    % file being uploaded shouldn't be deleted after only 30s of inactivity
    timer:sleep(timer:seconds(30)),
    ?assertMatch(true, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})),

    % uploaded files shouldn't be deleted automatically after INACTIVITY_PERIOD
    ?assertMatch({ok, _}, finalize_upload(UserId, FileGuid, WorkerP1, Config)),
    ?assertMatch(false, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),

    timer:sleep(timer:seconds(100)),

    ?assertMatch(false, is_upload_registered(UserId, FileGuid, WorkerP1), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})).


upload_with_time_warps_test(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    FileKey = {guid, FileGuid},
    CurrTime = time_test_utils:get_frozen_time_seconds(),
    ?assertMatch({ok, #file_attr{mtime = CurrTime}}, lfm_proxy:stat(WorkerP1, SessionId, FileKey)),
    ?assertMatch({ok, _}, initialize_upload(UserId, FileGuid, WorkerP1, Config)),

    AssertUploadStatusAfterStaleUploadsRemoval = fun(ExpMTime, ExpStatus) ->
        ?assertMatch(
            {ok, #file_attr{mtime = ExpMTime}},
            lfm_proxy:stat(WorkerP1, SessionId, FileKey),
            ?ATTEMPTS
        ),
        force_stale_uploads_removal(WorkerP1),
        case ExpStatus of
            ongoing ->
                ?assertMatch(true, is_upload_registered(UserId, FileGuid, WorkerP1)),
                ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, FileKey));
            removed ->
                ?assertMatch(false, is_upload_registered(UserId, FileGuid, WorkerP1)),
                ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP1, SessionId, FileKey))
        end
    end,
    AssertUploadStatusAfterStaleUploadsRemoval(CurrTime, ongoing),

    % upload should not be canceled if time warps backward (whether write occurred or not)
    time_test_utils:simulate_seconds_passing(-1000),
    PastTime = CurrTime - 1000,

    AssertUploadStatusAfterStaleUploadsRemoval(CurrTime, ongoing),
    do_multipart(WorkerP1, ?USER(UserId, SessionId), 5, 10, 1, FileGuid),
    AssertUploadStatusAfterStaleUploadsRemoval(PastTime, ongoing),

    % in case of forward time warp if next chunk was written to file (this updates file mtime)
    % it should be left. Otherwise it will be deleted as obsolete upload.
    time_test_utils:simulate_seconds_passing(3000),
    FutureTime = PastTime + 3000,

    do_multipart(WorkerP1, ?USER(UserId, SessionId), 5, 10, 1, FileGuid),
    AssertUploadStatusAfterStaleUploadsRemoval(FutureTime, ongoing),

    time_test_utils:simulate_seconds_passing(2000),
    AssertUploadStatusAfterStaleUploadsRemoval(FutureTime, removed).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config).


init_per_testcase(Case, Config) when
    Case =:= not_registered_upload_should_fail;
    Case =:= upload_test
->
    mock_cowboy_multipart(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(upload_with_time_warps_test = Case, Config) ->
    mock_cowboy_multipart(Config),
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"),
        Config
    ),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(Case, Config) when
    Case =:= not_registered_upload_should_fail;
    Case =:= upload_test
->
    unmock_cowboy_multipart(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(upload_with_time_warps_test = Case, Config) ->
    unmock_cowboy_multipart(Config),
    time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),

    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),

    lists:foreach(fun({SpaceId, _}) ->
        rpc:multicall(Workers, space_quota, delete, [SpaceId])
    end, ?config(spaces, Config)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mock_cowboy_multipart(test_config:config()) -> ok.
mock_cowboy_multipart(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, cow_multipart),
    ok = test_utils:mock_new(Workers, cowboy_req),
    ok = test_utils:mock_expect(Workers, cow_multipart, form_data,
        fun(_) -> {file, ok, ok, ok} end
    ),
    ok = test_utils:mock_expect(Workers, cowboy_req, read_part,
        fun
            (#{done := true} = Req) ->
                {done, Req};
            (Req) ->
                {ok, [], Req}
        end
    ),
    ok = test_utils:mock_expect(Workers, cowboy_req, read_part_body,
        fun
            (#{left := 1, size := Size} = Req, _) ->
                {ok, << <<$a>> || _ <- lists:seq(1, Size) >>, Req#{done => true}};
            (#{left := Left, size := Size} = Req, _) ->
                {more, << <<$a>> || _ <- lists:seq(1, Size) >>, Req#{left => Left-1}}
        end
    ).


%% @private
-spec unmock_cowboy_multipart(test_config:config()) -> ok.
unmock_cowboy_multipart(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, cowboy_req),
    test_utils:mock_unload(Workers, cow_multipart).


%% @private
-spec initialize_upload(od_user:id(), file_id:file_guid(), node(), test_config:config()) ->
    {ok, term()} | errors:error().
initialize_upload(UserId, FileGuid, Worker, Config) ->
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(Worker)}}, Config),

    rpc:call(Worker, gs_rpc, handle, [
        ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => FileGuid}
    ]).


%% @private
-spec finalize_upload(od_user:id(), file_id:file_guid(), node(), test_config:config()) ->
    {ok, term()} | errors:error().
finalize_upload(UserId, FileGuid, Worker, Config) ->
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(Worker)}}, Config),

    rpc:call(Worker, gs_rpc, handle, [
        ?USER(UserId, SessionId), <<"finalizeFileUpload">>, #{<<"guid">> => FileGuid}
    ]).


%% @private
-spec is_upload_registered(od_user:id(), file_id:file_guid(), node()) -> boolean().
is_upload_registered(UserId, FileGuid, Worker) ->
    rpc:call(Worker, file_upload_manager, is_upload_registered, [UserId, FileGuid]).


%% @private
-spec force_stale_uploads_removal(node()) -> ok.
force_stale_uploads_removal(Worker) ->
    {file_upload_manager, Worker} ! check_uploads,
    ok.


%% @private
-spec do_multipart(node(), aai:auth(), integer(), integer(), integer(), file_id:file_guid()) ->
    ok.
do_multipart(Worker, Auth, PartsNumber, PartSize, ChunksNumber, FileGuid) ->
    Params = #{
        <<"guid">> => FileGuid,
        <<"resumableChunkSize">> => integer_to_binary(PartsNumber*PartSize)
    },
    lists_utils:pforeach(fun(Chunk) ->
        rpc:call(Worker, page_file_upload, handle_multipart_req, [
            #{size => PartSize, left => PartsNumber},
            Auth,
            Params#{<<"resumableChunkNumber">> => integer_to_binary(Chunk)}
        ])
    end, lists:seq(1, ChunksNumber)).

%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% File upload tests
%%% @end
%%%-------------------------------------------------------------------
-module(file_upload_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

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
    finished_upload_file_should_be_left_intact/1
]).

all() ->
    ?ALL([
        registering_upload_for_directory_should_fail,
        registering_upload_for_non_empty_file_should_fail,
        registering_upload_for_not_owned_file_should_fail,
        not_registered_upload_should_fail,
        upload_test,
        stale_upload_file_should_be_deleted,
        finished_upload_file_should_be_left_intact
    ]).

-define(ATTEMPTS, 60).
-define(DEFAULT_FILE_MODE, 8#664).

-define(SPACE_ID, <<"space1">>).
-define(FILE_PATH, <<"/", ?SPACE_ID/binary, "/", (atom_to_binary(?FUNCTION_NAME, utf8))/binary>>).

%%%===================================================================
%%% Test functions
%%%===================================================================


registering_upload_for_directory_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, ?FILE_PATH),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>),
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ).


registering_upload_for_non_empty_file_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, Guid}, write),
    ?assertMatch({ok, _}, lfm_proxy:write(WorkerP1, FileHandle, 0, crypto:strong_rand_bytes(5))),
    lfm_proxy:fsync(WorkerP1, FileHandle),
    lfm_proxy:close(WorkerP1, FileHandle),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>),
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ).


registering_upload_for_not_owned_file_should_fail(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    User1Id = <<"user1">>,
    Session1Id = ?config({session_id, {User1Id, ?GET_DOMAIN(WorkerP1)}}, Config),
    User2Id = <<"user2">>,
    Session2Id = ?config({session_id, {User2Id, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, Guid} = lfm_proxy:create(WorkerP1, Session2Id, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>),
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(User1Id, Session1Id), <<"initializeFileUpload">>, #{<<"guid">> => Guid}
        ])
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
            SessionId,
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

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid})),

    ?assertMatch(
        {ok, _},
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ),
    ?assertMatch(true, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid])),

    do_multipart(WorkerP1, SessionId, 5, 10, 5, Guid),

    ?assertMatch(
        {ok, _},
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"finalizeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ),
    ?assertMatch(false, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid]), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = 250}}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid}), ?ATTEMPTS),

    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, Guid}, read),
    {ok, Data} = ?assertMatch({ok, _}, lfm_proxy:read(WorkerP1, FileHandle, 0, 250)),
    ?assert(lists:all(fun(X) -> X == true end, [$a == Char || <<Char>> <= Data])),
    lfm_proxy:close(WorkerP1, FileHandle).


stale_upload_file_should_be_deleted(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid})),

    ?assertMatch(
        {ok, _},
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ),
    ?assertMatch(true, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid])),

    % file shouldn't be deleted after only 30s of inactivity
    timer:sleep(timer:seconds(30)),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid})),
    ?assertMatch(true, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid])),

    % but after 1min (INACTIVITY_PERIOD) not finalized stale upload files should be deleted
    timer:sleep(timer:seconds(100)),
    ?assertMatch(false, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid]), ?ATTEMPTS),
    ?assertMatch({error, enoent}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid}), ?ATTEMPTS).


finished_upload_file_should_be_left_intact(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid})),

    ?assertMatch(
        {ok, _},
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ),
    ?assertMatch(true, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid])),

    % file shouldn't be deleted after only 30s of inactivity
    timer:sleep(timer:seconds(30)),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid})),
    ?assertMatch(true, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid])),

    ?assertMatch(
        {ok, _},
        rpc:call(WorkerP1, op_rpc, handle, [
            ?USER(UserId, SessionId), <<"finalizeFileUpload">>, #{<<"guid">> => Guid}
        ])
    ),
    ?assertMatch(false, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid]), ?ATTEMPTS),

    % finalized upload files shouldn't be deleted automatically after INACTIVITY_PERIOD
    timer:sleep(timer:seconds(100)),
    ?assertMatch(false, rpc:call(WorkerP1, file_upload_manager, is_upload_registered, [UserId, Guid])),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessionId, {guid, Guid})).


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
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, cow_multipart),
    ok = test_utils:mock_new(Workers, cowboy_req),
    ok = test_utils:mock_new(Workers, op_gui_session),
    ok = test_utils:mock_expect(Workers, op_gui_session, get_user_id,
        fun() -> <<"user1">> end
    ),
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
    ),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(Case, Config) when
    Case =:= not_registered_upload_should_fail;
    Case =:= upload_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, cowboy_req),
    test_utils:mock_unload(Workers, cow_multipart),
    test_utils:mock_unload(Workers, op_gui_session),
    end_per_testcase(all, Config);

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


do_multipart(Worker, SessionId, PartsNumber, PartSize, ChunksNumber, FileGuid) ->
    ChunkSize = PartsNumber*PartSize,
    Params = #{<<"guid">> => FileGuid, <<"resumableChunkSize">> => integer_to_binary(ChunkSize)},

    utils:pforeach(fun(Chunk) ->
        rpc:call(Worker, page_file_upload, handle_multipart_req, [
            #{size => PartSize, left => PartsNumber},
            SessionId,
            Params#{<<"resumableChunkNumber">> => integer_to_binary(Chunk)}
        ])
    end, lists:seq(1,ChunksNumber)).

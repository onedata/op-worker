%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains multiprovider tests of qos.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_multiprovider_test_SUITE).
-author("Michal Stanisz").

-include("transfers_test_mechanism.hrl").

-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

%% API
-export([
    qos_restoration_file_test/1,
    qos_restoration_dir_test/1,
    qos_status_test/1
]).

all() -> [
    qos_restoration_file_test,
    qos_restoration_dir_test,
    qos_status_test
].

-define(SPACE_ID, <<"space1">>).
-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

% qos for test providers
-define(TEST_QOS(Val), #{
    <<"country">> => Val
}).

-define(Q1, <<"q1">>).
-define(TEST_DATA, <<"test_data">>).

-define(simple_dir_structure(Name, Distribution),
    {?SPACE_ID, [
        {Name, ?TEST_DATA, Distribution}
    ]}
).

%%%===================================================================
%%% API
%%%===================================================================

qos_restoration_file_test(Config) ->
    basic_qos_restoration_test_base(Config, simple).


qos_restoration_dir_test(Config) ->
    basic_qos_restoration_test_base(Config, nested).


qos_status_test(Config) ->
    [_Worker2, Worker1] = Workers = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,

    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(Config, nested, Filename),
    {GuidsAndPaths, QosDesc} = qos_test_utils:add_qos_test_base(Config, QosSpec#{perform_checks => false}),
    QosList = [QosId || {_, QosId, _} <- QosDesc],

    % this is needed so only traverse transfers will start
    mock_qos_restore(Config),

    lists:foreach(fun({Guid, Path}) ->
        lists:foreach(fun(Worker) ->
            ?assertEqual(false, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, Guid}), ?ATTEMPTS),
            ?assertEqual(false, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, Path}), ?ATTEMPTS)
        end, Workers)
    end, maps:get(files, GuidsAndPaths)),

    finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),

    lists:foreach(fun({Guid, Path}) ->
        lists:foreach(fun(Worker) ->
            ?assertEqual(true, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, Guid}), ?ATTEMPTS),
            ?assertEqual(true, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, Path}), ?ATTEMPTS)
        end, Workers)
    end, maps:get(files, GuidsAndPaths)),

    % check status after restoration
    unmock_qos_restore(Config),
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),

    IsAncestor = fun(A, F) ->
        str_utils:binary_starts_with(F, A)
    end,

    lists:foreach(fun({FileGuid, FilePath}) ->
        ct:print("writing to file ~p", [FilePath]),
        {ok, FileHandle} = lfm_proxy:open(Worker1, SessId(Worker1), {guid, FileGuid}, write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker1, FileHandle),
        lists:foreach(fun(Worker) ->
            lists:foreach(fun({G, P}) ->
                ct:print("Checking ~p: ~n\tfile: ~p~n\tis_ancestor: ~p", [Worker, P, IsAncestor(P, FilePath)]),
                ?assertEqual(not IsAncestor(P, FilePath), lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, G}), ?ATTEMPTS),
                ?assertEqual(not IsAncestor(P, FilePath), lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, P}), ?ATTEMPTS)
            end, FilesAndDirs)
        end, Workers),
        finish_transfers([FileGuid]),
        lists:foreach(fun(Worker) ->
            lists:foreach(fun({G, P}) ->
                ?assertEqual(true, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, G}), ?ATTEMPTS),
                ?assertEqual(true, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, P}), ?ATTEMPTS)
            end, FilesAndDirs)
        end, Workers)
    end, maps:get(files, GuidsAndPaths)).


%%%===================================================================
%%% Tests base
%%%===================================================================

basic_qos_restoration_test_base(Config, DirStructureType) ->
    [Worker2, Worker1] = ?config(op_worker_nodes, Config),
    SessionId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(Config, DirStructureType, Filename),
    {GuidsAndPaths, _} = qos_test_utils:add_qos_test_base(Config, QosSpec),
    NewData = <<"new_test_data">>,
    StoragePaths = lists:map(fun({Guid, Path}) ->
        SpaceId = file_id:guid_to_space_id(Guid),

        % remove leading slash and space id
        [_, _ | PathTokens] = binary:split(Path, <<"/">>, [global]),
        StoragePath = storage_file_path(Worker2, SpaceId, filename:join(PathTokens)),

        ?assertEqual({ok, ?TEST_DATA}, read_file(Worker2, StoragePath)),

        {ok, FileHandle} = lfm_proxy:open(Worker1, SessionId1, {guid, Guid}, write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, NewData),
        ok = lfm_proxy:close(Worker1, FileHandle),
        StoragePath

    end, maps:get(files, GuidsAndPaths)),

    lists:foreach(fun(StoragePath) ->
        ?assertEqual({ok, NewData}, read_file(Worker2, StoragePath), ?ATTEMPTS)
    end, StoragePaths).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2),
        Workers = ?config(op_worker_nodes, NewConfig3),
        rpc:multicall(Workers, fslogic_worker, schedule_init_qos_cache_for_all_spaces, []),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, qos_test_utils, ?MODULE]}
        | Config
    ].

init_per_testcase(qos_status_test, Config) ->
    mock_schedule_transfers(Config),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    [Worker2, Worker1] = ?config(op_worker_nodes, Config),
    Mock = #{
        ?GET_DOMAIN_BIN(Worker1) => ?TEST_QOS(<<"PL">>),
        ?GET_DOMAIN_BIN(Worker2) => ?TEST_QOS(<<"PT">>)
    },
    qos_test_utils:mock_providers_qos(Config, Mock),
    case ?config(?SPACE_ID_KEY, Config) of
        undefined ->
            [{?SPACE_ID_KEY, ?SPACE_ID} | Config];
        _ ->
            Config
    end.

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_replication_worker(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:ensure_transfers_removed(Config),
    test_utils:mock_unload(Workers, providers_qos).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(filename(Name, Num), <<Name/binary,(integer_to_binary(Num))/binary>>).
-define(nested_dir_structure(Name, Distribution),
    {?SPACE_ID, [
        {Name, [
            {?filename(Name, 1), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution},
                {?filename(Name, 3), ?TEST_DATA, Distribution},
                {?filename(Name, 4), ?TEST_DATA, Distribution}
            ]},
            {?filename(Name, 2), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution},
                {?filename(Name, 3), ?TEST_DATA, Distribution},
                {?filename(Name, 4), ?TEST_DATA, Distribution}
            ]}
        ]}
    ]}
).

create_basic_qos_test_spec(Config, DirStructureType, QosFilename) ->
    Workers = [Worker2, Worker1 | _] = ?config(op_worker_nodes, Config),
    {DirStructure, DirStructureAfter} = case DirStructureType of
        simple ->
            {?simple_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1)]),
            ?simple_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(Worker2)])};
        nested ->
            {?nested_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1)]),
            ?nested_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(Worker2)])}
    end,

    #{
        source_provider => Worker1,
        directory_structure_before => DirStructure,
        assertion_workers => Workers,
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(QosFilename),
                expression => <<"country=PT">>
            }
        ],
        directory_structure_after => DirStructureAfter
    }.

read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).

storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    StorageId = get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).

get_supporting_storage_id(Worker, SpaceId) ->
    [StorageId] = rpc:call(Worker, space_storage, get_storage_ids, [SpaceId]),
    StorageId.

storage_mount_point(Worker, StorageId) ->
    [Helper | _] = rpc:call(Worker, storage, get_helpers, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

mock_qos_restore(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_hooks, [passthrough]),
    test_utils:mock_expect(Workers, qos_hooks, maybe_update_file_on_storage, fun(_,_) -> ok end).

unmock_qos_restore(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, qos_hooks).

mock_schedule_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_traverse, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(Workers, qos_traverse, synchronize_file,
        fun(_,FileCtx,_) ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            TestPid ! {qos_slave_job, self(), FileGuid},
            receive {completed, FileGuid} -> ok end
        end).

finish_transfers([]) -> ok;
finish_transfers(Files) ->
    receive {qos_slave_job, Pid, FileGuid} ->
        Pid ! {completed, FileGuid},
        finish_transfers(lists:delete(FileGuid, Files))
    end.

%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains base test of replication.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_transfers_test_base).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("transfers_test_mechanism.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

% TODO VFS-5617
%% API
-export([
    file_replication_failures_should_fail_whole_transfer/3,
    rtransfer_works_between_providers_with_different_ports/2,
    regular_file_deleted_locally_during_replication/3,
    regular_file_deleted_remotely_during_replication/3,
    file_deleted_during_directory_replication/3
]).

%% helpers invoked on worker nodes via rpc:call
-export([local_location_deleted/1]).

-define(SPACE_ID, <<"space1">>).
-define(SYNC_BLOCK_REACHED, sync_block_reached).

%%%===================================================================
%%% API
%%%===================================================================

file_replication_failures_should_fail_whole_transfer(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 60,
                timeout = timer:minutes(1)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 100,
                    files_processed => 100,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                assertion_nodes = [WorkerP2],
                attempts = 120,
                timeout = timer:minutes(10)
            }
        }
    ).

rtransfer_works_between_providers_with_different_ports(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    % Replication from p1 to p2
    TransferTestSpec = #transfer_test_spec{
        setup = Setup = #setup{
            setup_node = WorkerP1,
            assertion_nodes = [WorkerP2],
            files_structure = [{0, 1}],
            distribution = [
                #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
            ]
        },
        scenario = Scenario = #scenario{
            type = Type,
            schedule_node = WorkerP1,
            replicating_nodes = [WorkerP2],
            function = fun transfers_test_mechanism:replicate_each_file_separately/2
        },
        expected = #expected{
            expected_transfer = #{
                replication_status => completed,
                files_to_process => 1,
                files_processed => 1,
                files_replicated => 1,
                bytes_replicated => ?DEFAULT_SIZE
            },
            distribution = [
                #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
            ],
            assertion_nodes = [WorkerP1, WorkerP2]
        }
    },

    Config1 = transfers_test_mechanism:run_test(Config, TransferTestSpec),
    Config2 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config1),

    % Replication from p2 to p1
    TransferTestSpec2 = TransferTestSpec#transfer_test_spec{
        setup = Setup#setup{
            setup_node = WorkerP2,
            root_directory = <<"root_dir">>,
            assertion_nodes = [WorkerP1],
            distribution = [
                #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
            ]
        },
        scenario = Scenario#scenario{
            schedule_node = WorkerP2,
            replicating_nodes = [WorkerP1]
        }
    },
    transfers_test_mechanism:run_test(Config2, TransferTestSpec2).


regular_file_deleted_locally_during_replication(Config, _Type, _FileKeyType) ->
    deleted_during_replication_test_base(Config, local).

regular_file_deleted_remotely_during_replication(Config, _Type, _FileKeyType) ->
    deleted_during_replication_test_base(Config, remote).

%% @private
deleted_during_replication_test_base(Config, DeleteMode) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessP1 = ?DEFAULT_SESSION(WorkerP1, Config),
    SessP2 = ?DEFAULT_SESSION(WorkerP2, Config),
    ProviderId2 = transfers_test_utils:provider_id(WorkerP2),

    DeleteModeBin = atom_to_binary(DeleteMode, utf8),
    FilePath = <<"/", (?SPACE_ID)/binary, "/deleted_during_replication_", DeleteModeBin/binary>>,
    Guid = create_file_with_content(WorkerP1, SessP1, FilePath),

    % file (and its remote location) visible on P2
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP2, SessP2, ?FILE_REF(Guid)), ?ATTEMPTS),
    % give P2 a local file_location so the later delete leaves a *soft-deleted* one (doc is saved with deleted=true)
    ensure_local_location_created(WorkerP2, SessP2, Guid),

    Master = self(),
    Uuid = file_id:guid_to_uuid(Guid),
    install_synchronize_block_blocker(WorkerP2, Uuid, Master),

    {ok, Tid} = ?assertMatch({ok, _},
        opt_transfers:schedule_file_replication(WorkerP1, SessP1, ?FILE_REF(Guid), ProviderId2)),

    WorkerPid = await_synchronize_block_reached(),

    % delete while synchronize_block is blocked
    case DeleteMode of
        local -> ?assertEqual(ok, lfm_proxy:unlink(WorkerP2, SessP2, ?FILE_REF(Guid)));
        remote -> ?assertEqual(ok, lfm_proxy:unlink(WorkerP1, SessP1, ?FILE_REF(Guid)))
    end,
    % gate proceed on the local location being actually soft-deleted (deterministic race)
    await_local_location_deleted(WorkerP2, Guid),

    WorkerPid ! proceed,

    transfers_test_utils:assert_transfer_state(WorkerP2, Tid, #{
        replication_status => completed,
        scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
        files_to_process => 1,
        files_processed => 1,
        failed_files => 0,
        files_replicated => 0,
        bytes_replicated => 0
    }, ?ATTEMPTS).

file_deleted_during_directory_replication(Config, _Type, _FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessP1 = ?DEFAULT_SESSION(WorkerP1, Config),
    SessP2 = ?DEFAULT_SESSION(WorkerP2, Config),
    ProviderId1 = transfers_test_utils:provider_id(WorkerP1),
    ProviderId2 = transfers_test_utils:provider_id(WorkerP2),

    DirPath = <<"/", (?SPACE_ID)/binary, "/dir_del_repl">>,
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerP1, SessP1, DirPath)),
    Children = [
        create_file_with_content(WorkerP1, SessP1,
            <<DirPath/binary, "/f", (integer_to_binary(N))/binary>>)
        || N <- lists:seq(1, 3)
    ],
    [TargetGuid | _SurvivorGuids] = Children,

    lists:foreach(fun(G) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP2, SessP2, ?FILE_REF(G)), ?ATTEMPTS)
    end, Children),
    % only the TARGET child needs a pre-existing local location (so it soft-deletes);
    % leave the survivors un-replicated so they genuinely replicate during the transfer.
    ensure_local_location_created(WorkerP2, SessP2, TargetGuid),

    Master = self(),
    install_synchronize_block_blocker(WorkerP2, file_id:guid_to_uuid(TargetGuid), Master),

    {ok, Tid} = ?assertMatch({ok, _},
        opt_transfers:schedule_file_replication(WorkerP1, SessP1, ?FILE_REF(DirGuid), ProviderId2)),

    WorkerPid = await_synchronize_block_reached(),
    ?assertEqual(ok, lfm_proxy:unlink(WorkerP2, SessP2, ?FILE_REF(TargetGuid))),
    await_local_location_deleted(WorkerP2, TargetGuid),
    WorkerPid ! proceed,

    transfers_test_utils:assert_transfer_state(WorkerP2, Tid, #{
        replication_status => completed,
        scheduling_provider => ProviderId1,
        files_to_process => 3,
        files_processed => 3,
        failed_files => 0,
        files_replicated => 2,
        bytes_replicated => 2 * ?DEFAULT_SIZE
    }, ?ATTEMPTS).

%%%===================================================================
%%% Helpers for "file deleted during replication" testcases
%%%===================================================================

%% @private
create_file_with_content(Node, SessId, FilePath) ->
    {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(Guid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, ?DEFAULT_CONTENT)),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
    Guid.

%% @private
ensure_local_location_created(Node, SessId, Guid) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(Guid), read)),
    ?assertMatch({ok, _}, lfm_proxy:read(Node, Handle, 0, ?DEFAULT_SIZE)),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)).

%% @private
await_local_location_deleted(Node, Guid) ->
    ?assertEqual(true, rpc:call(Node, ?MODULE, local_location_deleted, [Guid]), ?ATTEMPTS).

%% @private
install_synchronize_block_blocker(Node, TargetUuid, Master) ->
    % reset the "already blocked" flag so (re-)runs start fresh
    ok = rpc:call(Node, application, set_env, [op_worker, sync_block_test_blocked, false]),
    ok = test_utils:mock_new(Node, sync_req, [passthrough]),
    ok = test_utils:mock_expect(Node, sync_req, synchronize_block, fun
        (UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
            case file_ctx:get_logical_uuid_const(FileCtx) of
                Uuid when Uuid =:= TargetUuid ->
                    % Block only the FIRST hit. synchronize_block can throw
                    % {error, not_found}, which the transfer framework RETRIES - re-entering
                    % this mock. Blocking again would deadlock (no second `proceed`), so let
                    % retries pass straight through (they re-throw not_found until retries are
                    % exhausted and the file is counted as processed).
                    case application:get_env(op_worker, sync_block_test_blocked, false) of
                        false ->
                            application:set_env(op_worker, sync_block_test_blocked, true),
                            Master ! {?SYNC_BLOCK_REACHED, self()},
                            receive proceed -> ok end;
                        true ->
                            ok
                    end,
                    meck:passthrough([UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority]);
                _ ->
                    meck:passthrough([UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority])
            end;
        (UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
            meck:passthrough([UserCtx, FileCtx, Block, Prefetch, TransferId, Priority])
    end).

%% @private
await_synchronize_block_reached() ->
    receive
        {?SYNC_BLOCK_REACHED, Pid} -> Pid
    after timer:seconds(60) ->
        ct:fail("synchronize_block was never reached for target file")
    end.

local_location_deleted(Guid) ->
    FileCtx = file_ctx:new_by_guid(Guid),
    case fslogic_location_cache:get_local_location_including_deleted(FileCtx, skip_local_blocks) of
        {ok, #document{deleted = true}} -> true;
        _ -> false
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, rerun_transfers, false)
        end, ?config(op_worker_nodes, NewConfig1)),

        application:start(ssl),
        application:ensure_all_started(hackney),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, ?MODULE]}
        | Config
    ].

init_per_testcase(rtransfer_works_between_providers_with_different_ports = Case, Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),
    {ok, C} = rpc:call(Worker1, application, get_env, [rtransfer_link, transfer]),
    C1 = lists:keyreplace(server_port, 1, C, {server_port, 30000}),
    rpc:call(Worker1, application, set_env, [rtransfer_link, transfer, C1]),
    
    ProviderId1 = rpc:call(Worker1, oneprovider, get_id, []),
    rpc:call(Worker2, node_cache, clear, [{rtransfer_port, ProviderId1}]),
    rpc:call(Worker1, rtransfer_config, restart_link, []),
    
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    [{space_id, ?SPACE_ID} | Config].


end_per_testcase(Case, Config) when
    Case =:= regular_file_deleted_locally_during_replication;
    Case =:= regular_file_deleted_remotely_during_replication;
    Case =:= file_deleted_during_directory_replication
->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    catch test_utils:mock_unload(WorkerP2, sync_req),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_replication_worker(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:remove_all_views(Workers, ?SPACE_ID),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    application:stop(hackney),
    application:stop(ssl),
    initializer:teardown_storage(Config).

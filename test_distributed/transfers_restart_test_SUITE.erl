%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of transfers in case of node restart without HA.
%%% @end
%%%-------------------------------------------------------------------
-module(transfers_restart_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    rtransfer_restart_test/1,
    node_gentle_restart_test/1,
    node_kill_test/1
]).

% For RPC
-export([count_missing_transfer_links_in_db/2, get_transfer_links/4]).

all() -> [
    rtransfer_restart_test,
    node_gentle_restart_test,
    node_kill_test
].

-define(FILE_DATA, <<"1234567890abcd">>).

%%%===================================================================
%%% API
%%%===================================================================

rtransfer_restart_test(Config) ->
    RestartFun = fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, rtransfer_config, restart_link, [])),
        ct:pal("Rtransfer restarted"),
        Config
    end,

    restart_test_base(Config, RestartFun, rtransfer_link_only).

node_gentle_restart_test(Config) ->
    RestartFun = fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, application, stop, [?APP_NAME])),
        ct:pal("Application stopped"),
        failure_test_utils:kill_nodes(Config, Worker),
        ct:pal("Node killed"),
        UpdatedConfig = failure_test_utils:restart_nodes(Config, Worker),
        ct:pal("Node restarted"),
        UpdatedConfig
    end,

    restart_test_base(Config, RestartFun, gentle_stop).

node_kill_test(Config) ->
    RestartFun = fun(Worker) ->
        failure_test_utils:kill_nodes(Config, Worker),
        ct:pal("Node killed"),
        UpdatedConfig = failure_test_utils:restart_nodes(Config, Worker),
        ct:pal("Node restarted"),
        UpdatedConfig
    end,

    restart_test_base(Config, RestartFun, kill).

restart_test_base(Config, RestartFun, RestartType) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = rpc:call(WorkerP1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    SessIdP1 = SessId(P1),
    SessIdP2 = SessId(P2),
    FlushCheckAttempts = 60,
    Attempts = 600,
    FilesCount = 500,
    FileSize = byte_size(?FILE_DATA),
    Priority = 32,
    UserCtxP2 = rpc:call(WorkerP2, user_ctx, new, [SessIdP2]),

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    onenv_test_utils:disable_panel_healthcheck(Config),

    % Create test files
    Files1 = file_ops_test_utils:create_files(WorkerP1, SessIdP1, SpaceGuid, FilesCount),
    Files2 = file_ops_test_utils:create_files(WorkerP1, SessIdP1, SpaceGuid, FilesCount),
    AllFiles = Files1 ++ Files2,

    % Wait until dbsync synchronizes all files
    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}},
            lfm_proxy:stat(WorkerP2, SessIdP2, {guid, File}), Attempts)
    end, AllFiles),

    % Schedule on_the_fly blocks replications
    lists_utils:pforeach(fun(File) ->
        FileCtx = file_ctx:new_by_guid(File),
        lists:foreach(fun(Offset) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                rpc:call(WorkerP2, sync_req, request_block_synchronization,
                    [UserCtxP2, FileCtx, #file_block{offset = Offset, size = 1}, false, undefined, Priority]))
        end, lists:seq(0, FileSize))
    end, Files1),

    % Schedule transfers
    PMapAns = lists_utils:pmap(fun(File) ->
        FileCtx = file_ctx:new_by_guid(File),
        lists:foreach(fun(Offset) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                rpc:call(WorkerP2, sync_req, request_block_synchronization,
                    [UserCtxP2, FileCtx, #file_block{offset = Offset, size = 1}, false, undefined, Priority]))
        end, lists:seq(0,9)),
        lfm_proxy:schedule_file_replication(WorkerP2, SessIdP2, {guid, File}, P2)
    end, Files2),

    % Verify transfers scheduling
    TransferIds = lists:map(fun(Ans) ->
        {ok, TransferId} = ?assertMatch({ok, _}, Ans),
        TransferId
    end, PMapAns),

    % If node is to be killed, check if information about transfers is persisted in couchbase
    case RestartType of
        kill ->
            lists:foreach(fun(TransferId) ->
                ?assertMatch({ok, _}, rpc:call(WorkerP2, datastore_model, get,
                    [#{model => transfer, memory_driver => undefined}, TransferId]), FlushCheckAttempts)
            end, TransferIds),
            ?assertEqual(0, count_missing_transfer_links_in_db(WorkerP2, SpaceId, TransferIds), FlushCheckAttempts);
        _ ->
            ok
    end,

    % Restart node
    UpdatedConfig = RestartFun(WorkerP2),
    UpdatedSessIdP2 = test_config:get_user_session_id_on_provider(UpdatedConfig, User1, P2),

    % Verify transfers
    TransferIdsToVerify = case RestartType of
        kill ->
            % It is possible that status in the document hasn't been updated
            % if node was killed when link was being moved from one tree to another.
            sets:to_list(sets:intersection(sets:from_list(TransferIds),
                get_scheduled_and_current_transfer_links_set(WorkerP2, SpaceId)));
        _ ->
            TransferIds
    end,
    lists:foreach(fun(TransferId) ->
        multi_provider_file_ops_test_base:await_replication_end(WorkerP2, TransferId, Attempts, get_effective)
    end, TransferIdsToVerify),

    % Verify if data has been replicated
    FilesToCheckDistribution = case RestartType of
        kill -> []; % documents with transferred blocks could be lost
        gentle_stop -> Files2; % on demand transfers could be lost
        rtransfer_link_only -> AllFiles
    end,
    lists:foreach(fun(File) ->
        % Use root session as user session is not valid after restart
        ?assertMatch({ok, [
            #{<<"blocks">> := [[0, FileSize]], <<"totalBlocksSize">> := FileSize},
            #{<<"blocks">> := [[0, FileSize]], <<"totalBlocksSize">> := FileSize}
        ]}, lfm_proxy:get_file_distribution(WorkerP2, UpdatedSessIdP2, {guid, File}), Attempts)
    end, FilesToCheckDistribution),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        onenv_test_utils:prepare_base_test_config(NewConfig)
    end,
    test_config:set_many(Config, [
        {add_envs, [op_worker, op_worker, [{session_validity_check_interval_seconds, 1800}]]},
        {add_envs, [op_worker, op_worker, [{fuse_session_grace_period_seconds, 1800}]]},
        {set_onenv_scenario, ["2op"]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).

init_per_testcase(_Case, Config) ->
    Workers = test_config:get_all_op_worker_nodes(Config),
    test_utils:set_env(Workers, ?APP_NAME, minimal_sync_request, 1),
    test_utils:set_env(Workers, ?APP_NAME, synchronizer_block_suiting, false),
    UpdatedConfig = provider_onenv_test_utils:setup_sessions(Config),
    lfm_proxy:init(UpdatedConfig, false).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

count_missing_transfer_links_in_db(Worker, SpaceId, TransferIds) ->
    test_node_starter:load_modules([Worker], [?MODULE]),
    rpc:call(Worker, ?MODULE, count_missing_transfer_links_in_db, [SpaceId, TransferIds]).

count_missing_transfer_links_in_db(SpaceId, TransferIds) ->
    {ok, Acc} = get_transfer_links_from_db(<<"SCHEDULED_TRANSFERS_KEY">>, SpaceId, sets:new()),
    {ok, Acc2} = get_transfer_links_from_db(<<"CURRENT_TRANSFERS_KEY">>, SpaceId, Acc),
    {ok, TransferIdsInDb} = get_transfer_links_from_db(<<"PAST_TRANSFERS_KEY">>, SpaceId, Acc2),

    length(lists:filter(fun(TransferId) -> not sets:is_element(TransferId, TransferIdsInDb) end, TransferIds)).

get_transfer_links_from_db(Prefix, SpaceId, Acc0) ->
    Ctx = #{model => transfer, memory_driver => undefined},
    get_transfer_links(Ctx, Prefix, SpaceId, Acc0).

get_transfer_links(Ctx, Prefix, SpaceId, Acc0) ->
    datastore_model:fold_links(Ctx, <<Prefix/binary, "_", SpaceId/binary>>, all, fun
        (#link{target = TransferId}, Acc) ->
            {ok, sets:add_element(TransferId, Acc)}
    end, Acc0, #{}).

get_scheduled_and_current_transfer_links_set(Worker, SpaceId) ->
    test_node_starter:load_modules([Worker], [?MODULE]),
    Ctx = #{model => transfer},
    {ok, Acc} = rpc:call(Worker, ?MODULE, get_transfer_links, [Ctx, <<"SCHEDULED_TRANSFERS_KEY">>, SpaceId, sets:new()]),
    {ok, Acc2} = rpc:call(Worker, ?MODULE, get_transfer_links, [Ctx, <<"CURRENT_TRANSFERS_KEY">>, SpaceId, Acc]),
    Acc2.
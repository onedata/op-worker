%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains implementation of generic mechanism for
%%% tests of transfers.
%%% It also contains implementation of test scenarios which are used to
%%% compose test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(transfers_test_mechanism).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("transfers_test_mechanism.hrl").
-include("countdown_server.hrl").
-include("rest_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([run_test/2]).

% test scenarios
-export([
    replicate_root_directory/2,
    replicate_each_file_separately/2,
    error_on_replicating_files/2,
    cancel_replication_on_target_nodes/2,
    restart_replication_on_target_nodes/2
]).

-export([move_transfer_ids_to_old_key/1]).

% functions exported to be called by rpc
-export([create_files_structure/10, create_file/6,
    assert_file_visible/5, assert_file_distribution/6]).

-define(DEFAULT_USER_TOKEN_HEADERS(Config),
    [?USER_TOKEN_HEADER(Config, ?DEFAULT_USER)]).

%%%===================================================================
%%% API
%%%===================================================================

run_test(Config, #transfer_test_spec{
    setup = Setup,
    scenario = Scenario,
    expected = Expected
}) ->

    try
        Config2 = setup_test(Config, Setup),
        Config3 = run_scenario(Config2, Scenario),
        assert_expectations(Config3, Expected),
        Config3
    catch
        throw:{test_timeout, Function} ->
            ct:fail(
                "Test timeout in function ~p:~p.~n~nStacktrace: ~s",
                [?MODULE, Function, lager:pr_stacktrace(erlang:get_stacktrace())]
            );
        throw:{wrong_assertion_key, Key, List} ->
            ct:fail(
                "Assertion key: ~p not found in list of keys: ~p~n"
                "Stacktrace: ~s",
                [Key, List, lager:pr_stacktrace(erlang:get_stacktrace())]
            );
        exit:Reason = {test_case_failed, _} ->
            erlang:exit(Reason);
        Type:Message ->
            ct:fail(
                "Unexpected error in ~p:run_scenario - ~p:~p~nStacktrace: ~s",
                [
                    ?MODULE, Type, Message,
                    lager:pr_stacktrace(erlang:get_stacktrace())
                ]
            )
    end.

%%%===================================================================
%%% Test scenarios
%%%===================================================================

replicate_root_directory(Config, #scenario{
    type = Type,
    file_key_type = FileKeyType,
    schedule_node = ScheduleNode,
    target_nodes = TargetNodes
}) ->
    {Guid, Path} = ?config(?ROOT_DIR_KEY, Config),
    FileKey = file_key(Guid, Path, FileKeyType),
    NodesTransferIdsAndFiles = lists:map(fun(TargetNode) ->
        TargetProviderId = transfers_test_utils:provider_id(TargetNode),
        {ok, Tid} = schedule_file_replication(ScheduleNode, TargetProviderId, FileKey, Config, Type),
        {TargetNode, Tid, Guid, Path}
    end, TargetNodes),
    update_config(?TRANSFERS_KEY, fun(OldNodesTransferIdsAndFiles) ->
        NodesTransferIdsAndFiles ++ OldNodesTransferIdsAndFiles
    end, Config, []).


replicate_each_file_separately(Config, #scenario{
    type = Type,
    file_key_type = FileKeyType,
    schedule_node = ScheduleNode,
    target_nodes = TargetNodes
}) ->
    FilesGuidsAndPaths = ?config(?FILES_KEY, Config),
    NodesTransferIdsAndFiles = lists:flatmap(fun(TargetNode) ->
        lists:map(fun({Guid, Path}) ->
            FileKey = file_key(Guid, Path, FileKeyType),
            TargetProviderId = transfers_test_utils:provider_id(TargetNode),
            {ok, Tid} = schedule_file_replication(ScheduleNode, TargetProviderId, FileKey, Config, Type),
            {TargetNode, Tid, Guid, Path}
        end, FilesGuidsAndPaths)
    end, TargetNodes),

    update_config(?TRANSFERS_KEY, fun(OldNodesTransferIdsAndFiles) ->
        NodesTransferIdsAndFiles ++ OldNodesTransferIdsAndFiles
    end, Config, []).

error_on_replicating_files(Config, #scenario{
    type = Type,
    file_key_type = FileKeyType,
    schedule_node = ScheduleNode,
    target_nodes = TargetNodes
}) ->
    FilesGuidsAndPaths = ?config(?FILES_KEY, Config),
    lists:foreach(fun(TargetNode) ->
        lists:foreach(fun({Guid, Path}) ->
            TargetProviderId = transfers_test_utils:provider_id(TargetNode),
            FileKey = file_key(Guid, Path, FileKeyType),
            ?assertMatch({error, _},
                schedule_file_replication(ScheduleNode, TargetProviderId, FileKey, Config, Type))
        end, FilesGuidsAndPaths)
    end, TargetNodes),
    Config.

cancel_replication_on_target_nodes(Config, #scenario{
    type = Type,
    file_key_type = FileKeyType,
    schedule_node = ScheduleNode,
    target_nodes = TargetNodes
}) ->
    {Guid, Path} = ?config(?ROOT_DIR_KEY, Config),
    FileKey = file_key(Guid, Path, FileKeyType),
    NodesTransferIdsAndFiles = lists:map(fun(TargetNode) ->
        TargetProviderId = transfers_test_utils:provider_id(TargetNode),
        {ok, Tid} = schedule_file_replication(ScheduleNode, TargetProviderId, FileKey, Config, Type),
        {TargetNode, Tid, Guid, Path}
    end, TargetNodes),

    utils:pforeach(fun({TargetNode, Tid, _Guid, _Path}) ->
        await_transfer_starts(TargetNode, Tid),
        cancel_transfer(TargetNode, Tid, Config, Type)
    end, NodesTransferIdsAndFiles),

    update_config(?TRANSFERS_KEY, fun(OldNodesTransferIdsAndFiles) ->
        NodesTransferIdsAndFiles ++ OldNodesTransferIdsAndFiles
    end, Config, []).

restart_replication_on_target_nodes(Config, #scenario{type = Type}) ->
    NodesTransferIdsAndFiles = ?config(?TRANSFERS_KEY, Config),
    lists:foreach(fun({TargetNode, Tid, _Guid, _Path}) ->
        restart_transfer(TargetNode, Tid, Config, Type)
    end, NodesTransferIdsAndFiles),
    Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================

run_scenario(Config, Scenario = #scenario{function = ScenarioFunction}) ->
    ScenarioFunction(Config, Scenario).

setup_test(Config, undefined) ->
    Config;
setup_test(Config, Setup = #setup{
    root_directory = undefined
}) ->
    setup_test(Config, Setup#setup{root_directory = <<"">>});
setup_test(Config, Setup) ->
    ensure_verification_pool_started(),
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(N) -> countdown_server:start_link(self(), N) end, Nodes),
    Config2 = create_files(Config, Setup),
    assert_setup(Config2, Setup),
    Config2.

assert_expectations(Config, Expected = #expected{
    assertion_nodes = AssertionNodes,
    distribution = ExpectedDistribution,
    attempts = Attempts,
    timeout = Timetrap
}) ->
    NodesTransferIdsAndFiles = ?config(?TRANSFERS_KEY, Config, []),
    OldNodesTransferIdsAndFiles = ?config(?OLD_TRANSFERS_KEY, Config, []),
    Tids = [Tid || {_, Tid, _, _} <- NodesTransferIdsAndFiles],
    OldTids = [Tid || {_, Tid, _, _} <- OldNodesTransferIdsAndFiles],
    AllTids = lists:usort(Tids ++ OldTids),

    lists:foreach(fun(AssertionNode) ->
        SpaceId = ?config(?SPACE_ID_KEY, Config),
        lists:foreach(fun({TargetNode, TransferId, FileGuid, FilePath}) ->
            assert_transfer(Config, AssertionNode, Expected, TransferId,
                FileGuid, FilePath, TargetNode),
            assert_files_distribution_on_all_nodes(Config, AssertionNodes,
                ExpectedDistribution, Attempts, Timetrap),
            ?assertEqual([], transfers_test_utils:get_ongoing_transfers_for_file(AssertionNode, FileGuid), Attempts),
            ?assertEqual(true, begin
                EndedTransfersForFile = transfers_test_utils:get_ended_transfers_for_file(AssertionNode, FileGuid),
                lists:member(TransferId, EndedTransfersForFile)
            end, Attempts),
            TransferId
        end, NodesTransferIdsAndFiles),

        ?assertEqual([], transfers_test_utils:list_waiting_transfers(AssertionNode, SpaceId), Attempts),
        ?assertEqual([], transfers_test_utils:list_ongoing_transfers(AssertionNode, SpaceId), Attempts),
        ?assertEqual(AllTids, lists:sort(transfers_test_utils:list_ended_transfers(AssertionNode, SpaceId)), Attempts)

    end, AssertionNodes).

assert_transfer(_Config, _Node, #expected{
    expected_transfer = undefined,
    attempts = _Attempts
}, _TransferId, _FileGuid, _FilePath, _TargetNode) ->
    ok;
assert_transfer(Config, Node, #expected{
    expected_transfer = TransferAssertion,
    attempts = Attempts
}, TransferId, FileGuid, FilePath, TargetNode) ->
    SpaceId = ?config(?SPACE_ID_KEY, Config),
    assert_transfer_state(Node, TransferId, SpaceId, FileGuid, FilePath,
        TargetNode, TransferAssertion, Attempts).

assert_transfer_state(Node, TransferId, SpaceId, FileGuid, FilePath, TargetNode,
    ExpectedTransfer, Attempts
) ->
    try
        Transfer = transfers_test_utils:get_transfer(Node, TransferId),
        assert(ExpectedTransfer, Transfer)
    catch
        throw:transfer_not_found ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_transfer_state(Node, TransferId, SpaceId, FileGuid,
                        FilePath, TargetNode, ExpectedTransfer, Attempts - 1);
                true ->
                    ct:pal("Transfer: ~p not found.", [TransferId]),
                    ct:fail("Transfer: ~p not found.", [TransferId])

            end;
        throw:{assertion_error, Field, Expected, Value} ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_transfer_state(Node, TransferId, SpaceId, FileGuid,
                        FilePath, TargetNode, ExpectedTransfer, Attempts - 1);
                true ->
                    {Format, Args} = transfer_fields_description(Node, TransferId),
                    ct:pal(
                        "Assertion of field \"~p\" in transfer ~p failed.~n"
                        "    Expected: ~p~n"
                        "    Value: ~p~n" ++ Format,[Field, TransferId, Expected, Value | Args]),
                    ct:fail("assertion failed")
            end
    end.

create_files(Config, #setup{
    setup_node = SetupNode,
    files_structure = FilesStructure,
    root_directory = RootDirectory,
    mode = Mode,
    size = Size,
    file_prefix = FilePrefix,
    dir_prefix = DirPrefix,
    timeout = Timetrap
}) ->

    validate_root_directory(RootDirectory),
    RootDirectory2 = utils:ensure_defined(RootDirectory, undefined, <<"">>),
    SessionId = ?DEFAULT_SESSION(SetupNode, Config),
    SpaceId = ?config(?SPACE_ID_KEY, Config),
    {DirsToCreate, FilesToCreate} = count_files_and_dirs(FilesStructure),

    FilesCounterRef = countdown_server:init_counter(SetupNode, FilesToCreate),
    DirsCounterRef = countdown_server:init_counter(SetupNode, DirsToCreate),

    {RootDirGuid, RootDirPath} = maybe_create_root_dir(SetupNode, RootDirectory2, SessionId, SpaceId),
    cast_create_files_structure(SetupNode, SessionId, FilesStructure, Mode, Size,
        RootDirPath, FilesCounterRef, DirsCounterRef, FilePrefix, DirPrefix
    ),

    DirsGuidsAndPaths = countdown_server:await(SetupNode, DirsCounterRef, Timetrap),
    FilesGuidsAndPaths = countdown_server:await(SetupNode, FilesCounterRef, Timetrap),

    [
        {?ROOT_DIR_KEY, {RootDirGuid, RootDirPath}},
        {?DIRS_KEY, DirsGuidsAndPaths},
        {?FILES_KEY, FilesGuidsAndPaths} | Config
    ].

assert_setup(Config, #setup{
    assertion_nodes = []
}) ->
    Config;
assert_setup(Config, #setup{
    assertion_nodes = AssertionNodes,
    distribution = ExpectedDistribution,
    attempts = Attempts,
    timeout = Timetrap
}) ->
    assert_files_visible_on_all_nodes(Config, AssertionNodes, Attempts, Timetrap),
    assert_files_distribution_on_all_nodes(Config, AssertionNodes,
        ExpectedDistribution, Attempts, Timetrap).

await_countdown(Refs, Timetrap) when is_list(Refs) ->
    await_countdown(sets:from_list(Refs), Timetrap);
await_countdown(Refs, Timetrap) ->
    await_countdown(Refs, #{}, Timetrap).

await_countdown(Refs, DataMap, Timetrap) ->
    receive
        {?COUNTDOWN_FINISHED, Ref, AssertionNode, Data} ->
            Refs2 = sets:del_element(Ref, Refs),
            DataMap2 = DataMap#{Ref => Data},
            case sets:size(Refs2) of
                0 ->
                    DataMap2;
                _ ->
                    await_countdown(Refs2, DataMap2, Timetrap)
            end
    after
        Timetrap ->
            throw(?TEST_TIMEOUT(?FUNCTION_NAME))
    end.

assert_files_visible_on_all_nodes(Config, AssertionNodes, Attempts, Timetrap) ->
    Refs = lists:map(fun(AssertionNode) ->
        cast_files_visible_assertion(Config, AssertionNode, Attempts)
    end, AssertionNodes),
    await_countdown(Refs, Timetrap).

assert_files_distribution_on_all_nodes(_Config, _AssertionNodes, undefined, _Attempts, _Timetrap) ->
    ok;
assert_files_distribution_on_all_nodes(Config, AssertionNodes, ExpectedDistribution, Attempts, Timetrap) ->
    Refs = lists:map(fun(AssertionNode) ->
        cast_files_distribution_assertion(Config, AssertionNode, ExpectedDistribution, Attempts)
    end, AssertionNodes),
    await_countdown(Refs, Timetrap).

cast_files_distribution_assertion(Config, Node, Expected, Attempts) ->
    FileGuidsAndPaths = ?config(?FILES_KEY, Config),
    SessionId = ?DEFAULT_SESSION(Node, Config),
    CounterRef = countdown_server:init_counter(Node, length(FileGuidsAndPaths)),
    lists:foreach(fun({FileGuid, _FilePath}) ->
        cast_file_distribution_assertion(Expected, Node, SessionId, FileGuid, CounterRef, Attempts)
    end, FileGuidsAndPaths),
    CounterRef.

cast_files_visible_assertion(Config, Node, Attempts) ->
    RootDirGuidAndPath = ?config(?ROOT_DIR_KEY, Config),
    FilesGuidsAndPaths = ?config(?FILES_KEY, Config),
    DirsGuidsAndPaths = ?config(?DIRS_KEY, Config),
    SessionId = ?DEFAULT_SESSION(Node, Config),
    GuidsAndPaths = FilesGuidsAndPaths ++ [RootDirGuidAndPath | DirsGuidsAndPaths],
    CounterRef = countdown_server:init_counter(Node, length(GuidsAndPaths)),
    lists:foreach(fun({FileGuid, _FilePath}) ->
        cast_file_visible_assertion(Node, SessionId, FileGuid, CounterRef, Attempts)
    end, GuidsAndPaths),
    CounterRef.

cast_file_visible_assertion(Node, SessionId, FileGuid, CounterRef, Attempts) ->
    worker_pool:cast(?WORKER_POOL, {?MODULE, assert_file_visible,
        [Node, SessionId, FileGuid, CounterRef, Attempts]}
    ).

cast_file_distribution_assertion(Expected, Node, SessionId, FileGuid, CounterRef, Attempts) ->
    worker_pool:cast(?WORKER_POOL, {?MODULE, assert_file_distribution,
        [Expected, Node, SessionId, FileGuid, CounterRef, Attempts]}
    ).

assert_file_visible(Node, SessId, FileGuid, CounterRef, Attempts) ->
    try
        ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, {guid, FileGuid}), Attempts),
        countdown_server:decrease(Node, CounterRef, FileGuid)
    catch
        _:_ ->
            % this function is executed by worker_pool process
            % this catch is used to avoid "ugly" worker pool error log
            ok
    end.

assert_file_distribution(Expected, Node, SessId, FileGuid, CounterRef, Attempts) ->
    Expected2 = lists:sort(Expected),
    try
        ?assertMatch(Expected2, begin
            {ok, Distribution} = lfm_proxy:get_file_distribution(Node, SessId, {guid, FileGuid}),
            lists:sort(Distribution)
        end, Attempts),
        countdown_server:decrease(Node, CounterRef, FileGuid)
    catch
        _:_ ->
            % this function is executed by worker_pool process
            % this catch is used to avoid "ugly" worker pool error log
            ok
    end.

maybe_create_root_dir(Node, RootDirectory, SessionId, SpaceId) ->
    case RootDirectory of
        <<"">> ->
            {space_guid(SpaceId), filename:join([<<"/">>, SpaceId])};
        _ ->
            Path = filename:join([<<"/">>, SpaceId, RootDirectory]),
            {ok, Guid} = lfm_proxy:mkdir(Node, SessionId, Path),
            {Guid, Path}
    end.

space_guid(SpaceId) ->
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).

validate_root_directory(Path = <<"/", _>>) ->
    throw({absolute_root_path, Path});
validate_root_directory(_) -> ok.

create_files_structure(_Scheduler, _SessionId, [], _Mode, _Size,
    _RootDirPath, _FilesCounterRef, _DirsCounterRef, _FilePrefix, _DirPrefix) ->
    ok;
create_files_structure(Scheduler, SessionId, [{SubDirs, SubFiles} | Rest], Mode,
    Size, RootDirPath, FilesCounterRef, DirsCounterRef, FilePrefix, DirPrefix
) ->
    cast_subfiles_creation(Scheduler, SessionId, SubFiles, Mode, Size,
        RootDirPath, FilesCounterRef, FilePrefix),
    cast_subdirs_creation(Scheduler, SessionId, SubDirs, Rest, Mode, Size,
        RootDirPath, FilesCounterRef, DirsCounterRef, FilePrefix, DirPrefix).

create_dir(Node, SessionId, DirPath, CounterRef) ->
    {ok, DirGuid} = lfm_proxy:mkdir(Node, SessionId, DirPath),
    mark_dir_created(Node, DirGuid, DirPath, CounterRef).

cast_subfiles_creation(Node, SessionId, SubfilesNum, Mode, Size, ParentPath,
    FilesCounterRef, FilePrefix) ->
    lists:foreach(fun(N) ->
        FilePath = subfile_path(ParentPath, FilePrefix, N),
        cast_file_creation(Node, SessionId, FilePath, Mode, Size, FilesCounterRef)
    end, lists:seq(1, SubfilesNum)).

cast_subdirs_creation(Node, SessionId, SubDirsNum, Structure, Mode, Size,
    ParentPath, FilesCounterRef, DirsCounterRef, FilePrefix, DirPrefix) ->
    lists:foreach(fun(N) ->
        DirPath = subdir_path(ParentPath, DirPrefix, N),
        create_dir(Node, SessionId, DirPath, DirsCounterRef),
        cast_create_files_structure(Node, SessionId, Structure, Mode, Size,
            DirPath, FilesCounterRef, DirsCounterRef, FilePrefix, DirPrefix)
    end, lists:seq(1, SubDirsNum)).

cast_file_creation(Node, SessionId, FilePath, Mode, Size, CounterRef) ->
    ok = worker_pool:cast(?WORKER_POOL, {?MODULE, create_file,
        [Node, SessionId, FilePath, Mode, Size, CounterRef]
    }).

create_file(Node, SessionId, FilePath, Mode, Size, CounterRef) ->
    {ok, Guid} = lfm_proxy:create(Node, SessionId, FilePath, Mode),
    {ok, Handle} = lfm_proxy:open(Node, SessionId, {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(Node, Handle, 0, crypto:strong_rand_bytes(Size)),
    ok = lfm_proxy:close(Node, Handle),
    mark_file_created(Node, Guid, FilePath, CounterRef).

mark_file_created(Node, FileGuid, FilePath, CounterRef) ->
    countdown_server:decrease(Node, CounterRef, {FileGuid, FilePath}).

mark_dir_created(Node, DirGuid, DirPath, CounterRef) ->
    countdown_server:decrease(Node, CounterRef, {DirGuid, DirPath}).

cast_create_files_structure(Node, SessionId, Structure, Mode, Size, ParentPath,
    FilesCounterRef, DirsCounterRef, FilePrefix, DirPrefix
) ->
    ok = worker_pool:cast(?WORKER_POOL, {?MODULE, create_files_structure,
        [Node, SessionId, Structure, Mode, Size, ParentPath, FilesCounterRef,
            DirsCounterRef, FilePrefix, DirPrefix]}).

subfile_path(ParentPath, FilePrefix, N) ->
    filename:join([ParentPath, <<FilePrefix/binary, (integer_to_binary(N))/binary>>]).

subdir_path(ParentPath, DirPrefix, N) ->
    filename:join([ParentPath, <<DirPrefix/binary, (integer_to_binary(N))/binary>>]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Counts total number of directories and regular files in the Structure.
%% Structure is the list of tuples {DirsNum, FilesNum} specifying number
%% given file types on subsequent levels of a file tree.
%% @end
%%-------------------------------------------------------------------
-spec count_files_and_dirs([{non_neg_integer(), non_neg_integer()}]) -> {non_neg_integer(), non_neg_integer()}.
count_files_and_dirs(Structure) ->
    {DirsSum, FilesSum, _} = lists:foldl(fun({SubDirs, SubFiles}, {AccDirs, AccFiles, PrevLevelDirs}) ->
        CurrentLevelDirs = PrevLevelDirs * SubDirs,
        CurrentLevelFiles = PrevLevelDirs * SubFiles,
        {AccDirs + CurrentLevelDirs, AccFiles + CurrentLevelFiles, CurrentLevelDirs}
    end, {0, 0, 1}, Structure),
    {DirsSum, FilesSum}.

ensure_verification_pool_started() ->
    {ok, _} = application:ensure_all_started(worker_pool),
    worker_pool:start_sup_pool(?WORKER_POOL, [{workers, 8}]).

move_transfer_ids_to_old_key(Config) ->
    map_config_key(?TRANSFERS_KEY, ?OLD_TRANSFERS_KEY, Config).

map_config_key(Key0, NewKey, Config) ->
    lists:keymap(fun(Key) ->
        case Key of
            Key0 -> NewKey;
            (Other) -> Other
        end
    end, 1, Config).

update_config(Key, UpdateFun, Config, DefaultValue) ->
    case proplists:get_value(Key, Config, no_value) of
        no_value ->
            [{Key, UpdateFun(DefaultValue)} | Config];
        OldValue ->
            lists:keyreplace(Key, 1, Config, {Key, UpdateFun(OldValue)})
    end.

schedule_file_replication(ScheduleNode, ProviderId, FileKey, Config, lfm) ->
    schedule_file_replication_by_lfm(ScheduleNode, ProviderId, FileKey, Config);
schedule_file_replication(ScheduleNode, ProviderId, FileKey, Config, rest) ->
    schedule_file_replication_by_rest(ScheduleNode, ProviderId, FileKey, Config).

schedule_file_replication_by_lfm(ScheduleNode, ProviderId, FileKey, Config) ->
    SessionId = ?DEFAULT_SESSION(ScheduleNode, Config),
    lfm_proxy:schedule_file_replication(ScheduleNode, SessionId, FileKey, ProviderId).

schedule_file_replication_by_rest(Worker, ProviderId, {path, FilePath}, Config) ->
    case rest_test_utils:request(Worker,
        <<"replicas/", FilePath/binary, "?provider_id=", ProviderId/binary>>,
        post, ?DEFAULT_USER_TOKEN_HEADERS(Config), []
    ) of
    {ok, 200, _, Body} ->
        DecodedBody = json_utils:decode(Body),
        #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
        {ok, Tid};
    {ok, 400, _, Body} ->
            {error, Body}
    end;
schedule_file_replication_by_rest(Worker, ProviderId, {guid, FileGuid}, Config) ->
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    case rest_test_utils:request(Worker,
        <<"replicas-id/", FileObjectId/binary, "?provider_id=", ProviderId/binary>>,
        post, ?DEFAULT_USER_TOKEN_HEADERS(Config), []
    ) of
        {ok, 200, _, Body} ->
            DecodedBody = json_utils:decode(Body),
            #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
            {ok, Tid};
        {ok, 400, _, Body} ->
            {error, Body}
    end.

cancel_transfer(ScheduleNode, Tid, Config, lfm) ->
    cancel_transfer_by_lfm(ScheduleNode, Tid, Config);
cancel_transfer(ScheduleNode, Tid, Config, rest) ->
    cancel_transfer_by_rest(ScheduleNode, Tid, Config).

restart_transfer(ScheduleNode, Tid, Config, lfm) ->
    restart_transfer_by_lfm(ScheduleNode, Tid, Config);
restart_transfer(ScheduleNode, Tid, Config, rest) ->
    restart_transfer_by_rest(ScheduleNode, Tid, Config).

cancel_transfer_by_lfm(_Worker, _Tid, _Config) ->
    erlang:error(not_implemented).

restart_transfer_by_lfm(_ScheduleNode, _Tid, _Config) ->
    erlang:error(not_implemented).

cancel_transfer_by_rest(Worker, Tid, Config) ->
    rest_test_utils:request(Worker, <<"transfers/", Tid/binary>>, delete,
        ?DEFAULT_USER_TOKEN_HEADERS(Config), []).

restart_transfer_by_rest(Worker, Tid, Config) ->
    rest_test_utils:request(Worker, <<"transfers/", Tid/binary>>, patch,
        ?DEFAULT_USER_TOKEN_HEADERS(Config), []).

file_key(Guid, _Path, guid) ->
    {guid, Guid};
file_key(_Guid, Path, path) ->
    {path, Path}.

assert(ExpectedTransfer, Transfer) ->
    maps:fold(fun(FieldName, ExpectedValue, _AccIn) ->
        assert_field(ExpectedValue, Transfer, FieldName)
    end, undefined,  ExpectedTransfer).

assert_field(ExpectedValue, Transfer, FieldName) ->
    Value = get_transfer_value(Transfer, FieldName),
    try
        case Value of
            ExpectedValue -> ok;
            _ ->
                throw({assertion_error, FieldName, ExpectedValue, Value})
        end
    catch
        error:{assertMatch_failed, _} ->
            throw({assertion_error, FieldName, ExpectedValue, Value})
    end.

get_transfer_value(Transfer, FieldName) ->
    FieldsList = record_info(fields, transfer),
    Index = index(FieldName, FieldsList),
    element(Index + 1, Transfer).

index(Key, List) ->
    case lists:keyfind(Key, 2, lists:zip(lists:seq(1, length(List)), List)) of
        false ->
            throw({wrong_assertion_key, Key, List});
        {Index, _} ->
            Index
    end.

transfer_fields_description(Node, TransferId) ->
    FieldsList = record_info(fields, transfer),
    Transfer = transfers_test_utils:get_transfer(Node, TransferId),
    lists:foldl(fun(FieldName, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~p = ~p~n", AccArgs ++ [FieldName, get_transfer_value(Transfer, FieldName)]}
    end, {"~nTransfer ~p fields values:~n", [TransferId]}, FieldsList).

await_transfer_starts(Node, TransferId) ->
    ct:pal("AAAA"),
    ?assertEqual(true, begin
        try
            #transfer{
                bytes_transferred = BytesTransferred,
                files_transferred = FilesTransferred
            } = transfers_test_utils:get_transfer(Node, TransferId),
                ct:pal("BytesTransferred: ~p~nFilesTransferred: ~p", [BytesTransferred, FilesTransferred]),
            (BytesTransferred > 0) or (FilesTransferred > 0)
        catch
            throw:transfer_not_found ->
                false;
            Other:Error ->
                ct:pal("DUUUPA: ~p:~p", [Other, Error])
        end
    end, 60).
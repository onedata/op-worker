%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utils functions for QoS tests.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_tests_utils).
-author("Michal Cwiertnia").
-author("Michal Stanisz").

-include("qos_tests_utils.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("proto/oneclient/fuse_messages.hrl").


% assertions
-export([
    assert_distribution_in_dir_structure/3,
    assert_effective_qos/4, assert_effective_qos/5,
    assert_file_qos_documents/4, assert_file_qos_documents/5,
    assert_qos_entry_documents/3, assert_qos_entry_documents/4
]).

% mocks
-export([
    mock_space_storages/2,
    mock_storage_qos/2,
    mock_synchronize_transfers/1
]).

% util functions
-export([
    fulfill_qos_test_base/2,
    get_op_nodes_sorted/1, get_guid/2, get_guid/3,
    create_dir_structure/2, create_dir_structure/4,
    create_file/4, create_directory/3,
    wait_for_qos_fulfilment_in_parallel/4,
    add_qos/2, add_multiple_qos_in_parallel/2,
    map_qos_names_to_ids/2,
    get_provider_storage/1,
    inject_storage_id/2
]).

-define(ATTEMPTS, 60).
-define(SESS_ID(Config, Worker), ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config)).
-define(GET_FILE_UUID(Worker, SessId, FilePath),
    file_id:guid_to_uuid(qos_tests_utils:get_guid(Worker, SessId, FilePath))
).


%%%====================================================================
%%% Util functions
%%%====================================================================

fulfill_qos_test_base(Config, #fulfill_qos_test_spec{
    initial_dir_structure = InitialDirStructure,
    qos_to_add = QosToAddList,
    wait_for_qos_fulfillment = WaitForQos,
    expected_qos_entries = ExpectedQosEntries,
    expected_file_qos = ExpectedFileQos,
    expected_dir_structure = ExpectedDirStructure
}) ->
    % create initial dir structure
    GuidsAndPaths = create_dir_structure(Config, InitialDirStructure),
    ?assertMatch(true, assert_distribution_in_dir_structure(Config, InitialDirStructure, GuidsAndPaths)),

    % add QoS and w8 for fulfillment
    QosNameIdMapping = add_multiple_qos_in_parallel(Config, QosToAddList),
    wait_for_qos_fulfilment_in_parallel(Config, WaitForQos, QosNameIdMapping, ExpectedQosEntries),

    % check file distribution and qos documents
    ?assertMatch(ok, assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, ?ATTEMPTS)),
    ?assertMatch(ok, assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, ?ATTEMPTS)),
    ?assertMatch(true, assert_distribution_in_dir_structure(Config, ExpectedDirStructure, GuidsAndPaths)),
    {GuidsAndPaths, QosNameIdMapping}.


get_op_nodes_sorted(Config) ->
    Workers = ?config(op_worker_nodes, Config),

    % return list of workers sorted using provider ID
    SortingFun = fun(Worker1, Worker2) ->
        ProviderId1 = ?GET_DOMAIN_BIN(Worker1),
        ProviderId2 = ?GET_DOMAIN_BIN(Worker2),
        ProviderId1 =< ProviderId2
    end,
    lists:sort(SortingFun, Workers).


add_multiple_qos_in_parallel(Config, QosToAddList) ->
    Results = utils:pmap(fun(QosToAdd) -> add_qos(Config, QosToAdd) end, QosToAddList),
    ?assert(lists:all(fun(Result) ->
        case Result of
            {ok, {_QosName, _QosEntryId}} ->
                true;
            _ ->
                false
        end
    end, Results)),
    maps:from_list(lists:map(fun({ok, Result}) -> Result end, Results)).


add_qos(Config, #qos_to_add{
    worker = WorkerOrUndef,
    qos_name = QosName,
    path = FilePath,
    replicas_num = ReplicasNum,
    expression = QosExpression
}) ->
    % use first worker from sorted worker list if worker is not specified
    Worker = ensure_worker(Config, WorkerOrUndef),
    SessId = ?SESS_ID(Config, Worker),

    % ensure file exists
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {path, FilePath}), ?ATTEMPTS),

    {ok, QosEntryId} = ?assertMatch(
        {ok, _QosEntryId},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, FilePath}, QosExpression, ReplicasNum)
    ),

    {ok, {QosName, QosEntryId}}.


create_dir_structure(Config, #test_dir_structure{
    worker = WorkerOrUndef,
    dir_structure = DirStructureToCreate
}) ->
    % use first worker from sorted worker list if worker is not specified
    Worker = ensure_worker(Config, WorkerOrUndef),
    SessionId = ?SESS_ID(Config, Worker),
    create_dir_structure(Worker, SessionId, DirStructureToCreate, <<"/">>).

create_dir_structure(Worker, SessionId, {DirName, DirContent}, CurrPath) when is_list(DirContent) ->
    DirPath = filename:join(CurrPath, DirName),
    Map = case CurrPath == <<"/">> of
        true ->
            % skip creating space directory
            #{files => [], dirs => []};
        false ->
            DirGuid = create_directory(Worker, SessionId, DirPath),
            #{dirs => [{DirGuid, DirPath}]}
    end,
    lists:foldl(fun(Child, #{files := Files, dirs := Dirs}) ->
        #{files := NewFiles, dirs := NewDirs} = create_dir_structure(Worker, SessionId, Child, DirPath),
        #{files => Files ++ NewFiles, dirs => Dirs ++ NewDirs}
    end, maps:merge(#{files => [], dirs => []}, Map), DirContent);

create_dir_structure(Worker, SessionId, {FileName, FileContent}, CurrPath) ->
    create_dir_structure(Worker, SessionId, {FileName, FileContent, undefined}, CurrPath);

create_dir_structure(Worker, SessionId, {FileName, FileContent, _FileDistribution}, CurrPath) ->
    FilePath = filename:join(CurrPath, FileName),
    FileGuid = create_file(Worker, SessionId, FilePath, FileContent),
    #{files => [{FileGuid, FilePath}], dirs => []}.


create_directory(Worker, SessionId, DirPath) ->
    {ok, DirGuid} = ?assertMatch({ok, _DirGuid}, lfm_proxy:mkdir(Worker, SessionId, DirPath)),
    DirGuid.


create_file(Worker, SessionId, FilePath, FileContent) ->
    {ok, FileGuid} = ?assertMatch({ok, _FileGuid}, lfm_proxy:create(Worker, SessionId, FilePath, 8#700)),
    {ok, Handle} = ?assertMatch({ok, _Handle}, lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write)),
    Size = size(FileContent),
    ?assertMatch({ok, Size}, lfm_proxy:write(Worker, Handle, 0, FileContent)),
    ?assertMatch(ok, lfm_proxy:fsync(Worker, Handle)),
    ?assertMatch(ok, lfm_proxy:close(Worker, Handle)),
    FileGuid.


fill_in_expected_distribution(ExpectedDistribution, FileContent) ->
    lists:map(fun(ProviderDistributionOrId) ->
        case is_map(ProviderDistributionOrId) of
            true ->
                ProviderDistribution = ProviderDistributionOrId,
                case maps:is_key(<<"blocks">>, ProviderDistribution) of
                    true ->
                        ProviderDistribution;
                    _ ->
                        ProviderDistribution#{
                            <<"blocks">> => [[0, size(FileContent)]],
                            <<"totalBlocksSize">> => size(FileContent)
                        }
                end;
            false ->
                ProviderId = ProviderDistributionOrId,
                #{
                    <<"providerId">> => ProviderId,
                    <<"blocks">> => [[0, size(FileContent)]],
                    <<"totalBlocksSize">> => size(FileContent)
                }
        end
    end, ExpectedDistribution).


get_guid(Path, #{files := FilesGuidsAndPaths, dirs := DirsGuidsAndPaths}) ->
    lists:foldl(fun({Guid, P}, _) when P == Path -> Guid;
                   ({_, _}, Acc) -> Acc
    end, undefined, FilesGuidsAndPaths ++ DirsGuidsAndPaths).

get_guid(Worker, SessId, Path) ->
    {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:resolve_guid(Worker, SessId, Path)),
    Guid.


wait_for_qos_fulfilment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntries) ->
    % if test spec does not specify for which QoS fulfillment wait, wait for all QoS
    % on all workers
    Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    QosNamesWithWorkerList = lists:foldl(fun(QosName, Acc) ->
        [{QosName, Workers} | Acc]
    end, [], maps:keys(QosNameIdMapping)),
    wait_for_qos_fulfilment_in_parallel(Config, QosNamesWithWorkerList, QosNameIdMapping, ExpectedQosEntries);

wait_for_qos_fulfilment_in_parallel(Config, QosToWaitForList, QosNameIdMapping, ExpectedQosEntries) ->
    Results = utils:pmap(fun({QosName, WorkerList}) ->
        QosEntryId = maps:get(QosName, QosNameIdMapping),

        % try to find expected QoS entry associated with QoS name and get
        % expected value for is_possible field. If no QoS entry is found
        % assert "true" value for is_possible field.
        LookupExpectedQosEntry = lists:filter(fun(Entry) ->
            Entry#expected_qos_entry.qos_name == QosName
        end, ExpectedQosEntries),
        ExpectedIsPossible = case LookupExpectedQosEntry of
            [ExpectedQosEntry] ->
                ExpectedQosEntry#expected_qos_entry.computing_provider =/= undefined;
            [] ->
                true
        end,

        % wait for QoS fulfillment on different worker nodes
        utils:pmap(fun(Worker) ->
            wait_for_qos_fulfilment_in_parallel(Config, Worker, QosEntryId, QosName, ExpectedIsPossible)
        end, WorkerList)
    end, QosToWaitForList),

    ?assert(lists:all(fun(Result) -> Result =:= ok end, lists:flatten(Results))).

wait_for_qos_fulfilment_in_parallel(Config, Worker, QosEntryId, QosName, ExpectedIsPossible) ->
    SessId = ?SESS_ID(Config, Worker),
    Fun = fun() ->
        ErrMsg = case rpc:call(Worker, lfm_qos, get_qos_entry, [SessId, QosEntryId]) of
            {ok, #qos_entry{
                computing_provider = ProviderId,
                traverse_reqs = TraversReqs
            }} ->
                case ExpectedIsPossible of
                    true ->
                        str_utils:format(
                            "QoS is not fulfilled while it should be. ~n"
                            "Worker: ~p ~n"
                            "QosName: ~p ~n"
                            "ProviderId: ~p ~n"
                            "TraverseReqs: ~p ~n",
                            [Worker, QosName, ProviderId, TraversReqs]
                        );
                    false ->
                        str_utils:format(
                            "QoS is fulfilled while it shouldn't be. ~n"
                            "Worker: ~p ~n"
                            "QosName: ~p ~n", [Worker, QosName]
                        )
                end;
            {error, _} = Error ->
                str_utils:format(
                    "Error when checking if QoS is fulfilled. ~n"
                    "Worker: ~p~n"
                    "QosName: ~p~n"
                    "Error: ~p~n", [Worker, QosName, Error]
                )
        end,
        {lfm_proxy:check_qos_fulfilled(Worker, SessId, QosEntryId), ErrMsg}
    end,
    assert_match_with_err_msg(Fun, {ok, ExpectedIsPossible}, 3 * ?ATTEMPTS, 1000).


map_qos_names_to_ids(QosNamesList, QosNameIdMapping) ->
    [maps:get(QosName, QosNameIdMapping) || QosName <- QosNamesList].


get_provider_storage(Worker) ->
    {ok, [StorageId]}  = rpc:call(Worker, provider_logic, get_storage_ids, [?GET_DOMAIN_BIN(Worker)]),
    StorageId.


inject_storage_id(Workers, QosMockMap) ->
    lists:foldl(fun(Worker, Acc) ->
        [ProviderName | _] = binary:split(?GET_DOMAIN_BIN(Worker), <<".">>),
        StorageId = qos_tests_utils:get_provider_storage(Worker),
        Acc#{StorageId => maps:get(ProviderName, QosMockMap)}
    end, #{}, Workers).


%%%====================================================================
%%% Assertions
%%%====================================================================

assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping) ->
    assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, 1).

assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, Attempts) ->
    lists:foreach(fun(#expected_qos_entry{
        workers = WorkersOrUndef,
        qos_expression_in_rpn = QosExpressionRPN,
        replicas_num = ReplicasNum,
        file_key = FileKey,
        qos_name = QosName,
        computing_provider = ProviderId
    }) ->
        QosEntryId = QosEntryId = maps:get(QosName, QosNameIdMapping),
        % if not specified in tests spec, check document on all nodes
        Workers = ensure_workers(Config, WorkersOrUndef),

        lists:foreach(fun(Worker) ->
            FileUuid = case FileKey of
                {path, Path} ->
                    ?GET_FILE_UUID(Worker, ?SESS_ID(Config, Worker), Path);
                {uuid, Uuid} ->
                    Uuid
            end,
            assert_qos_entry_document(
                Worker, QosEntryId, FileUuid, QosExpressionRPN, ReplicasNum, Attempts, ProviderId
            )
        end, Workers)
    end, ExpectedQosEntries).

assert_qos_entry_document(Worker, QosEntryId, FileUuid, Expression, ReplicasNum, Attempts, ProviderId) ->
    ExpectedQosEntry = #qos_entry{
        file_uuid = FileUuid,
        expression = Expression,
        replicas_num = ReplicasNum,
        computing_provider = ProviderId
    },
    GetQosEntryFun = fun() ->
        ?assertMatch({ok, _Doc}, rpc:call(Worker, qos_entry, get, [QosEntryId]), Attempts),
        {ok, #document{value = QosEntry}} = rpc:call(Worker, qos_entry, get, [QosEntryId]),
        ErrMsg = str_utils:format(
            "Worker: ~p ~n"
            "Expected qos_entry: ~p ~n"
            "Got: ~p", [Worker, ExpectedQosEntry, QosEntry]
        ),
        {QosEntry, ErrMsg}
    end,
    assert_match_with_err_msg(GetQosEntryFun, ExpectedQosEntry, Attempts, 200).


assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, FilterOther) ->
    assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, FilterOther, 1).

assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, FilterOther, Attempts) ->
    lists:foreach(fun(#expected_file_qos{
        workers = WorkersOrUndef,
        path = FilePath,
        qos_entries = ExpectedQosEntriesNames,
        assigned_entries = ExpectedAssignedEntries
    }) ->
        % if not specified in tests spec, check document on all nodes
        Workers = ensure_workers(Config, WorkersOrUndef),
        ExpectedQosEntriesId = map_qos_names_to_ids(ExpectedQosEntriesNames, QosNameIdMapping),
        ExpectedAssignedEntriesId = maps:map(fun(_, QosNamesList) ->
            map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedAssignedEntries),

        lists:foreach(fun(W) ->
            SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
            FileUuid = ?GET_FILE_UUID(W, SessId, FilePath),
            assert_file_qos_document(
                W, FileUuid, ExpectedQosEntriesId, ExpectedAssignedEntriesId,
                FilePath, FilterOther, Attempts
            )
        end, Workers)
    end, ExpectedFileQos).


assert_file_qos_document(
    Worker, FileUuid, QosEntries, AssignedEntries, FilePath, FilterAssignedEntries, Attempts
) ->
    ExpectedFileQos = #file_qos{
        qos_entries = QosEntries,
        assigned_entries = case FilterAssignedEntries of
            true ->
                maps:filter(fun(Key, _Val) -> Key == get_provider_storage(Worker) end, AssignedEntries);
            false ->
                AssignedEntries
        end
    },
    ExpectedFileQosSorted = sort_file_qos(ExpectedFileQos),

    GetSortedFileQosFun = fun() ->
        {ok, #document{value = FileQos}} = ?assertMatch(
            {ok, _Doc},
            rpc:call(Worker, datastore_model, get, [file_qos:get_ctx(), FileUuid])
        ),
        FileQosSorted = sort_file_qos(FileQos),
        ErrMsg = str_utils:format(
            "Worker: ~p~n"
            "File: ~p~n"
            "Sorted file_qos: ~p~n"
            "Expected file_qos: ~p~n",
            [Worker, FilePath, FileQosSorted, ExpectedFileQos]
        ),
        {FileQosSorted, ErrMsg}
    end,
    assert_match_with_err_msg(GetSortedFileQosFun, ExpectedFileQosSorted, Attempts, 500).


assert_effective_qos(Config, ExpectedEffQosEntries, QosNameIdMapping, FilterAssignedEntries) ->
    assert_effective_qos(Config, ExpectedEffQosEntries, QosNameIdMapping, FilterAssignedEntries, 1).

assert_effective_qos(Config, ExpectedEffQosEntries, QosNameIdMapping, FilterAssignedEntries, Attempts) ->
    lists:foreach(fun(#expected_file_qos{
        workers = WorkersOrUndef,
        path = FilePath,
        qos_entries = ExpectedQosEntriesWithNames,
        assigned_entries = ExpectedAssignedEntries
    }) ->
        % if not specified in tests spec, check document on all nodes
        Workers = ensure_workers(Config, WorkersOrUndef),
        ExpectedQosEntriesId = qos_tests_utils:map_qos_names_to_ids(ExpectedQosEntriesWithNames, QosNameIdMapping),
        ExpectedAssignedEntriesId = maps:map(fun(_, QosNamesList) ->
            qos_tests_utils:map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedAssignedEntries),

        lists:foreach(fun(Worker) ->
            SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
            FileUuid = ?GET_FILE_UUID(Worker, SessId, FilePath),
            assert_effective_qos(
                Worker, FileUuid, FilePath, ExpectedQosEntriesId, ExpectedAssignedEntriesId,
                FilterAssignedEntries, Attempts
            ),

            % check that for file document has not been created
            ?assertMatch({error, not_found}, rpc:call(Worker, datastore_model, get, [file_qos:get_ctx(), FileUuid]))

        end, Workers)
    end, ExpectedEffQosEntries).

assert_effective_qos(
    Worker, FileUuid, FilePath, QosEntries, AssignedEntries, FilterAssignedEntries, Attempts
) ->
    ExpectedEffectiveQos = #effective_file_qos{
        qos_entries = QosEntries,
        assigned_entries = case FilterAssignedEntries of
            true -> maps:filter(fun(Key, _Val) -> Key == get_provider_storage(Worker) end, AssignedEntries);
            false -> AssignedEntries
        end
    },
    ExpectedEffectiveQosSorted = sort_effective_qos(ExpectedEffectiveQos),

    GetSortedEffectiveQos = fun() ->
        {ok, EffQos} = rpc:call(Worker, file_qos, get_effective, [FileUuid]),
        EffQosSorted = sort_effective_qos(EffQos),
        ErrMsg = str_utils:format(
            "Worker: ~p~n"
            "File: ~p~n"
            "Sorted effective QoS: ~p~n"
            "Expected effective QoS: ~p~n",
            [Worker, FilePath, EffQosSorted, ExpectedEffectiveQos]
        ),
        {EffQosSorted, ErrMsg}
    end,
    assert_match_with_err_msg(GetSortedEffectiveQos, ExpectedEffectiveQosSorted, Attempts, 500).


assert_distribution_in_dir_structure(_, undefined, _) ->
    true;

assert_distribution_in_dir_structure(Config, #test_dir_structure{
    assertion_workers = WorkersOrUndef,
    dir_structure = ExpectedDirStructure
}, GuidsAndPaths) ->
    % if not specified in tests spec, check document on all nodes
    Workers = ensure_workers(Config, WorkersOrUndef),

    assert_distribution_in_dir_structure(Config, Workers, ExpectedDirStructure, <<"/">>, GuidsAndPaths, ?ATTEMPTS).

assert_distribution_in_dir_structure(_Config, _Workers, _DirStructure, _Path, _GuidsAndPaths, 0) ->
    false;

assert_distribution_in_dir_structure(Config, Workers, DirStructure, Path, GuidsAndPaths, Attempts) ->
    PrintError = Attempts == 1,
    case assert_file_distribution(Config, Workers, DirStructure, Path, PrintError, GuidsAndPaths) of
        true ->
            true;
        false ->
            timer:sleep(timer:seconds(1)),
            assert_distribution_in_dir_structure(Config, Workers, DirStructure, Path, GuidsAndPaths, Attempts - 1)
    end.


assert_file_distribution(Config, Workers, {DirName, DirContent}, Path, PrintError, GuidsAndPaths) ->
    lists:foldl(fun(Child, Matched) ->
        DirPath = filename:join(Path, DirName),
        case assert_file_distribution(Config, Workers, Child, DirPath, PrintError, GuidsAndPaths) of
            true ->
                Matched;
            false ->
                false
        end
    end, true, DirContent);

assert_file_distribution(Config, Workers, {FileName, FileContent, ExpectedFileDistribution},
    Path, PrintError, GuidsAndPaths
) ->
    FilePath = filename:join(Path, FileName),
    FileGuid = get_guid(FilePath, GuidsAndPaths),

    lists:foldl(fun(Worker, Res) ->
        SessId = ?SESS_ID(Config, Worker),
        ExpectedDistributionSorted = lists:sort(
            fill_in_expected_distribution(ExpectedFileDistribution, FileContent)
        ),

        FileLocationsSorted = case lfm_proxy:get_file_distribution(Worker, SessId, {guid, FileGuid}) of
            {ok, FileLocations} ->
                lists:sort(FileLocations);
            Error ->
                Error
        end,

        case {FileLocationsSorted == ExpectedDistributionSorted, PrintError} of
            {false, false} ->
                false;
            {false, true} ->
                ct:pal(
                    "Wrong file distribution for ~p on worker ~p. ~n"
                    "Expected: ~p~n"
                    "Got: ~p~n",
                    [FilePath, Worker, ExpectedDistributionSorted, FileLocationsSorted]);
            {true, _} ->
                Res
        end
    end, true, Workers).


%%%====================================================================
%%% Mocks
%%%====================================================================

mock_space_storages(Config, StorageList) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_expression, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_expression, get_space_storages,
        fun(_) ->
            StorageList
        end).


mock_storage_qos(Workers, StorageQos) ->
    test_utils:mock_expect(Workers, storage_logic, get_qos_parameters, fun(StorageId, _SpaceId) ->
        {ok, maps:get(StorageId, StorageQos, #{})}
    end).


mock_synchronize_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, replica_synchronizer, [passthrough]),
    ok = test_utils:mock_expect(Workers, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _) ->
            {ok, ok}
        end).


%%%====================================================================
%%% Internal functions
%%%====================================================================

sort_file_qos(FileQos) ->
    FileQos#file_qos{
        qos_entries = lists:sort(FileQos#file_qos.qos_entries),
        assigned_entries = maps:map(fun(_, QosEntriesForStorage) ->
            lists:sort(QosEntriesForStorage)
        end, FileQos#file_qos.assigned_entries)
    }.

sort_effective_qos(EffectiveQos) ->
    EffectiveQos#effective_file_qos{
        qos_entries = lists:sort(EffectiveQos#effective_file_qos.qos_entries),
        assigned_entries = maps:map(fun(_, QosEntriesForStorage) ->
            lists:sort(QosEntriesForStorage)
        end, EffectiveQos#effective_file_qos.assigned_entries)
    }.


ensure_workers(Config, Undef) when Undef == undefined ->
    Workers = get_op_nodes_sorted(Config),
    Workers;

ensure_workers(_Config, Workers) ->
    Workers.


ensure_worker(Config, Undef) when Undef == undefined ->
    Workers = get_op_nodes_sorted(Config),
    hd(Workers);

ensure_worker(_Config, Worker) ->
    Worker.


assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected, Attempts, _Sleep) when Attempts =< 1 ->
    try
        {ActualVal, ErrMsg} = GetActualValAndErrMsgFun(),
        try
            ?assertMatch(Expected, ActualVal),
            ok
        catch
            error:{assertMatch_failed, _} = Error ->
                ct:pal(ErrMsg),
                error(Error)
        end
    catch
        Error2 ->
            ct:pal(Error2)
    end;

assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected , Attempts, Sleep) ->
    try
        {ActualVal, _ErrMsg} = GetActualValAndErrMsgFun(),
        case ActualVal of
            Expected ->
                ?assertMatch(Expected, ActualVal),
                ok;
            _ ->
                timer:sleep(Sleep),
                assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected, Attempts - 1, Sleep)
        end
    catch
        _ ->
            timer:sleep(Sleep),
            assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected, Attempts - 1, Sleep)
    end.

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
    mock_providers_qos/2,
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
    map_qos_names_to_ids/2
]).


-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).
-define(ATTEMPTS, 60).
-define(SESS_ID(Config, Worker), ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config)).
-define(GET_FILE_UUID(Worker, SessId, FilePath),
    file_id:guid_to_uuid(qos_tests_utils:get_guid(Worker, SessId, FilePath))
).
-define(PROVIDER_ID(Worker), initializer:domain_to_provider_id(?GET_DOMAIN(Worker))).


%%%====================================================================
%%% Util functions
%%%====================================================================

fulfill_qos_test_base(Config, TestSpec) ->
    #fulfill_qos_test_spec{
        initial_dir_structure = InitialDirStructure,
        qos_to_add = QosToAddList,
        wait_for_qos_fulfillment = WaitForQos,
        expected_qos_entries = ExpectedQosEntries,
        expected_file_qos = ExpectedFileQos,
        expected_dir_structure = ExpectedDirStructure
    } = TestSpec,

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
        ProviderId1 = initializer:domain_to_provider_id(?GET_DOMAIN(Worker1)),
        ProviderId2 = initializer:domain_to_provider_id(?GET_DOMAIN(Worker2)),
        ProviderId1 =< ProviderId2
    end,
    lists:sort(SortingFun, Workers).


add_multiple_qos_in_parallel(Config, QosToAddList) ->
    Results = utils:pmap(fun(QosToAdd) -> add_qos(Config, QosToAdd) end, QosToAddList),
    ?assert(lists:all(fun(Result) ->
        case Result of
            {ok, {_QosName, _QosId}} ->
                true;
            _ ->
                false
        end
    end, Results)),
    maps:from_list(lists:map(fun({ok, Result}) -> Result end, Results)).


add_qos(Config, QosToAdd) ->
    #qos_to_add{
        worker = WorkerOrUndef,
        qos_name = QosName,
        path = FilePath,
        replicas_num = ReplicasNum,
        expression = QosExpression
    } = QosToAdd,

    % use first worker from sorted worker list if worker is not specified
    Worker = ensure_worker(Config, WorkerOrUndef),
    SessId = ?SESS_ID(Config, Worker),

    % ensure file exists
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {path, FilePath}), ?ATTEMPTS),

    {ok, QosId} = ?assertMatch(
        {ok, _QosId},
        lfm_proxy:add_qos(Worker, SessId, {path, FilePath}, QosExpression, ReplicasNum)
    ),
    {ok, {QosName, QosId}}.


create_dir_structure(Config, TestDirStructure) ->
    #test_dir_structure{
        worker = WorkerOrUndef,
        dir_structure = DirStructureToCreate
    } = TestDirStructure,

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
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
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
        QosId = ?GET_QOS_ID(QosName, QosNameIdMapping),

        % try to find expected QoS entry associated with QoS name and get
        % expected value for is_possible field. If no QoS entry is found
        % assert "true" value for is_possible field.
        MbyExpectedQosEntry = lists:filter(fun(Entry) ->
            Entry#expected_qos_entry.qos_name == QosName
        end, ExpectedQosEntries),
        ExpectedIsPossible = case MbyExpectedQosEntry of
            [ExpectedQosEntry] ->
                ExpectedQosEntry#expected_qos_entry.is_possible;
            [] ->
                true
        end,

        % wait for QoS fulfillment on different worker nodes
        utils:pmap(fun(Worker) ->
            wait_for_qos_fulfilment_in_parallel(Config, Worker, QosId, QosName, ExpectedIsPossible)
        end, WorkerList)
    end, QosToWaitForList),

    ?assert(lists:all(fun(Result) -> Result =:= ok end, lists:flatten(Results))).

wait_for_qos_fulfilment_in_parallel(Config, Worker, QosId, QosName, ExpectedIsPossible) ->
    SessId = ?SESS_ID(Config, Worker),
    Fun = fun() ->
        ErrMsg = case rpc:call(Worker, lfm_qos, get_qos_details, [SessId, QosId]) of
            {ok, #qos_entry{
                is_possible = IsPossible,
                traverse_reqs = TraversReqs,
                traverses = Traverses
            }} ->
                case ExpectedIsPossible of
                    true ->
                        str_utils:format(
                            "QoS is not fulfilled while it should be. ~n"
                            "Worker: ~p ~n"
                            "QosName: ~p ~n"
                            "IsPossible: ~p ~n"
                            "TraverseReqs: ~p ~n"
                            "Traverses: ~p ~n",
                            [Worker, QosName, IsPossible, TraversReqs, Traverses]
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
        {lfm_proxy:check_qos_fulfilled(Worker, SessId, QosId), ErrMsg}
    end,
    assert_match_with_err_msg(Fun, ExpectedIsPossible, 3 * ?ATTEMPTS, 1000).


map_qos_names_to_ids(QosNamesList, QosNameIdMapping) ->
    [maps:get(QosName, QosNameIdMapping) || QosName <- QosNamesList].


%%%====================================================================
%%% Assertions
%%%====================================================================

assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping) ->
    assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, 1).

assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, Attempts) ->
    lists:foreach(fun(ExpectedQosEntry) ->
        #expected_qos_entry{
            workers = WorkersOrUndef,
            is_possible = IsPossible,
            qos_expression_in_rpn = QosExpressionRPN,
            replicas_num = ReplicasNum,
            file_key = FileKey,
            qos_name = QosName
        } = ExpectedQosEntry,
        QosId = ?GET_QOS_ID(QosName, QosNameIdMapping),
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
                Worker, QosId, FileUuid, QosExpressionRPN, ReplicasNum, IsPossible, Attempts
            )
        end, Workers)
    end, ExpectedQosEntries).

assert_qos_entry_document(Worker, QosId, FileUuid, Expression, ReplicasNum, IsPossible, Attempts) ->
    ExpectedQosEntry = #qos_entry{
        file_uuid = FileUuid,
        expression = Expression,
        replicas_num = ReplicasNum,
        is_possible = IsPossible
    },
    GetQosEntryFun = fun() ->
        ?assertMatch({ok, _Doc}, rpc:call(Worker, qos_entry, get, [QosId]), Attempts),
        {ok, #document{value = QosEntry}} = rpc:call(Worker, qos_entry, get, [QosId]),
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
    lists:foreach(fun(FileQos) ->
        #expected_file_qos{
            workers = WorkersOrUndef,
            path = FilePath,
            qos_entries = ExpectedQosEntriesNames,
            target_storages = ExpectedTargetStorages
        } = FileQos,
        % if not specified in tests spec, check document on all nodes
        Workers = ensure_workers(Config, WorkersOrUndef),
        ExpectedQosEntriesId = map_qos_names_to_ids(ExpectedQosEntriesNames, QosNameIdMapping),
        ExpectedTargetStoragesId = maps:map(fun(_, QosNamesList) ->
            map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedTargetStorages),

        lists:foreach(fun(W) ->
            SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
            FileUuid = ?GET_FILE_UUID(W, SessId, FilePath),
            assert_file_qos_document(
                W, FileUuid, ExpectedQosEntriesId, ExpectedTargetStoragesId, 
                FilePath, FilterOther, Attempts
            )
        end, Workers)
    end, ExpectedFileQos).


assert_file_qos_document(Worker, FileUuid, QosEntries, TargetStorages, FilePath, FilterTS, Attempts) ->
    ExpectedFileQos = #file_qos{
        qos_entries = QosEntries,
        target_storages = case FilterTS of
            true ->
                maps:filter(fun(Key, _Val) -> Key == ?PROVIDER_ID(Worker) end, TargetStorages);
            false ->
                TargetStorages
        end
    },
    ExpectedFileQosSorted = sort_file_qos(ExpectedFileQos),

    GetSortedFileQosFun = fun() ->
        {ok, #document{value = FileQos}} = ?assertMatch({ok, _Doc}, rpc:call(Worker, file_qos, get, [FileUuid])),
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


assert_effective_qos(Config, ExpectedEffQosEntries, QosNameIdMapping, FilterTS) ->
    assert_effective_qos(Config, ExpectedEffQosEntries, QosNameIdMapping, FilterTS, 1).

assert_effective_qos(Config, ExpectedEffQosEntries, QosNameIdMapping, FilterTS, Attempts) ->
    lists:foreach(fun(ExpectedEffQos) ->
        #expected_file_qos{
            workers = WorkersOrUndef,
            path = FilePath,
            qos_entries = ExpectedQosEntriesWithNames,
            target_storages = ExpectedTargetStorages
        } = ExpectedEffQos,
        % if not specified in tests spec, check document on all nodes
        Workers = ensure_workers(Config, WorkersOrUndef),
        ExpectedQosEntriesId = qos_tests_utils:map_qos_names_to_ids(ExpectedQosEntriesWithNames, QosNameIdMapping),
        ExpectedTargetStoragesId = maps:map(fun(_, QosNamesList) ->
            qos_tests_utils:map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedTargetStorages),

        lists:foreach(fun(Worker) ->
            SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
            FileUuid = ?GET_FILE_UUID(Worker, SessId, FilePath),
            assert_effective_qos(
                Worker, FileUuid, FilePath, ExpectedQosEntriesId, ExpectedTargetStoragesId,
                FilterTS, Attempts
            ),

            % check that for file document has not been created
            ?assertMatch({error, not_found}, rpc:call(Worker, file_qos, get, [FileUuid]))

        end, Workers)
    end, ExpectedEffQosEntries).

assert_effective_qos(Worker, FileUuid, FilePath, QosEntries, TargetStorages, FilterTS, Attempts) ->
    ExpectedEffectiveQos = #file_qos{
        qos_entries = QosEntries,
        target_storages = case FilterTS of
            true -> maps:filter(fun(Key, _Val) -> Key == ?PROVIDER_ID(Worker) end, TargetStorages);
            false -> TargetStorages
        end
    },
    ExpectedEffectiveQosSorted = sort_file_qos(ExpectedEffectiveQos),

    GetSortedEffectiveQos = fun() ->
        EffQos = rpc:call(Worker, file_qos, get_effective, [FileUuid]),
        EffQosSorted = sort_file_qos(EffQos),
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

assert_distribution_in_dir_structure(Config, TestDirStructure, GuidsAndPaths) ->
    #test_dir_structure{
        assertion_workers = WorkersOrUndef,
        dir_structure = ExpectedDirStructure
    } = TestDirStructure,
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
    test_utils:mock_new(Workers, qos_req, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_req, get_space_storages,
        fun(_, _) ->
            StorageList
        end).


mock_providers_qos(Config, ProvidersQos) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, providers_qos),
    test_utils:mock_expect(Workers, providers_qos, get_storage_qos,
        fun(StorageId, _StorageSet) ->
            % names of test providers start with p1, p2 etc.
            maps:get(binary:part(StorageId, 0, 2), ProvidersQos)
        end).


mock_synchronize_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_traverse, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_traverse, synchronize_file,
        fun(_, _) ->
            ok
        end).


%%%====================================================================
%%% Internal functions
%%%====================================================================

sort_file_qos(FileQos) ->
    FileQos#file_qos{
        qos_entries = lists:sort(FileQos#file_qos.qos_entries),
        target_storages = maps:map(fun(_, QosEntriesForStorage) ->
            lists:sort(QosEntriesForStorage)
        end, FileQos#file_qos.target_storages)
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

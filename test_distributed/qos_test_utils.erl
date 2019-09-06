%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utility functions for qos tests.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_utils).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([add_qos_test_base/2, mock_providers_qos/2, get_guid/2]).


-define(ATTEMPTS, 60).
-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

add_qos_test_base(Config, Spec) ->
    #{
        source_provider := SourceProvider,
        directory_structure_before := DirStructure,
        qos := QosToAddList
    } = Spec,

    AssertionWorkers = maps:get(assertion_workers, Spec, ?config(op_worker_nodes, Config)),

    SessId = fun(Worker) ->
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,

    % create initial dir structure, check initial distribution
    GuidsAndPaths = create_dir_structure(SourceProvider, SessId(SourceProvider), DirStructure, <<"/">>),
    lists:foreach(fun(Worker) ->
        ?assertMatch(true, assert_distribution_in_dir_structure(Worker,
            SessId(Worker), DirStructure, <<"/">>, GuidsAndPaths, ?ATTEMPTS))
    end, AssertionWorkers),

    % add qos specified in test spec
    PathToFileQos = add_qos_in_parallel(SourceProvider, SessId(SourceProvider), QosToAddList, GuidsAndPaths),
    case maps:get(perform_checks, Spec, true) of
        true ->
            wait_for_qos_fulfilment_in_parallel(SourceProvider, SessId(SourceProvider), PathToFileQos),

            % check distribution after qos is fulfilled, check file_qos
            case maps:find(directory_structure_after, Spec) of
                {ok, DirStructureAfter} ->
                    ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider,
                        SessId(SourceProvider), DirStructureAfter, <<"/">>, GuidsAndPaths, ?ATTEMPTS));
                _ ->
                    ok
            end,
            case maps:find(files_qos, Spec) of
                {ok, ExpectedFilesQosEntries} ->
                    ?assertMatch(true, assert_file_qos(SourceProvider, SessId(SourceProvider), ExpectedFilesQosEntries, PathToFileQos));
                _ ->
                    ok
            end,

            case maps:is_key(add_files_after_qos_fulfilled, Spec) of
                true ->
                    #{
                        add_files_after_qos_fulfilled := NewFiles,
                        qos_invalidated := QosInvalidated,
                        dir_structure_with_new_files := NewDirStructure
                    } = Spec,

                    create_dir_structure(SourceProvider, SessId(SourceProvider), NewFiles, <<"/">>),

                    % check file distribution and that qos is not fulfilled
                    ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider, SessId(SourceProvider),
                        NewFiles, <<"/">>, GuidsAndPaths, ?ATTEMPTS)),

                    % check that qos have been invalidated
                    InvalidatedQos = assert_qos_invalidated(SourceProvider, SessId(SourceProvider), QosInvalidated, PathToFileQos, GuidsAndPaths),

                    % get qos record
                    wait_for_qos_fulfilment_in_parallel(SourceProvider, SessId(SourceProvider), InvalidatedQos),

                    ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider,
                        SessId(SourceProvider), NewDirStructure, <<"/">>, GuidsAndPaths, ?ATTEMPTS));
                false ->
                    ok
            end;
        false ->
            ok
    end,
    {GuidsAndPaths, PathToFileQos}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_dir_structure(Worker, SessionId, {DirName, DirContent}, Path) ->
    DirPath = filename:join(Path, DirName),
    Map = case lfm_proxy:mkdir(Worker, SessionId, DirPath) of
        {ok, DirGuid} -> #{dirs => [{DirGuid, DirPath}]};
        _Error -> #{}
    end,
    lists:foldl(fun(Child, #{files := Files, dirs := Dirs}) ->
        #{files := NewFiles, dirs := NewDirs} = create_dir_structure(Worker, SessionId, Child, DirPath),
        #{files => Files ++ NewFiles, dirs => Dirs ++ NewDirs}
    end, maps:merge(#{files => [], dirs => []}, Map), DirContent);
create_dir_structure(Worker, SessionId, {FileName, FileContent, _FileDistribution}, Path) ->
    FilePath = filename:join(Path, FileName),
    #{files => [{create_test_file(Worker, SessionId, FilePath, FileContent), FilePath}], dirs => []}.


create_test_file(Worker, SessionId, File, TestData) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(Worker, Handle, 0, TestData),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.

assert_distribution_in_dir_structure(_Worker, _SessionId, _DirStructure, _Path, _GuidsAndPaths, 0) ->
    false;
assert_distribution_in_dir_structure(Worker, SessionId, DirStructure, Path, GuidsAndPaths, Attempts) ->
    PrintError = Attempts == 1,
    case assert_file_distribution(Worker, SessionId, DirStructure, Path, PrintError, GuidsAndPaths) of
        true ->
            true;
        false ->
            timer:sleep(timer:seconds(1)),
            assert_distribution_in_dir_structure(Worker, SessionId, DirStructure, Path, GuidsAndPaths, Attempts - 1)
    end.

assert_file_distribution(Worker, SessionId, {DirName, DirContent}, Path, PrintError, GuidsAndPaths) ->
    lists:foldl(fun(Child, Matched) ->
        DirPath = filename:join(Path, DirName),
        case assert_file_distribution(Worker, SessionId, Child, DirPath, PrintError, GuidsAndPaths) of
            true ->
                Matched;
            false ->
                false
        end
    end, true, DirContent);
assert_file_distribution(Worker, SessId, {FileName, FileContent, ExpectedFileDistribution}, Path, PrintError, GuidsAndPaths) ->
    FilePath = filename:join(Path, FileName),
    FileGuid = get_guid(FilePath, GuidsAndPaths),
    {ok, FileLocations} = lfm_proxy:get_file_distribution(Worker, SessId, {guid, FileGuid}),
    ExpectedDistributionSorted = lists:sort(
        fill_in_expected_distribution(ExpectedFileDistribution, FileContent)
    ),
    FileLocationsSorted = lists:sort(FileLocations),

    case FileLocationsSorted == ExpectedDistributionSorted of
        true ->
            true;
        false ->
            case PrintError of
                true ->
                    ct:pal("Wrong file distribution for ~p. ~n"
                    "    Expected: ~p~n"
                    "    Got: ~p~n", [FilePath, ExpectedDistributionSorted, FileLocationsSorted]),
                    false;
                false ->
                    false
            end
    end.

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

assert_file_qos(Worker, SessId, ExpectedFileQosEntries, QosDescList) ->
    lists:all(fun(ExpectedFileQos) ->
        #{
            paths := PathsList,
            qos_list := ExpectedQosEntriesWithNames,
            target_providers := ExpectedTargetProvidersWithNames
        } = ExpectedFileQos,

        lists:foldl(fun(Path, Matched) ->
            % get actual file qos
            {ok, {QosEntries, TargetStorages}} = lfm_proxy:get_file_qos(Worker, SessId, {path, Path}),

            % in test spec we pass qos name, now we have to change it to qos id
            ExpectedQosEntries = lists:map(fun(QosName) ->
                QosDesc = lists:keyfind(QosName, 1, QosDescList),
                element(2, QosDesc)
            end, ExpectedQosEntriesWithNames),

            % sort both expected and actual qos_list and check if they match
            ExpectedQosEntriesSorted = lists:sort(ExpectedQosEntries),
            FileQosSorted = lists:sort(QosEntries),
            QosEntriesMatched = ExpectedQosEntriesSorted == FileQosSorted,
            case QosEntriesMatched of
                true ->
                    ok;
                false ->
                    ct:pal("Wrong qos_list for: ~p~n"
                    "    Expected: ~p~n"
                    "    Got: ~p~n", [Path, ExpectedQosEntries, FileQosSorted])
            end,

            % again in test spec we have qos names, need to change them to qos ids
            % in the same time all qos id lists are sorted
            ExpectedTargetProvidersSorted = maps:map(fun(_ProvId, QosNamesList) ->
                lists:sort(
                    lists:map(fun(QosName) ->
                        QosDesc = lists:keyfind(QosName, 1, QosDescList),
                        element(2, QosDesc)
                    end, QosNamesList)
                )
            end, ExpectedTargetProvidersWithNames),

            % sort qos id lists in actual target providers
            TargetProvidersSorted = maps:map(fun(_ProvId, QosIdList) ->
                lists:sort(QosIdList)
            end, TargetStorages),

            TargetProvidersMatched = TargetProvidersSorted == ExpectedTargetProvidersSorted,
            case TargetProvidersMatched of
                true ->
                    ok;
                false ->
                    ct:pal("Wrong target providers for: ~p~n"
                    "    Expected: ~p~n"
                    "    Got: ~p~n", [Path, ExpectedTargetProvidersSorted,
                        TargetProvidersSorted])
            end,
            case QosEntriesMatched andalso TargetProvidersMatched of
                true ->
                    Matched;
                false ->
                    false
            end
        end, true, PathsList)
    end, ExpectedFileQosEntries).

assert_qos_invalidated(Worker, SessId, QosToCheckList, QosDescList, GuidsAndPaths) ->
    lists:foldl(fun(InvalidatedQosMap, InvalidatedQosPartial) ->
        #{
            name := QosName,
            path := Path
        } = InvalidatedQosMap,

        FileGuid = get_guid(Path, GuidsAndPaths),
        {ok, #file_qos{qos_entries = FileQos}} = ?assertMatch(
            {ok, {_,_}}, lfm_proxy:get_file_qos(Worker, SessId, {guid, FileGuid})
        ),

        QosId = element(2, lists:keyfind(QosName, 1, QosDescList)),
        ?assertMatch(false, rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, FileQos])),
        ?assertMatch(false, rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, QosId])),

        [{QosName, QosId, Path} | InvalidatedQosPartial]
    end, [], QosToCheckList).

wait_for_qos_fulfilment_in_parallel(Worker, SessId, QosPathToId) ->
    utils:pforeach(fun({QosName, QosId, _Path}) ->
        {ok, QosRecord} = ?assertMatch({ok, _}, lfm_proxy:get_qos_details(Worker, SessId, QosId)),
        ct:pal("Waiting for fulfilment of qos ~p: ~n"
        "    Expression:          ~p~n",
            [QosName, QosRecord#qos_entry.expression]
        ),
        ?assertEqual(true, rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, QosId]), ?ATTEMPTS)
    end, QosPathToId).

add_qos_in_parallel(Worker, SessId, QosToAddList, GuidsAndPaths) ->
    utils:pmap(fun(QosToAdd) ->
        #{
            name := QosName,
            expression := QosExpression,
            path := Path
        } = QosToAdd,
        ReplicasNum = maps:get(replicas_num, QosToAdd, 1),
        add_qos(Worker, SessId, Path, QosExpression, ReplicasNum, QosName, GuidsAndPaths)
    end, QosToAddList).

add_qos(Worker, SessId, Path, QosExpression, ReplicasNum, QosName, GuidsAndPaths) ->
    ct:pal("Adding qos with expression: ~p for file: ~p~n", [QosExpression, Path]),
    Guid = get_guid(Path, GuidsAndPaths),
    {ok, QosId} = ?assertMatch(
        {ok, _QosId},
        lfm_proxy:add_qos(Worker, SessId, {guid, Guid}, QosExpression, ReplicasNum)
    ),

    {QosName, QosId, Path}.

mock_providers_qos(Config, Mock) ->
    Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Workers, providers_qos),
    test_utils:mock_expect(Workers, providers_qos, get_provider_qos,
        fun (ProviderId) ->
            maps:get(ProviderId, Mock, #{})
        end).

get_guid(Path, #{files := FilesGuidsAndPaths, dirs := DirsGuidsAndPaths}) ->
    lists:foldl(fun({Guid, P}, _) when P == Path -> Guid;
                   ({_, _}, Acc) -> Acc
    end, undefined, FilesGuidsAndPaths ++ DirsGuidsAndPaths).

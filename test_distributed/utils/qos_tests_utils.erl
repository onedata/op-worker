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
-include("rest_test_utils.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


% util functions
-export([
    fulfill_qos_test_base/1,
    get_guid/2, get_guid/3,
    create_dir_structure/1, create_dir_structure/4,
    create_file/4, create_file/5, create_directory/3,
    create_and_open/4,
    wait_for_qos_fulfillment_in_parallel/2,
    add_qos/1, add_multiple_qos/1,
    map_qos_names_to_ids/2,
    set_qos_parameters/3, reset_qos_parameters/0
]).

% mock related functions
-export([
    mock_transfers/1,
    wait_for_file_transfer_start/1,
    finish_transfers/1, finish_transfers/2,
    finish_all_transfers/0,
    mock_replica_synchronizer/2
]).

% assertions
-export([
    assert_distribution_in_dir_structure/2,
    assert_effective_qos/3, assert_effective_qos/4,
    assert_file_qos_documents/3, assert_file_qos_documents/4,
    assert_qos_entry_documents/2, assert_qos_entry_documents/3,
    gather_not_matching_statuses_on_all_nodes/3
]).

-define(USER_PLACEHOLDER, user2).
-define(SPACE, space1).
-define(SESS_ID(ProviderPlaceholder), oct_background:get_user_session_id(?USER_PLACEHOLDER, ProviderPlaceholder)).
-define(GET_FILE_UUID(Node, SessId, FilePath),
    file_id:guid_to_uuid(qos_tests_utils:get_guid(Node, SessId, FilePath))
).

-define(ATTEMPTS, 60).

%%%====================================================================
%%% Util functions
%%%====================================================================

fulfill_qos_test_base(#fulfill_qos_test_spec{
    initial_dir_structure = InitialDirStructure,
    qos_to_add = QosToAddList,
    wait_for_qos_fulfillment = WaitForQos,
    expected_qos_entries = ExpectedQosEntries,
    expected_file_qos = ExpectedFileQos,
    expected_dir_structure = ExpectedDirStructure
}) ->
    % create initial dir structure
    GuidsAndPaths = create_dir_structure(InitialDirStructure),
    ?assertMatch(true, assert_distribution_in_dir_structure(InitialDirStructure, GuidsAndPaths)),

    % add QoS and w8 for fulfillment
    QosNameIdMapping = add_multiple_qos(QosToAddList),
    WaitForQos andalso wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntries),

    % check file distribution and qos documents
    ?assertMatch(ok, assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping, ?ATTEMPTS)),
    ?assertMatch(ok, assert_file_qos_documents(ExpectedFileQos, QosNameIdMapping, true, ?ATTEMPTS)),
    ?assertMatch(true, assert_distribution_in_dir_structure(ExpectedDirStructure, GuidsAndPaths)),
    {GuidsAndPaths, QosNameIdMapping}.


add_multiple_qos(QosToAddList) ->
    lists:foldl(fun(QosToAdd, Acc) ->
        {ok, {QosName, QosEntryId}} = add_qos(QosToAdd),
        Acc#{QosName => QosEntryId}
    end, #{}, QosToAddList).


add_qos(#qos_to_add{
    provider_selector = ProviderOrUndef,
    qos_name = QosName,
    path = FilePath,
    replicas_num = ReplicasNum,
    expression = QosExpression
}) ->
    % use first provider from list if provider is not specified
    Provider = ensure_provider(ProviderOrUndef),
    SessId = ?SESS_ID(Provider),
    Node = oct_background:get_random_provider_node(Provider),

    % ensure file exists
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, {path, FilePath}), ?ATTEMPTS),

    case add_qos_by_rest(Provider, FilePath, QosExpression, ReplicasNum) of
        {ok, RespBody} ->
            DecodedBody = json_utils:decode(RespBody),
            #{<<"qosRequirementId">> := QosEntryId} = ?assertMatch(#{<<"qosRequirementId">> := _}, DecodedBody),
            {ok, {QosName, QosEntryId}};
        {error, _} = Error -> Error
    end.


add_qos_by_rest(Provider, FilePath, QosExpression, ReplicasNum) ->
    Node = oct_background:get_random_provider_node(Provider),
    FileGuid = get_guid(Node, ?SESS_ID(Provider), FilePath),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    URL = <<"qos_requirements">>,
    Headers = [rest_test_utils:user_token_header(oct_background:get_user_access_token(?USER_PLACEHOLDER)), 
        {?HDR_CONTENT_TYPE, <<"application/json">>}],
    ReqBody = #{
        <<"expression">> => QosExpression,
        <<"replicasNum">> => ReplicasNum,
        <<"fileId">> => FileObjectId
    },
    SpaceId = oct_background:get_space_id(?SPACE),
    make_rest_request(Node, URL, post, Headers, ReqBody, SpaceId, [?SPACE_MANAGE_QOS]).


create_dir_structure(undefined) ->
    #{};
    
create_dir_structure(#test_dir_structure{
    provider = ProviderOrUndef,
    dir_structure = DirStructureToCreate
}) ->
    % use first provider from list if provider is not specified
    Provider = ensure_provider(ProviderOrUndef),
    SessionId = ?SESS_ID(Provider),
    create_dir_structure(Provider, SessionId, DirStructureToCreate, <<"/">>).

create_dir_structure(Provider, SessionId, {DirName, DirContent}, CurrPath) when is_list(DirContent) ->
    DirPath = filename:join(CurrPath, DirName),
    Map = case CurrPath == <<"/">> of
        true ->
            % skip creating space directory
            #{files => [], dirs => []};
        false ->
            DirGuid = create_directory(Provider, SessionId, DirPath),
            #{dirs => [{DirGuid, DirPath}]}
    end,
    lists:foldl(fun(Child, #{files := Files, dirs := Dirs}) ->
        #{files := NewFiles, dirs := NewDirs} = create_dir_structure(Provider, SessionId, Child, DirPath),
        #{files => Files ++ NewFiles, dirs => Dirs ++ NewDirs}
    end, maps:merge(#{files => [], dirs => []}, Map), DirContent);

create_dir_structure(Provider, SessionId, {FileName, FileContent}, CurrPath) ->
    create_dir_structure(Provider, SessionId, {FileName, FileContent, undefined}, CurrPath);

create_dir_structure(Provider, SessionId, {FileName, FileContent, FileDistribution}, CurrPath) ->
    create_dir_structure(Provider, SessionId, {FileName, FileContent, FileDistribution, reg_file}, CurrPath);

create_dir_structure(Provider, SessionId, {FileName, FileContent, _FileDistribution, TypeSpec}, CurrPath) ->
    FilePath = filename:join(CurrPath, FileName),
    FileGuid = create_file(Provider, SessionId, FilePath, FileContent, TypeSpec),
    #{files => [{FileGuid, FilePath}], dirs => []}.


create_directory(Provider, SessionId, DirPath) ->
    {ok, DirGuid} = ?assertMatch({ok, _DirGuid}, lfm_proxy:mkdir(
        oct_background:get_random_provider_node(Provider), SessionId, DirPath)),
    DirGuid.


create_file(Provider, SessionId, Path, FileContent) ->
    create_file(Provider, SessionId, Path, FileContent, reg_file).

create_file(Provider, SessionId, Path, FileContent, TypeSpec) ->
    Node = oct_background:get_random_provider_node(Provider),
    {ok, ParentGuid} = lfm_proxy:resolve_guid(Node, SessionId, filename:dirname(Path)),
    {ok, {FileGuid, Handle}} = ?assertMatch({ok, {_, _}}, create_and_open(Node, SessionId, ParentGuid, filename:basename(Path), TypeSpec)),
    Size = size(FileContent),
    ?assertMatch({ok, Size}, lfm_proxy:write(Node, Handle, 0, FileContent)),
    ?assertMatch(ok, lfm_proxy:fsync(Node, Handle)),
    ?assertMatch(ok, lfm_proxy:close(Node, Handle)),
    FileGuid.

create_and_open(Node, SessId, ParentGuid, TypeSpec) ->
    create_and_open(Node, SessId, ParentGuid, generator:gen_name(), TypeSpec).

create_and_open(Node, SessId, ParentGuid, Filename, reg_file) ->
    lfm_proxy:create_and_open(Node, SessId, ParentGuid, Filename, ?DEFAULT_FILE_PERMS);
create_and_open(Node, SessId, ParentGuid, Filename, {hardlink, FileToLinkGuid}) ->
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(Node, SessId, ?FILE_REF(FileToLinkGuid), ?FILE_REF(ParentGuid), Filename),
    {ok, Handle} = lfm_proxy:open(Node, SessId, ?FILE_REF(LinkGuid), rdwr),
    {ok, {LinkGuid, Handle}}.


fill_in_expected_distribution(ExpectedDistribution, FileContent) ->
    NotEmptyDistributionMap = lists:foldl(fun(ProviderDistributionOrId, Acc) ->
        case is_map(ProviderDistributionOrId) of
            true ->
                ProviderDistribution = ProviderDistributionOrId,
                ProviderId = maps:get(<<"providerId">>, ProviderDistribution),
                case maps:is_key(<<"blocks">>, ProviderDistribution) of
                    true ->
                        Acc#{ProviderId => ProviderDistribution};
                    _ ->
                        Acc#{
                            ProviderId => #{
                                <<"providerId">> => ProviderId,
                                <<"blocks">> => [[0, size(FileContent)]],
                                <<"totalBlocksSize">> => size(FileContent)
                            }
                        }
                end;
            false ->
                ProviderId = ProviderDistributionOrId,
                Acc#{
                    ProviderId => #{
                        <<"providerId">> => ProviderId,
                        <<"blocks">> => [[0, size(FileContent)]],
                        <<"totalBlocksSize">> => size(FileContent)
                    }
                }
        end
    end, #{}, ExpectedDistribution),
    lists:map(fun(ProviderId) ->
        maps:get(ProviderId, NotEmptyDistributionMap, #{
            <<"providerId">> => ProviderId,
            <<"blocks">> => [],
            <<"totalBlocksSize">> => 0
        })
    end, oct_background:get_provider_ids()).


get_guid(Path, #{files := FilesGuidsAndPaths, dirs := DirsGuidsAndPaths}) ->
    lists:foldl(fun({Guid, P}, _) when P == Path -> Guid;
                   ({_, _}, Acc) -> Acc
    end, undefined, FilesGuidsAndPaths ++ DirsGuidsAndPaths).

get_guid(Node, SessId, Path) ->
    {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:resolve_guid(Node, SessId, Path)),
    Guid.


wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntries) ->
    Nodes = oct_background:get_all_providers_nodes(),
    QosToWaitForList = lists:foldl(fun(QosName, Acc) ->
        [{QosName, Nodes} | Acc]
    end, [], maps:keys(QosNameIdMapping)),
    
    Results = lists_utils:pmap(fun({QosName, Nodes}) ->
        QosEntryId = maps:get(QosName, QosNameIdMapping),

        % try to find expected QoS entry associated with QoS name and get
        % expected value for is_possible field. If no QoS entry is found
        % assert "true" value for is_possible field.
        LookupExpectedQosEntry = lists:filter(fun(Entry) ->
            Entry#expected_qos_entry.qos_name == QosName
        end, ExpectedQosEntries),
        ExpectedFulfillmentStatus = case LookupExpectedQosEntry of
            [ExpectedQosEntry] ->
                case ExpectedQosEntry#expected_qos_entry.possibility_check of
                    {possible, _} -> ?FULFILLED_QOS_STATUS;
                    {impossible, _} -> ?IMPOSSIBLE_QOS_STATUS
                end;
            [] ->
                ?FULFILLED_QOS_STATUS
        end,

        % wait for QoS fulfillment on different nodes
        lists_utils:pmap(fun(Node) ->
            wait_for_qos_fulfilment(Node, QosEntryId, QosName, ExpectedFulfillmentStatus)
        end, Nodes)
    end, QosToWaitForList),

    ?assert(lists:all(fun(Result) -> Result =:= ok end, lists:flatten(Results))).

wait_for_qos_fulfilment(Node, QosEntryId, QosName, ExpectedFulfillmentStatus) ->
    Fun = fun() ->
        ErrMsg = case opt_qos:get_qos_entry(Node, ?ROOT_SESS_ID, QosEntryId) of
            {ok, #qos_entry{
                possibility_check = PossibilityCheck,
                traverse_reqs = TraverseReqs
            }} ->
                case ExpectedFulfillmentStatus of
                    ?FULFILLED_QOS_STATUS ->
                        str_utils:format(
                            "QoS is not fulfilled while it should be. ~n"
                            "Node: ~p ~n"
                            "QosName: ~p ~n"
                            "ProviderId: ~p ~n"
                            "TraverseReqs: ~p ~n",
                            [Node, QosName, PossibilityCheck, TraverseReqs]
                        );
                    ?IMPOSSIBLE_QOS_STATUS ->
                        str_utils:format(
                            "QoS is fulfilled while it shouldn't be. ~n"
                            "Node: ~p ~n"
                            "QosName: ~p ~n", [Node, QosName]
                        )
                end;
            {error, _} = Error ->
                str_utils:format(
                    "Error when checking QoS status. ~n"
                    "Node: ~p~n"
                    "QosName: ~p~n"
                    "Error: ~p~n", [Node, QosName, Error]
                )
        end,
        {opt_qos:check_qos_status(Node, ?ROOT_SESS_ID, QosEntryId), ErrMsg}
    end,
    assert_match_with_err_msg(Fun, {ok, ExpectedFulfillmentStatus}, ?ATTEMPTS, 1000).


map_qos_names_to_ids(QosNamesList, QosNameIdMapping) ->
    [maps:get(QosName, QosNameIdMapping) || QosName <- QosNamesList].


set_qos_parameters(Provider, StorageId, QosParameters) ->
    ok = opw_test_rpc:call(Provider, storage, set_qos_parameters,
        [StorageId, QosParameters]).


reset_qos_parameters() ->
    Providers = oct_background:get_provider_ids(),
    lists:foreach(fun(Provider) ->
        {ok, Storages} = opw_test_rpc:call(Provider, provider_logic, get_storages, []),
        lists:foreach(fun(StorageId) ->
            ok = opw_test_rpc:call(Provider, storage, set_qos_parameters, [StorageId, #{}])
        end, Storages)
    end, Providers).


%%%====================================================================
%%% Mock related functions
%%%====================================================================

mock_transfers(Nodes) ->
    test_utils:mock_new(Nodes, replica_synchronizer, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(Nodes, replica_synchronizer, synchronize,
        fun(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, CallbackModule) ->
            FileGuid = file_ctx:get_logical_guid_const(FileCtx),
            TestPid ! {qos_slave_job, self(), FileGuid},
            receive
                {completed, FileGuid} ->
                    meck:passthrough([UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, CallbackModule]),
                    {ok, FileGuid}
            end
        end).


% above mock (mock_transfers/1) required for this function to work
wait_for_file_transfer_start(FileGuid) ->
    receive {qos_slave_job, _Pid, FileGuid} = Msg ->
        self() ! Msg
    after timer:seconds(?ATTEMPTS) ->
        throw(reconciliation_transfer_not_started)
    end.


% above mock (mock_transfers/1) required for this function to work
finish_transfers(Files) ->
    finish_transfers(Files, strict).

% above mock (mock_transfers/1) required for this function to work
finish_transfers(Files, Mode) ->
    finish_transfers(Files, Mode, []).

% above mock (mock_transfers/1) required for this function to work
finish_transfers([], _Mode, IgnoredMsgs) -> 
    resend_msgs(IgnoredMsgs);
finish_transfers(Files, Mode, IgnoredMsgs) ->
    receive {qos_slave_job, Pid, FileGuid} = Msg ->
        case lists:member(FileGuid, Files) of
            true ->
                Pid ! {completed, FileGuid},
                finish_transfers(lists:delete(FileGuid, Files), Mode, IgnoredMsgs);
            false ->
                finish_transfers(Files, Mode, [Msg | IgnoredMsgs])
        end
    after timer:seconds(10) ->
        resend_msgs(IgnoredMsgs),
        case Mode of
            strict ->
                ct:print("Transfers not started: ~p", [Files]),
                {error, transfers_not_started};
            non_strict ->
                ok
        end
    end.


% above mock (mock_transfers/1) required for this function to work
finish_all_transfers() ->
    receive {qos_slave_job, Pid, FileGuid}->
        Pid ! {completed, FileGuid},
        finish_all_transfers()
    after 0 ->
        ok
    end.

resend_msgs([]) -> 
    ok;
resend_msgs([Msg | Tail]) -> 
    self() ! Msg, 
    resend_msgs(Tail).


mock_replica_synchronizer(Nodes, passthrough) ->
    ok = test_utils:mock_expect(Nodes, replica_synchronizer, synchronize,
        fun(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, CallbackModule) ->
            meck:passthrough([UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, CallbackModule])
        end);
mock_replica_synchronizer(Nodes, {throw, Error}) ->
    ok = test_utils:mock_expect(Nodes, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _, _) ->
            throw(Error)
        end);
mock_replica_synchronizer(Nodes, Expected) ->
    ok = test_utils:mock_expect(Nodes, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _, _) ->
            Expected
        end).

%%%====================================================================
%%% Assertions
%%%====================================================================

assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping) ->
    assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping, 1).

assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping, Attempts) ->
    lists:foreach(fun(#expected_qos_entry{
        providers = ProvidersOrUndef,
        qos_expression = QosExpressionRPN,
        replicas_num = ReplicasNum,
        file_key = FileKey,
        qos_name = QosName,
        possibility_check = PossibilityCheck
    }) ->
        QosEntryId = QosEntryId = maps:get(QosName, QosNameIdMapping),
        % if not specified in tests spec, check document on all nodes
        Providers = ensure_providers(ProvidersOrUndef),

        lists:foreach(fun(Provider) ->
            lists:foreach(fun(Node) ->
                FileUuid = case FileKey of
                    {path, Path} ->
                        ?GET_FILE_UUID(Node, ?SESS_ID(Provider), Path);
                    {uuid, Uuid} ->
                        Uuid
                end,
                assert_qos_entry_document(
                    Node, QosEntryId, FileUuid, QosExpressionRPN, ReplicasNum, Attempts, PossibilityCheck
                )
            end, oct_background:get_provider_nodes(Provider))
        end, Providers)
    end, ExpectedQosEntries).

assert_qos_entry_document(Node, QosEntryId, FileUuid, Expression, ReplicasNum, Attempts, PossibilityCheck) ->
    ExpectedQosEntryFirstVersion = #qos_entry{
        file_uuid = FileUuid,
        expression = Expression,
        replicas_num = ReplicasNum,
        possibility_check = PossibilityCheck
    },
    ExpectedQosEntry = upgrade_qos_entry_record(ExpectedQosEntryFirstVersion),
    GetQosEntryFun = fun() ->
        ?assertMatch({ok, _Doc}, opw_test_rpc:call(Node, qos_entry, get, [QosEntryId]), Attempts),
        {ok, #document{value = QosEntry, scope = SpaceId}} = opw_test_rpc:call(Node, qos_entry, get, [QosEntryId]),
        ?assertEqual({ReplicasNum, FileUuid}, get_qos_entry_by_rest(Node, QosEntryId, SpaceId)),
        % do not assert traverse reqs
        QosEntryWithoutTraverseReqs = QosEntry#qos_entry{traverse_reqs = #{}},
        ErrMsg = str_utils:format(
            "Node: ~p ~n"
            "Expected qos_entry: ~p ~n"
            "Got: ~p", [Node, ExpectedQosEntry, QosEntryWithoutTraverseReqs]
        ),
        {QosEntryWithoutTraverseReqs, ErrMsg}
    end,
    assert_match_with_err_msg(GetQosEntryFun, ExpectedQosEntry, Attempts, 200).


upgrade_qos_entry_record(QosEntryRecordInFirstVersion) ->
    MaxVersion = qos_entry:get_record_version(),
    lists:foldl(fun(Version, Record) ->
        {NewVersion, NewRecord} = qos_entry:upgrade_record(Version, Record),
        ?assertEqual(Version + 1, NewVersion),
        NewRecord
    end, QosEntryRecordInFirstVersion, lists:seq(1, MaxVersion - 1)).


get_qos_entry_by_rest(Node, QosEntryId, SpaceId) ->
    URL = <<"qos_requirements/", QosEntryId/binary>>,
    Headers = [rest_test_utils:user_token_header(oct_background:get_user_access_token(?USER_PLACEHOLDER))],
    case make_rest_request(Node, URL, get, Headers, #{}, SpaceId, [?SPACE_VIEW_QOS]) of
        {ok, RespBody} ->
            DecodedBody = json_utils:decode(RespBody),
            #{
                <<"fileId">> := FileObjectId,
                <<"replicasNum">> := ReplicasNum
            } = DecodedBody,
            {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),
            FileUuid = file_id:guid_to_uuid(FileGuid),
            {ReplicasNum, FileUuid};
        {error, _} = Error -> Error
    end.


assert_file_qos_documents(ExpectedFileQos, QosNameIdMapping, FilterOther) ->
    assert_file_qos_documents(ExpectedFileQos, QosNameIdMapping, FilterOther, 1).

assert_file_qos_documents(ExpectedFileQos, QosNameIdMapping, FilterOther, Attempts) ->
    lists:foreach(fun(#expected_file_qos{
        providers = ProvidersOrUndef,
        path = FilePath,
        qos_entries = ExpectedQosEntriesNames,
        assigned_entries = ExpectedAssignedEntries
    }) ->
        % if not specified in tests spec, check document on all nodes
        Providers = ensure_providers(ProvidersOrUndef),
        ExpectedQosEntriesId = map_qos_names_to_ids(ExpectedQosEntriesNames, QosNameIdMapping),
        ExpectedAssignedEntriesId = maps:map(fun(_, QosNamesList) ->
            map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedAssignedEntries),

        lists:foreach(fun(Provider) ->
            Node = oct_background:get_random_provider_node(Provider),
            FileUuid = ?GET_FILE_UUID(Node, ?SESS_ID(Provider), FilePath),
            assert_file_qos_document(
                Node, FileUuid, ExpectedQosEntriesId, ExpectedAssignedEntriesId,
                FilePath, FilterOther, Attempts
            )
        end, Providers)
    end, ExpectedFileQos).


assert_file_qos_document(
    Node, FileUuid, QosEntries, AssignedEntries, FilePath, FilterAssignedEntries, Attempts
) ->
    {ok, StorageId} = opw_test_rpc:call(Node, space_logic, get_local_supporting_storage, [oct_background:get_space_id(?SPACE)]),
    ExpectedFileQos = #file_qos{
        qos_entries = QosEntries,
        assigned_entries = case FilterAssignedEntries of
            true ->
                maps:filter(fun(Key, _Val) -> Key == StorageId end, AssignedEntries);
            false ->
                AssignedEntries
        end
    },
    ExpectedFileQosSorted = sort_file_qos(ExpectedFileQos),

    GetSortedFileQosFun = fun() ->
        {ok, #document{value = FileQos}} = ?assertMatch(
            {ok, _Doc},
            opw_test_rpc:call(Node, datastore_model, get, [file_qos:get_ctx(), FileUuid])
        ),
        FileQosSorted = sort_file_qos(FileQos),
        ErrMsg = str_utils:format(
            "Node: ~p~n"
            "File: ~p~n"
            "Sorted file_qos: ~p~n"
            "Expected file_qos: ~p~n",
            [Node, FilePath, FileQosSorted, ExpectedFileQos]
        ),
        {FileQosSorted, ErrMsg}
    end,
    assert_match_with_err_msg(GetSortedFileQosFun, ExpectedFileQosSorted, Attempts, 500).


assert_effective_qos(ExpectedEffQosEntries, QosNameIdMapping, FilterAssignedEntries) ->
    assert_effective_qos(ExpectedEffQosEntries, QosNameIdMapping, FilterAssignedEntries, 1).

assert_effective_qos(ExpectedEffQosEntries, QosNameIdMapping, FilterAssignedEntries, Attempts) ->
    lists:foreach(fun(#expected_file_qos{
        providers = ProviderOrUndef,
        path = FilePath,
        qos_entries = ExpectedQosEntriesWithNames,
        assigned_entries = ExpectedAssignedEntries
    }) ->
        % if not specified in tests spec, check document on all nodes
        Providers = ensure_providers(ProviderOrUndef),
        ExpectedQosEntriesId = qos_tests_utils:map_qos_names_to_ids(ExpectedQosEntriesWithNames, QosNameIdMapping),
        ExpectedAssignedEntriesId = maps:map(fun(_, QosNamesList) ->
            qos_tests_utils:map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedAssignedEntries),

        lists:foreach(fun(Provider) ->
            Node = oct_background:get_random_provider_node(Provider),
            SessId = ?SESS_ID(Provider),
            assert_effective_qos(
                Provider, FilePath, ExpectedQosEntriesId, ExpectedAssignedEntriesId,
                FilterAssignedEntries, Attempts
            ),

            % check that for file document has not been created
            FileUuid = ?GET_FILE_UUID(Node, SessId, FilePath),
            ?assertMatch({error, not_found}, opw_test_rpc:call(Node, datastore_model, get, [file_qos:get_ctx(), FileUuid]))

        end, Providers)
    end, ExpectedEffQosEntries).

assert_effective_qos(Provider, FilePath, QosEntries, AssignedEntries, FilterAssignedEntries, Attempts) ->
    Node = oct_background:get_random_provider_node(Provider),
    StorageId = opt_spaces:get_storage_id(Provider, oct_background:get_space_id(?SPACE)),
    ExpectedEffectiveQos = #effective_file_qos{
        qos_entries = QosEntries,
        assigned_entries = case FilterAssignedEntries of
            true -> maps:filter(fun(Key, _Val) -> Key == StorageId end, AssignedEntries);
            false -> AssignedEntries
        end
    },
    ExpectedEffectiveQosSorted = sort_effective_qos(ExpectedEffectiveQos),

    GetSortedEffectiveQos = fun() ->
        FileGuid = qos_tests_utils:get_guid(Node, ?SESS_ID(Provider), FilePath),
        {ok, EffQos} = get_effective_qos_by_lfm(Node, ?SESS_ID(Provider), FileGuid),
        EffQosSorted = sort_effective_qos(EffQos),
        ErrMsg = str_utils:format(
            "Node: ~p~n"
            "File: ~p~n"
            "Sorted effective QoS: ~p~n"
            "Expected effective QoS: ~p~n",
            [Node, FilePath, EffQosSorted, ExpectedEffectiveQos]
        ),
        {EffQosSorted, ErrMsg}
    end,
    assert_match_with_err_msg(GetSortedEffectiveQos, ExpectedEffectiveQosSorted, Attempts, 500).


get_effective_qos_by_lfm(Node, SessionId, FileGuid) ->
    {ok, {QosEntriesWithStatus, AssignedEntries}} =
        opt_qos:get_effective_file_qos(Node, SessionId, ?FILE_REF(FileGuid)),
    {ok, #effective_file_qos{
        assigned_entries = AssignedEntries,
        qos_entries = maps:keys(QosEntriesWithStatus)
    }}.


assert_distribution_in_dir_structure(undefined, _) ->
    true;

assert_distribution_in_dir_structure(#test_dir_structure{
    assertion_providers = ProvidersOrUndef,
    dir_structure = ExpectedDirStructure
}, GuidsAndPaths) ->
    % if not specified in tests spec, check document on all nodes
    Providers = ensure_providers(ProvidersOrUndef),

    assert_distribution_in_dir_structure(Providers, ExpectedDirStructure, <<"/">>, GuidsAndPaths, ?ATTEMPTS);

assert_distribution_in_dir_structure(ExpectedDirStructure, GuidsAndPaths) ->
    assert_distribution_in_dir_structure(#test_dir_structure{dir_structure = ExpectedDirStructure}, GuidsAndPaths).

assert_distribution_in_dir_structure(_Providers, _DirStructure, _Path, _GuidsAndPaths, 0) ->
    false;

assert_distribution_in_dir_structure(Providers, DirStructure, Path, GuidsAndPaths, Attempts) ->
    PrintError = Attempts == 1,
    case assert_file_distribution(Providers, DirStructure, Path, PrintError, GuidsAndPaths) of
        true ->
            true;
        false ->
            timer:sleep(timer:seconds(1)),
            assert_distribution_in_dir_structure(Providers, DirStructure, Path, GuidsAndPaths, Attempts - 1)
    end.

assert_file_distribution(Providers, {DirName, DirContent}, Path, PrintError, GuidsAndPaths) ->
    lists:foldl(fun(Child, Matched) ->
        DirPath = filename:join(Path, DirName),
        case assert_file_distribution(Providers, Child, DirPath, PrintError, GuidsAndPaths) of
            true ->
                Matched;
            false ->
                false
        end
    end, true, DirContent);

assert_file_distribution(Providers, {FileName, FileContent, ExpectedFileDistribution}, 
    Path, PrintError, GuidsAndPaths
) ->
    assert_file_distribution(Providers, {FileName, FileContent, ExpectedFileDistribution, reg_file}, 
        Path, PrintError, GuidsAndPaths);
assert_file_distribution(Providers, {FileName, FileContent, ExpectedFileDistribution, _},
    Path, PrintError, GuidsAndPaths
) ->
    FilePath = filename:join(Path, FileName),
    FileGuid = get_guid(FilePath, GuidsAndPaths),

    lists:foldl(fun(Provider, Res) ->
        SessId = ?SESS_ID(Provider),
        ExpectedDistributionSorted = lists:sort(
            fill_in_expected_distribution(ExpectedFileDistribution, FileContent)
        ),
        Node = oct_background:get_random_provider_node(Provider),

        FileDistributionSorted = case opt_file_metadata:get_distribution_deprecated(Node, SessId, ?FILE_REF(FileGuid)) of
            {ok, FileLocations} ->
                lists:sort(FileLocations);
            Error ->
                Error
        end,

        case {FileDistributionSorted == ExpectedDistributionSorted, PrintError} of
            {false, false} ->
                false;
            {false, true} ->
                ct:pal(
                    "Wrong file distribution for ~p on node ~p. ~n"
                    "Expected: ~p~n"
                    "Got: ~p~n",
                    [FilePath, Node, ExpectedDistributionSorted, FileDistributionSorted]),
                    false;
            {true, _} ->
                Res
        end
    end, true, Providers).


gather_not_matching_statuses_on_all_nodes(Guids, QosList, ExpectedStatus) ->
    ExpectedStatusMatcher = case ExpectedStatus of
        {error, _} -> ExpectedStatus;
        _ -> {ok, ExpectedStatus}
    end, 
    lists:flatmap(fun(Node) ->
        lists:filtermap(fun(Guid) ->
            case opt_qos:check_qos_status(Node, ?ROOT_SESS_ID, QosList, ?FILE_REF(Guid)) of
                ExpectedStatusMatcher -> false;
                NotExpectedStatus -> {true, {Node, Guid, NotExpectedStatus}}
            end
        end, Guids)
    end, oct_background:get_all_providers_nodes()).

%%%====================================================================
%%% Internal functions
%%%====================================================================

%% @private
sort_file_qos(FileQos) ->
    FileQos#file_qos{
        qos_entries = lists:sort(FileQos#file_qos.qos_entries),
        assigned_entries = maps:map(fun(_, QosEntriesForStorage) ->
            lists:sort(QosEntriesForStorage)
        end, FileQos#file_qos.assigned_entries)
    }.

%% @private
sort_effective_qos(EffectiveQos) ->
    EffectiveQos#effective_file_qos{
        qos_entries = lists:sort(EffectiveQos#effective_file_qos.qos_entries),
        assigned_entries = maps:map(fun(_, QosEntriesForStorage) ->
            lists:sort(QosEntriesForStorage)
        end, EffectiveQos#effective_file_qos.assigned_entries)
    }.


%% @private
ensure_providers(undefined) ->
    oct_background:get_provider_ids();
ensure_providers(Providers) ->
    Providers.


%% @private
ensure_provider(undefined) ->
    hd(ensure_providers(undefined));
ensure_provider(Provider) ->
    Provider.


%% @private
assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected, Attempts, _Sleep) when Attempts < 1 ->
    {ActualVal, ErrMsg} = GetActualValAndErrMsgFun(),
    try
        ?assertMatch(Expected, ActualVal),
        ok
    catch
        error:{assertMatch_failed, _} = Error ->
            ct:pal(ErrMsg),
            error(Error)
    end;

assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected , Attempts, Sleep) ->
    {ActualVal, _ErrMsg} = GetActualValAndErrMsgFun(),
    case ActualVal of
        Expected -> ok;
        _ ->
            timer:sleep(Sleep),
            assert_match_with_err_msg(GetActualValAndErrMsgFun, Expected, Attempts - 1, Sleep)
    end.


%% @private
make_rest_request(Node, URL, Method, Headers, ReqBody, SpaceId, RequiredPrivs) ->
    ErrorForbidden = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),
    AllSpacePrivs = privileges:space_privileges(),
    EncodedReqBody = json_utils:encode(ReqBody),
    UserId = oct_background:get_user_id(?USER_PLACEHOLDER),
    UserPrivileges = opt_spaces:get_privileges(Node, SpaceId, UserId),
    try
        ozt_spaces:set_privileges(SpaceId, ?USER_PLACEHOLDER, AllSpacePrivs -- RequiredPrivs),
        {ok, Code, _, Resp} = rest_test_utils:request(Node, URL, Method, Headers, EncodedReqBody),
        ?assertMatch(ErrorForbidden, {Code, json_utils:decode(Resp)}),

        ozt_spaces:set_privileges(SpaceId, ?USER_PLACEHOLDER, AllSpacePrivs),
        case rest_test_utils:request(Node, URL, Method, Headers, EncodedReqBody) of
            {ok, 200, _, RespBody} ->
                {ok, RespBody};
            {ok, 201, _, RespBody} ->
                {ok, RespBody};
            {ok, Code1, _, RespBody} ->
                {error, {Code1, RespBody}}
        end
    after
        ozt_spaces:set_privileges(SpaceId, ?USER_PLACEHOLDER, UserPrivileges)
    end.

%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Skeleton of tests of db_sync changes requesting.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_changes_requesting_test_base).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/dbsync/dbsync.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([generic_test_skeleton/2]).
-export([handle_each_correlation_in_out_stream_separately/1, add_delay_to_in_stream_handler/1,
    use_single_doc_broadcast_batch/1, use_default_broadcast_batch_size/1]).
-export([create_tester_session/1, get_tester_session/0]).
-export([create_dirs_batch/4, verify_dirs_batch/2, verify_sequences_correlation/2]).

% Record sent by mocks to gather information about synchronization progress
-record(synchronization_log, {
    operation,
    provider_id,
    documents
}).

% Type of test_action (different request can be created depending on action type)
-type action_type() ::
    request_range |                               % Requesting changes using range [Since, Until]
    request_using_map |                           % Requesting changes starting from map generated at the begging
                                                  % of the test (before test data creation)
    estart_using_seq_returned_from_prev_request | % Requesting changes using range [Since, Until] where Since
                                                  % is Until from previous request
    restart_using_map_returned_from_prev_request. % Requesting changes using map returned by from previous request
% Type describing custom parameter for test action (it depends on action type)
-type action_param() :: datastore_doc:seq() | dbsync_processed_seqs_history:encoded_correlations() | oneprovider:id().

% Record describing action to be executed during the test
-record(test_action, {
    worker_handling_request :: node(),
    action_type :: action_type(),
    action_param :: action_param(),
    % parameters to be included in changes request message
    include_mutators :: dbsync_worker:mutators_to_include(),
    until :: datastore_doc:seq(),
    provider_id :: oneprovider:id()
}).

% Requesting changes using range [Since, Until]
-define(RANGE_REQUEST(Since, Until, ProviderId, WorkerHandlingRequest), #test_action{
    action_type = request_range,
    action_param = Since,
    include_mutators = all_providers,
    until = Until,
    provider_id = ProviderId,
    worker_handling_request = WorkerHandlingRequest
}).

% Requesting changes starting from map generated at the begging of the test (before test data creation)
-define(REQUEST_USING_MAP(EncodedInitSequencesMap, Until, ProviderId, WorkerHandlingRequest), #test_action{
    action_type = request_using_map,
    action_param = EncodedInitSequencesMap,
    include_mutators = all_providers,
    until = Until,
    provider_id = ProviderId,
    worker_handling_request = WorkerHandlingRequest
}).

% Requesting changes using range [Since, Until] where Since is Until from prev request to PrevProvider
-define(RESTART_FROM_PREV_REQUEST_USING_SEQ(PrevProvider, Until, ProviderId, WorkerHandlingRequest), #test_action{
    action_type = restart_using_seq_returned_from_prev_request,
    action_param = PrevProvider,
    include_mutators = all_providers,
    until = Until,
    provider_id = ProviderId,
    worker_handling_request = WorkerHandlingRequest
}).

% Requesting changes using map returned by from prev request to PrevProvider
-define(RESTART_FROM_PREV_REQUEST_MAP_REQUEST(PrevProvider, Until, ProviderId, WorkerHandlingRequest), #test_action{
    action_type = restart_using_map_returned_from_prev_request,
    action_param = PrevProvider,
    include_mutators = all_providers,
    until = Until,
    provider_id = ProviderId,
    worker_handling_request = WorkerHandlingRequest
}).

% Requesting changes of single provider using range [Since, Until]
-define(SINGLE_PROVIDER_CHANGES_REQUEST(Since, Until, ProviderId, WorkerHandlingRequest), #test_action{
    action_type = request_range,
    action_param = Since,
    include_mutators = single_provider,
    until = Until,
    provider_id = ProviderId,
    worker_handling_request = WorkerHandlingRequest
}).

-define(SPACE_ID, <<"space1_id">>).
-define(SPACE_NAME, <<"space1">>).

%%%===================================================================
%%% Test skeleton
%%%===================================================================

generic_test_skeleton(Config0, TestSize) ->
    % Prepare variables used during test
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {6, 0, 0, 1}, 180),
    SpaceName = ?SPACE_NAME,
    SpaceId = ?SPACE_ID,
    [Worker1, Worker2, Worker3, Worker4, Worker5, Worker6] = Workers = ?config(op_worker_nodes, Config),
    WorkerToProviderId = maps:from_list(lists:map(fun(Worker) ->
        {Worker, rpc:call(Worker, oneprovider, get_id_or_undefined, [])}
    end, Workers)),

    %%%===================================================================

    % Prepare test environment
    prepare_mocks_gathering_synchronization_logs(Workers, WorkerToProviderId),

    %%%===================================================================

    % Get initial sequences before test data creation
    W1Id = maps:get(Worker1, WorkerToProviderId),
    P1InitSeq = rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, W1Id]),
    W2Id = maps:get(Worker2, WorkerToProviderId),
    P2InitSeq = rpc:call(Worker2, dbsync_state, get_seq, [SpaceId, W2Id]),
    W3Id = maps:get(Worker3, WorkerToProviderId),
    P3InitSeq = rpc:call(Worker3, dbsync_state, get_seq, [SpaceId, W3Id]),
    W4Id = maps:get(Worker4, WorkerToProviderId),
    P4InitSeq = rpc:call(Worker4, dbsync_state, get_seq, [SpaceId, W4Id]),
    W5Id = maps:get(Worker5, WorkerToProviderId),
    P5InitSeq = rpc:call(Worker5, dbsync_state, get_seq, [SpaceId, W5Id]),
    W6Id = maps:get(Worker6, WorkerToProviderId),
    P6InitSeq = rpc:call(Worker6, dbsync_state, get_seq, [SpaceId, W6Id]),

    % Prepare sequences map used during changes request
    InitSequencesMap = #{
        W1Id => P1InitSeq,
        W2Id => P2InitSeq,
        W3Id => P3InitSeq,
        W4Id => P4InitSeq,
        W5Id => P5InitSeq,
        W6Id => P6InitSeq
    },
    EncodedInitSequencesMap = maps:fold(fun
        (ProviderId, Seq, Acc) ->
            <<Acc/binary, (dbsync_processed_seqs_history:provider_seq_to_binary(ProviderId, Seq))/binary>>
    end, <<>>, InitSequencesMap),

    %%%===================================================================

    % Prepare test data and verify sequences correlation
    % build after synchronization of test data
    create_and_verify_dirs(Config, SpaceName, SpaceId, TestSize),
    verify_sequences_correlation(WorkerToProviderId, SpaceId),

    %%%===================================================================

    % Gather synchronization logs connected with test data creation
    InitialSynchronizationLogs = gather_synchronization_logs(),

    % Calculate number of sequences produced by providers
    P1SeqCount = rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, W1Id]) - P1InitSeq,
    P2SeqCount = rpc:call(Worker2, dbsync_state, get_seq, [SpaceId, W2Id]) - P2InitSeq,
    P3SeqCount = rpc:call(Worker3, dbsync_state, get_seq, [SpaceId, W3Id]) - P3InitSeq,
    P4SeqCount = rpc:call(Worker4, dbsync_state, get_seq, [SpaceId, W4Id]) - P4InitSeq,
    P5SeqCount = rpc:call(Worker5, dbsync_state, get_seq, [SpaceId, W5Id]) - P5InitSeq,
    P6SeqCount = rpc:call(Worker6, dbsync_state, get_seq, [SpaceId, W6Id]) - P6InitSeq,

    %%%===================================================================

    % Prepare and execute test cases

    Test1 = [
        ?RANGE_REQUEST(P2InitSeq, P2InitSeq + P2SeqCount, W2Id, Worker2)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test1),

    Test2 = [
        ?RANGE_REQUEST(P2InitSeq, P2InitSeq + P2SeqCount div 5, W2Id, Worker2),
        ?RANGE_REQUEST(P2InitSeq + P2SeqCount div 5, P2InitSeq + 2 * P2SeqCount div 5, W2Id, Worker2),
        ?RANGE_REQUEST(P2InitSeq + 2 * P2SeqCount div 5, P2InitSeq + 3 * P2SeqCount div 5, W2Id, Worker2),
        ?RANGE_REQUEST(P2InitSeq + 3 * P2SeqCount div 5, P2InitSeq + 4 * P2SeqCount div 5, W2Id, Worker2),
        ?RANGE_REQUEST(P2InitSeq + 4 * P2SeqCount div 5, P2InitSeq + P2SeqCount, W2Id, Worker2)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test2),

    Test3 = [
        ?REQUEST_USING_MAP(EncodedInitSequencesMap, P3InitSeq + P3SeqCount, W3Id, Worker2),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W2Id, P2InitSeq + P2SeqCount, W2Id, Worker2)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test3),

    Test4 = [
        ?REQUEST_USING_MAP(EncodedInitSequencesMap, P3InitSeq + P3SeqCount div 2, W3Id, Worker2),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W2Id, P2InitSeq + P2SeqCount, W2Id, Worker2)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test4),

    Test5 = [
        ?REQUEST_USING_MAP(EncodedInitSequencesMap, P3InitSeq + P3SeqCount div 5, W3Id, Worker2),
        ?RANGE_REQUEST(P3InitSeq + P3SeqCount div 5, P3InitSeq + 2 * P3SeqCount div 5, W3Id, Worker2),
        ?RANGE_REQUEST(P3InitSeq + 2 * P3SeqCount div 5, P3InitSeq + 3 * P3SeqCount div 5, W3Id, Worker2),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W2Id, P2InitSeq + P2SeqCount, W2Id, Worker2)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test5),

    Test6 = [
        ?REQUEST_USING_MAP(EncodedInitSequencesMap, P1InitSeq + P1SeqCount div 6, W1Id, Worker6),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W6Id, P2InitSeq + 2 * P2SeqCount div 6, W2Id, Worker6),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W6Id, P3InitSeq + 3 * P3SeqCount div 6, W3Id, Worker6),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W6Id, P4InitSeq + 4 * P4SeqCount div 6, W4Id, Worker6),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W6Id, P5InitSeq + 5 * P5SeqCount div 6, W5Id, Worker6),
        ?RESTART_FROM_PREV_REQUEST_USING_SEQ(W6Id, P6InitSeq + P6SeqCount, W6Id, Worker6)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test6),

    Test7 = [
        ?REQUEST_USING_MAP(EncodedInitSequencesMap, P1InitSeq + P1SeqCount div 6, W1Id, Worker1),
        ?RESTART_FROM_PREV_REQUEST_MAP_REQUEST(W1Id, P2InitSeq + 2 * P2SeqCount div 6, W2Id, Worker2),
        ?RESTART_FROM_PREV_REQUEST_MAP_REQUEST(W2Id, P3InitSeq + 3 * P3SeqCount div 6, W3Id, Worker3),
        ?RESTART_FROM_PREV_REQUEST_MAP_REQUEST(W3Id, P4InitSeq + 4 * P4SeqCount div 6, W4Id, Worker4),
        ?RESTART_FROM_PREV_REQUEST_MAP_REQUEST(W4Id, P5InitSeq + 5 * P5SeqCount div 6, W5Id, Worker5),
        ?RESTART_FROM_PREV_REQUEST_MAP_REQUEST(W5Id, P6InitSeq + P6SeqCount, W6Id, Worker6)
    ],
    perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, Test7),

    SingleProviderTest1 = ?SINGLE_PROVIDER_CHANGES_REQUEST(P2InitSeq + P2SeqCount div 5,
        P2InitSeq + 2 * P2SeqCount div 5, W2Id, Worker2),
    perform_single_provider_changes_request_verification(SpaceId, InitialSynchronizationLogs, SingleProviderTest1),

    SingleProviderTest2 = ?SINGLE_PROVIDER_CHANGES_REQUEST(P3InitSeq + P3SeqCount div 5,
        2 * P3InitSeq + P3SeqCount div 5, W3Id, Worker2),
    perform_single_provider_changes_request_verification(SpaceId, InitialSynchronizationLogs, SingleProviderTest2),

    ok.

%%%===================================================================
%%% Test cases execution and helper functions
%%%===================================================================

perform_changes_requests_verification(SpaceId, InitialSynchronizationLogs, ActionsSequence) ->
    ct:pal("Starting verification of actions' sequence ~p", [ActionsSequence]),

    SynchronizationLogsGeneratedByRequests = lists:foldl(fun(Action, Acc) ->
        ct:pal("Starting verification of action ~p", [Action]),
        {#test_action{
            until = Until,
            provider_id = ProviderId,
            worker_handling_request = WorkerHandlingRequest
        }, Since} = process_and_send_changes_request(SpaceId, Action),
        SynchronizationLogsGeneratedByRequest = gather_synchronization_logs(Acc),
        verify_synchronization_logs(SynchronizationLogsGeneratedByRequest, InitialSynchronizationLogs,
            WorkerHandlingRequest, ProviderId, Since, Until),
        SynchronizationLogsGeneratedByRequest
    end, #{}, ActionsSequence),

    % Get synchronization logs generated handling test requests
    RequestGeneratedChanges = get_synchronization_logs(requested_changes, all_changes, SynchronizationLogsGeneratedByRequests),

    % Get initial logs to compare with logs generated handling test request
    InitialBroadcastChanges = get_synchronization_logs(broadcast, all_changes, InitialSynchronizationLogs),
    InitialIgnoredLogs = get_synchronization_logs(ignored, all_changes, InitialSynchronizationLogs),

    % Filter ignored logs as they cannot be produced generated handling test request
    FilteredInitialBroadcastChanges = generate_changes_list_diff(InitialBroadcastChanges, InitialIgnoredLogs),

    % Get and check difference between initial broadcast logs and logs generated handling test request
    ?assertEqual([], generate_changes_list_diff(FilteredInitialBroadcastChanges, RequestGeneratedChanges)).

perform_single_provider_changes_request_verification(SpaceId, InitialSynchronizationLogs, Action) ->
    ct:pal("Starting verification of single provider's request ~p", [Action]),

    {#test_action{
        until = Until,
        provider_id = ProviderId,
        worker_handling_request = WorkerHandlingRequest
    }, Since} = process_and_send_changes_request(SpaceId, Action),
    SynchronizationLogsGeneratedByRequest = gather_synchronization_logs(#{}),
    verify_synchronization_logs(SynchronizationLogsGeneratedByRequest, InitialSynchronizationLogs,
        WorkerHandlingRequest, ProviderId, Since, Until).

process_and_send_changes_request(SpaceId, #test_action{
    action_type = restart_using_map_returned_from_prev_request,
    action_param = PrevProvider
} = Action) ->
    RequestedSequencesCorrelation = get_requested_sequences_correlation(),
    ProviderSequencesCorrelation = maps:get(PrevProvider, RequestedSequencesCorrelation, <<>>),
    process_and_send_changes_request(SpaceId, Action#test_action{
        action_type = request_using_map,
        action_param = ProviderSequencesCorrelation
    });
process_and_send_changes_request(SpaceId, #test_action{
    action_type = restart_using_seq_returned_from_prev_request,
    action_param = PrevProvider,
    provider_id = ProviderId
} = Action) ->
    RequestedSequencesCorrelation = get_requested_sequences_correlation(),
    ProviderSequencesCorrelation = maps:get(PrevProvider, RequestedSequencesCorrelation, <<>>),
    ProviderSequencesCorrelationMap = dbsync_processed_seqs_history:decode(ProviderSequencesCorrelation),
    Since = maps:get(ProviderId, ProviderSequencesCorrelationMap, 1),
    process_and_send_changes_request(SpaceId, Action#test_action{
        action_type = request_range,
        action_param = Since
    });
process_and_send_changes_request(SpaceId, #test_action{
    action_type = ActionType,
    action_param = SinceOrSequenceMap,
    include_mutators = IncludeMutators,
    until = Until,
    provider_id = ProviderId,
    worker_handling_request = WorkerHandlingRequest
} = Action) ->
    SessionId = dbsync_changes_requesting_test_base:get_tester_session(),
    ?assertMatch(ok, rpc:call(WorkerHandlingRequest, worker_proxy, call, [
        dbsync_worker, {dbsync_message, SessionId, #custom_changes_request{
            space_id = SpaceId,
            since = SinceOrSequenceMap,
            until = Until,
            mutator_id = ProviderId,
            include_mutators = IncludeMutators
        }}
    ])),
    
    case ActionType of
        request_range ->
            {Action, SinceOrSequenceMap};
        request_using_map ->
            DecodedSequencesCorrelationMap = dbsync_processed_seqs_history:decode(SinceOrSequenceMap),
            Since = maps:get(ProviderId, DecodedSequencesCorrelationMap, 1),
            {Action, Since}
    end.

verify_synchronization_logs(SynchronizationLogsGeneratedByRequest, InitialSynchronizationLogs,
    WorkerHandlingRequest, ProviderIdToCheckChanges, Since, Until) ->
    % Get synchronization logs generated handling test request
    RequestGeneratedChanges = get_synchronization_logs(requested_changes, new_changes, SynchronizationLogsGeneratedByRequest),

    % Get initial logs to compare with logs generated handling test request
    InitialBroadcastChanges = get_synchronization_logs(broadcast, ProviderIdToCheckChanges, InitialSynchronizationLogs),
    ProviderHandlingRequest = rpc:call(WorkerHandlingRequest, oneprovider, get_id_or_undefined, []),
    InitialIgnoredLogs = get_synchronization_logs(ignored, ProviderHandlingRequest, InitialSynchronizationLogs),

    % Filter initial broadcast logs to request range
    InitialBroadcastChangesInRange = filer_changes_range(InitialBroadcastChanges, Since, Until),
    % Filter ignored logs as they cannot be produced handling test request
    FilteredInitialBroadcastChangesInRange = generate_changes_list_diff(InitialBroadcastChangesInRange, InitialIgnoredLogs),

    % Get difference between initial broadcast logs and logs generated handling test request
    ChangesDiff = generate_changes_list_diff(FilteredInitialBroadcastChangesInRange, RequestGeneratedChanges),
    % Some changes could be overridden so they should be filtered
    FilteredChangesDiff = generate_changes_list_diff(ChangesDiff, InitialBroadcastChanges, overridden),

    % TODO VFS-7035 - check batch ranges (until of one batche should be <= than since of next batch)

    % TODO VFS-7035 - investigate if we need check remote_applied log
%%    % There should be no difference
%%    ?assertEqual([], FilteredChangesDiff).
    InitialRemoteAppliedLogs = get_synchronization_logs(remote_applied, ProviderHandlingRequest, InitialSynchronizationLogs),
    case FilteredChangesDiff of
        [] -> ok;
        _ -> ct:pal("Changes diff before remote_applied filtering: ~p", [FilteredChangesDiff])
    end,

    ?assertEqual([], generate_changes_list_diff(FilteredChangesDiff, InitialRemoteAppliedLogs, overridden)).

%%%===================================================================
%%% Preparing test data and verifying initial state of the environment
%%%===================================================================

create_and_verify_dirs(Config, SpaceName, SpaceId, TestSize) ->
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    Timestamp0 = rpc:call(Worker1, global_clock, timestamp_seconds, []),

    DirsCount = case TestSize of
        small -> [10, 5, 1, 20, 2, 1];
        medium -> [20, 10, 1, 40, 5, 2];
        large -> [100, 50, 5, 200, 20, 10]
    end,
    
    % Create dirs with sleeps to better mix outgoing changes from different providers
    create_and_verify_dirs_batch(Config, SpaceName, Workers, DirsCount),
    timer:sleep(10000),
    create_and_verify_dirs_batch(Config, SpaceName, lists:reverse(Workers), DirsCount),
    timer:sleep(5000),
    create_and_verify_dirs_batch(Config, SpaceName, Workers, DirsCount),

    % TODO VFS-7205 This check did not work as module was not loaded - fix it
%%    ?assertEqual(true, catch dbsync_test_utils:are_all_seqs_and_timestamps_equal(Workers, SpaceId, Timestamp0), 60).
    ok.

create_and_verify_dirs_batch(Config, SpaceName, Workers, DirSizes) ->
    DirsList = create_dirs_batch(Config, SpaceName, Workers, DirSizes),
    verify_dirs_batch(Config, DirsList).

create_dirs_batch(Config, SpaceName, Workers, DirSizes) ->
    SessId = ?config(session, Config),
    
    DirsList = lists:map(fun(DirsNum) ->
        MainDir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
        Dirs = lists:map(fun(_) ->
            <<MainDir/binary, "/", (generator:gen_name())/binary>>
        end, lists:seq(1, DirsNum)),
        {MainDir, Dirs}
    end, DirSizes),

    lists:foreach(fun({Worker, {MainDir, Dirs}}) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId(Worker), MainDir, 8#755)),
        lists:foreach(fun(D) ->
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId(Worker), D, 8#755))
        end, Dirs)
    end, lists:zip(Workers, DirsList)),
    ct:pal("Dirs created"),

    DirsList.

verify_dirs_batch(Config, DirsList) ->
    lists:foreach(fun({MainDir, Dirs}) ->
        multi_provider_file_ops_test_base:verify_stats(Config, MainDir, true),
        lists:foreach(fun(D) ->
            multi_provider_file_ops_test_base:verify_stats(Config, D, true)
        end, Dirs)
    end, DirsList),
    ct:pal("Dirs verified").

verify_sequences_correlation(WorkerToProviderId, SpaceId) ->
    lists:foreach(fun({Worker, ProviderId}) ->
        AnalyseDbsyncState = fun() ->
            Seqs = rpc:call(Worker, dbsync_state, get_sync_progress, [SpaceId]),
            Correlation = rpc:call(Worker, dbsync_state, get_seqs_correlations, [SpaceId]),
            ProviderIds = lists:sort(maps:keys(Seqs)),

            % Check if all providers can be found in sequences correlation map
            CorrelationMapSizeCheck = case lists:sort([ProviderId | maps:keys(Correlation)]) =:= ProviderIds of
                true -> ok;
                _ -> {missing_providers_in_correlation_map, ProviderIds, Correlation}
            end,

            % Check if there are no pending sequences left
            PendingSequences = lists:filtermap(fun(RemoteProvider) ->
                case rpc:call(Worker, dbsync_pending_seqs, get_first_pending_seq, [SpaceId, RemoteProvider]) of
                    undefined -> false;
                    FirstSeq -> {true, {RemoteProvider, FirstSeq}}
                end
            end, ProviderIds),

            % Check if all sequences has been processed
            NotProcessedSequences = lists:filtermap(fun({RemoteProvider,
                #sequences_correlation{remote_consecutively_processed_max = RemoteConsecutivelyProcessedMax} = Correlation}) ->
                {ProviderSeqNumber, _} = maps:get(RemoteProvider, Seqs),
                case ProviderSeqNumber =:= RemoteConsecutivelyProcessedMax of
                    true -> false;
                    false -> {true, {RemoteProvider, ProviderSeqNumber, Correlation}}
                end
            end, maps:to_list(Correlation)),

            {CorrelationMapSizeCheck, PendingSequences, NotProcessedSequences}
        end,

        ct:pal("Verify sequences on worker ~p", [Worker]),
        ?assertEqual({ok, [], []}, AnalyseDbsyncState(), 30),
        ct:pal("Sequences verified on worker ~p", [Worker])
    end, maps:to_list(WorkerToProviderId)).

%%%===================================================================
%%% Gathering/getting logs from synchronization
%%%===================================================================

prepare_mocks_gathering_synchronization_logs(Workers, WorkerToProviderId) ->
    Master = self(),

    % Gather information about broadcast changes
    test_utils:mock_expect(Workers, dbsync_communicator, broadcast_changes,
        fun(SpaceId, Since, Until, Timestamp, Docs) ->
            Master ! #synchronization_log{
                operation = broadcast,
                provider_id = maps:get(node(), WorkerToProviderId),
                documents = Docs
            },
            meck:passthrough([SpaceId, Since, Until, Timestamp, Docs])
        end),

    % Gather information about changes synchronized on demand
    test_utils:mock_expect(Workers, dbsync_communicator, send_changes_and_correlations,
        fun(_CallerProviderId, _MutatorId, _SpaceId, _Since, _Until, _Timestamp, Docs, EncodedCorrelations) ->
            ProviderId = maps:get(node(), WorkerToProviderId),
            Master ! #synchronization_log{
                operation = requested_changes,
                provider_id = ProviderId,
                documents = Docs
            },
            Master ! {requested_sequences_correlation, ProviderId, EncodedCorrelations},
            ok
        end),

    % Gather information about ignored changes
    test_utils:mock_expect(Workers, dbsync_changes, apply,
        fun(Doc, ProviderId) ->
            Ans = meck:passthrough([Doc, ProviderId]),
            case Ans of
                {ok, undefined} ->
                    Master ! #synchronization_log{
                        operation = ignored,
                        provider_id = maps:get(node(), WorkerToProviderId),
                        documents = [Doc]
                    };
                _ ->
                    ok
            end,
            Ans
        end),

    % TODO VFS-7035 - investigate if we need check remote_applied log
    % Add mock management to init/end_per testcase if the answer is `yes`
    test_utils:mock_expect(Workers, dbsync_events, change_replicated,
        fun(SpaceId, DocToHandle) ->
            ProviderId = maps:get(node(), WorkerToProviderId),
            case DocToHandle of
                #document{mutators = [Mutator | _]} when Mutator =/= ProviderId ->
                    % will be ignored in out stream so we need to gather them for further analysis
                    Master ! #synchronization_log{
                        operation = remote_applied,
                        provider_id = ProviderId,
                        documents = [DocToHandle]
                    };
                _ ->
                    ok
            end,
            meck:passthrough([SpaceId, DocToHandle])
        end).

gather_synchronization_logs() ->
    gather_synchronization_logs(#{}).

gather_synchronization_logs(Acc) ->
    gather_synchronization_logs_loop(Acc#{new_changes => []}).

gather_synchronization_logs_loop(Acc) ->
    receive
        #synchronization_log{operation = ChangesType, provider_id = ProviderId, documents = Docs} when Docs =/= [] ->
            gather_synchronization_logs_loop(Acc#{
                {ChangesType, ProviderId} => maps:get({ChangesType, ProviderId}, Acc, []) ++ Docs,
                {ChangesType, all_changes} => maps:get({ChangesType, all_changes}, Acc, []) ++ Docs,
                {ChangesType, new_changes} => maps:get({ChangesType, new_changes}, Acc, []) ++ Docs
            })
    after
        10000 -> Acc
    end.

get_synchronization_logs(Type, ProviderIdOrScope, LogsMap) ->
    maps:get({Type, ProviderIdOrScope}, LogsMap, []).

get_requested_sequences_correlation() ->
    get_requested_sequences_correlation(#{}).

get_requested_sequences_correlation(Acc) ->
    receive
        {requested_sequences_correlation, ProviderId, OtherProvidersSeqs} ->
            get_requested_sequences_correlation(Acc#{ProviderId => OtherProvidersSeqs})
    after
        0 -> Acc
    end.

%%%===================================================================
%%% Comparing and filtering synchronization logs
%%%===================================================================

filer_changes_range(ChangesList, MinSeq, MaxSeq) ->
    lists:filter(fun(#document{seq = Seq}) -> Seq >= MinSeq andalso Seq =< MaxSeq end, ChangesList).

generate_changes_list_diff(ChangesList1, ChangesList2) ->
    generate_changes_list_diff(ChangesList1, ChangesList2, existing_or_overriden).

generate_changes_list_diff(ChangesList1, ChangesList2, FilterType) ->
    % Create map with max generation and sequence number for each unique id
    % (single document can appear multiple times in changes stream)
    List2GenerationsAndSequencesMap = lists:foldl(fun(Doc, Acc) ->
        UniqueId = get_doc_unique_id(Doc),
        {Generation, Seq} = maps:get(UniqueId, Acc, {0, 0}),
        Acc#{UniqueId => {max(Generation, get_doc_generation(Doc)), max(Seq, get_doc_seq(Doc))}}
    end, #{}, ChangesList2),

    % Get difference between changes list comparing generations and sequences numbers receive instead of
    % whole documents (some documents can be found multiple times so only newest generations are compared)
    lists:foldl(fun(Doc, Acc) ->
        UniqueId = get_doc_unique_id(Doc),
        Generation = get_doc_generation(Doc),
        Seq = get_doc_seq(Doc),
        case {maps:get(UniqueId, List2GenerationsAndSequencesMap, undefined), FilterType} of
            {undefined, _} ->
                % Document cannot be found in ChangesList2
                [Doc | Acc];
            {{GenerationInMap, _}, _} when GenerationInMap < Generation ->
                % Document in ChangesList1 is newer than document from ChangesList2
                [Doc | Acc];
            {{GenerationInMap, SeqInMap}, overridden} when
                % TODO VFS-7035 - investigate if we `overridden` filter
                GenerationInMap =:= Generation andalso SeqInMap =< Seq ->
                % Documents in both lists have the same generation but document from ChangesList has
                % newer sequence as a result of conflicts resolution mechanism
                [Doc | Acc];
            _ ->
                % Document in ChangesList1 is older than document from ChangesList2 - filter it
                Acc
        end
    end, [], ChangesList1).

get_doc_unique_id(#document{key = Key, value = V}) ->
    Model = element(1, V),
    {Key, Model}.

get_doc_generation(#document{revs = [Rev | _]}) ->
    {Generation, _} = datastore_rev:parse(Rev),
    Generation.

get_doc_seq(#document{seq = Seq}) ->
    Seq.

%%%===================================================================
%%% Functions used to init and get tester session
%%%===================================================================

-define(TESTER_ID, <<"TesterProviderId">>).

create_tester_session(Config) ->
    Workers =  ?config(op_worker_nodes, Config),

    Doc = #document{
        key = ?TESTER_ID,
        value = #session{
            type = provider_incoming,
            status = initializing,
            identity = ?SUB(?ONEPROVIDER, ?TESTER_ID)
        }
    },

    lists:foreach(fun(Worker) ->
        {ok, _} = rpc:call(Worker, session, save, [Doc])
    end, Workers).

get_tester_session() ->
    ?TESTER_ID.

%%%===================================================================
%%% Functions changing environment with mocks and env variables
%%% to simulate different problems and different working conditions during generic test
%%%===================================================================

handle_each_correlation_in_out_stream_separately(Config) ->
    Workers =  ?config(op_worker_nodes, Config),

    % NOTE: use erlang:apply with meck_util:original_name functions instead of meck:passthrough because
    % meck crashes if one mocked function called by other mocked function uses passthrough (meck bug)
    test_utils:mock_expect(Workers, dbsync_out_stream, handle_cast, fun
        ({change, {ok, #document{}}} = Request, State) ->
            {noreply, State2} = dbsync_out_stream:handle_info(handle_changes_batch, State),
            erlang:apply(meck_util:original_name(dbsync_out_stream), handle_cast, [Request, State2]);
        ({change, {ok, Docs}}, State)  when is_list(Docs) ->
            OriginalModName = meck_util:original_name(dbsync_out_stream),
            lists:foldl(fun(#document{} = Doc, {noreply, TmpState}) ->
                {noreply, TmpState2} = dbsync_out_stream:handle_info(handle_changes_batch, TmpState),
                erlang:apply(OriginalModName, handle_cast, [{change, {ok, Doc}}, TmpState2])
            end, {noreply, State}, Docs);
        (Request, State) ->
            meck:passthrough([Request, State])
    end).

add_delay_to_in_stream_handler(Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, dbsync_in_stream_worker, handle_info, fun(Info, State) ->
        timer:sleep(timer:seconds(10)),
        meck:passthrough([Info, State])
    end).

use_single_doc_broadcast_batch(Config) ->
    [W | _] = Workers =  ?config(op_worker_nodes, Config),

    {ok, BatchSize} = test_utils:get_env(W, ?APP_NAME, dbsync_changes_broadcast_batch_size),
    ok = test_utils:set_env(Workers, ?APP_NAME, dbsync_changes_broadcast_batch_size, 1),

    [{batch_size, BatchSize} | Config].

use_default_broadcast_batch_size(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    BatchSize = ?config(batch_size, Config),
    ok = test_utils:set_env(Workers, ?APP_NAME, dbsync_changes_broadcast_batch_size, BatchSize).
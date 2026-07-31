%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains tests of the auto-cleaning mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("file/distribution_assert.hrl").
-include("env/space_setup_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    autocleaning_run_should_not_start_when_file_popularity_is_disabled/1,
    forcefully_started_autocleaning_should_return_error_when_file_popularity_is_disabled/1,
    autocleaning_run_should_not_start_when_autocleaning_is_disabled/1,
    forcefully_started_autocleaning_should_return_error_when_autocleaning_is_disabled/1,
    autocleaning_should_not_evict_file_replica_when_it_is_not_replicated/1,
    autocleaning_should_evict_file_replica_when_it_is_replicated/1,
    periodical_autocleaning_should_evict_file_replica_when_it_is_replicated/1,
    forcefully_started_autocleaning_should_evict_file_replica_when_it_is_replicated/1,
    autocleaning_should_evict_file_replica_replicated_by_job/1,
    autocleaning_should_evict_file_replica_replicated_by_qos/1,
    autocleaning_should_evict_file_replicas_until_it_reaches_configured_target/1,
    autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_open_count_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_hours_since_last_open_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_file_size_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_file_size_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_hourly_moving_average_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_daily_moving_average_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_monthly_moving_average_rule/1,
    restart_autocleaning_run_test/1,
    autocleaning_should_evict_file_when_it_is_old_enough/1,
    autocleaning_should_not_evict_opened_file_replica/1,
    cancel_autocleaning_run/1,
    time_warp_test/1
]).

all() -> [
    autocleaning_run_should_not_start_when_file_popularity_is_disabled,
    forcefully_started_autocleaning_should_return_error_when_file_popularity_is_disabled,
    autocleaning_run_should_not_start_when_autocleaning_is_disabled,
    forcefully_started_autocleaning_should_return_error_when_autocleaning_is_disabled,
    autocleaning_should_not_evict_file_replica_when_it_is_not_replicated,
    autocleaning_should_evict_file_replica_when_it_is_replicated,
    periodical_autocleaning_should_evict_file_replica_when_it_is_replicated,
    forcefully_started_autocleaning_should_evict_file_replica_when_it_is_replicated,
    autocleaning_should_evict_file_replica_replicated_by_job,
    autocleaning_should_evict_file_replica_replicated_by_qos,
    autocleaning_should_evict_file_replicas_until_it_reaches_configured_target,
    autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_open_count_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_hours_since_last_open_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_file_size_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_file_size_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_hourly_moving_average_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_daily_moving_average_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_monthly_moving_average_rule,
    restart_autocleaning_run_test,
    autocleaning_should_evict_file_when_it_is_old_enough,
    autocleaning_should_not_evict_opened_file_replica,
    cancel_autocleaning_run,
    time_warp_test
].

%% description of the environment shared by all the test cases - see describe_test_env/1
-type test_env() :: #{
    krk_node := node(),
    paris_node := node(),
    krk_session_id := session:id(),
    paris_session_id := session:id(),
    krk_provider_id := od_provider:id(),
    paris_provider_id := od_provider:id(),
    space_id := od_space:id(),
    space_guid := file_id:file_guid()
}.

-define(ATTEMPTS, 60).
% eviction of every single replica requires a round trip to the supporting provider
% (performed via dbsync), hence cleaning up a space holding a hundred replicas takes
% considerably longer than the single file cases
-define(BULK_EVICTION_ATTEMPTS, 180).
-define(MAX_LIMIT, 10000).
% cap on files created at once, so that the bulk test cases do not flood the provider
% with requests to the point where it starts rejecting them (?EAGAIN)
-define(MAX_PARALLEL_FILE_CREATIONS, 100).
% on a settled provider the flush takes well below a second; the cap is only there so that
% a node that never fully settles does not stall the auto-cleaning check indefinitely
-define(FLUSH_ATTEMPTS, 100).
-define(FLUSH_ATTEMPT_INTERVAL_MILLIS, 100).

% Gate suspending the auto-cleaning traverse at a chosen candidate
% (see mock_gated_candidate_processing/1)
% gate suspending the auto-cleaning traverse (see permit_gate_test_utils)
-define(CANDIDATE_PROCESSING_GATE, autocleaning_candidate_processing).

-define(FILE_NAME, <<"file_", (atom_to_binary(?FUNCTION_NAME, utf8))/binary>>).

-define(RULE_SETTING(Value), ?RULE_SETTING(true, Value)).
-define(RULE_SETTING(Enabled, Value), #{enabled => Enabled, value => Value}).

-define(assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid, ?ATTEMPTS)).

-define(assertFilesInView(Worker, SpaceId, ExpectedGuids),
    ?assertMatch([], begin
        % the query forces the view to catch up with all the already persisted documents,
        % which may time out while the provider is busy - such an answer is not a verdict
        % on the view content and must be retried rather than fail the test case
        case rpc:call(Worker, index, query, [SpaceId, <<"file-popularity">>, [{limit, ?MAX_LIMIT}, {stale, false}]]) of
            {ok, #{<<"rows">> := __Rows}} ->
                __Guids = [?id_to_guid(maps:get(<<"value">>, __Row)) || __Row <- __Rows],
                ExpectedGuids -- __Guids;
            __Error ->
                __Error
        end
    end, ?ATTEMPTS)).

-define(assertRunFinished(Worker, __ARId), ?assertRunFinished(Worker, __ARId, ?ATTEMPTS)).

-define(assertRunFinished(Worker, __ARId, __Attempts),
    ?assertEqual(true, begin
        {ok, Info} = get_run_report(Worker, __ARId),
        maps:get(stopped_at, Info) =/= null
    end, __Attempts)).

-define(assertReport(Expected, Worker, __ARId),
    ?assertRunFinished(Worker, __ARId),
    ?assertMatch(Expected, get_run_report(Worker, __ARId))
).

-define(assertOneOfReports(Expected, Worker, SpaceId),
    ?assertOneOfReports(Expected, Worker, SpaceId, ?ATTEMPTS)).

%%--------------------------------------------------------------------
%% Asserts that one of the space's auto-cleaning runs has finished and its report
%% matches the expectation. A space may hold reports of several runs, and only one of
%% them is expected to match, so the reports are examined with a plain pattern match
%% rather than with the assertion macros - the latter would log a failure summary for
%% every report that does not match, even when the assertion as a whole succeeds.
%% On failure the reports themselves are returned, so that the summary shows what the
%% space actually held.
%%--------------------------------------------------------------------
-define(assertOneOfReports(Expected, Worker, SpaceId, Attempts),
    ?assertEqual(matching_report_found, begin
        {ok, __ARIds} = list(Worker, SpaceId),
        __Reports = [__Report || __ARId <- __ARIds, {ok, __Report} <- [get_run_report(Worker, __ARId)]],
        case lists:any(fun(__Report) ->
            maps:get(stopped_at, __Report) =/= null andalso case {ok, __Report} of
                Expected -> true;
                _ -> false
            end
        end, __Reports) of
            true -> matching_report_found;
            false -> __Reports
        end
    end, Attempts)).

-define(id_to_guid(ObjectId), begin
    {ok, __Guid} = file_id:objectid_to_guid(ObjectId),
    __Guid
end).

-define(FAILED, <<"failed">>).
-define(COMPLETED, <<"completed">>).
-define(ACTIVE, <<"active">>).
-define(CANCELLED, <<"cancelled">>).

%%%===================================================================
%%% Negative tests
%%%===================================================================

autocleaning_run_should_not_start_when_file_popularity_is_disabled(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    write_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, Size),
    % the write changes the space occupancy, which must trigger the check
    assert_autocleaning_check_triggered(KrkNode, SpaceId),
    ?assertEqual({ok, []}, list(KrkNode, SpaceId)).

forcefully_started_autocleaning_should_return_error_when_file_popularity_is_disabled(Config) ->
    #{krk_node := KrkNode, space_id := SpaceId} = describe_test_env(Config),
    ?assertEqual(?ERR_FILE_POPULARITY_DISABLED, force_start(KrkNode, SpaceId)).

autocleaning_run_should_not_start_when_autocleaning_is_disabled(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    write_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, Size),
    % the write changes the space occupancy, which must trigger the check
    assert_autocleaning_check_triggered(KrkNode, SpaceId),
    ?assertEqual({ok, []}, list(KrkNode, SpaceId)).

forcefully_started_autocleaning_should_return_error_when_autocleaning_is_disabled(Config) ->
    #{krk_node := KrkNode, space_id := SpaceId} = describe_test_env(Config),
    enable_file_popularity(KrkNode, SpaceId),
    ?assertEqual(?ERR_AUTO_CLEANING_DISABLED, force_start(KrkNode, SpaceId)).

%%%===================================================================
%%% Basic eviction tests
%%%===================================================================

autocleaning_should_not_evict_file_replica_when_it_is_not_replicated(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    Guid = write_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, Size),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, 0]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0,
        status := ?FAILED
    }}, KrkNode, SpaceId).

autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    ok = enable_file_popularity(KrkNode, SpaceId),
    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),
    ok = configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, Guid),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, SpaceId).

periodical_autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    Guid = write_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, Size),
    ?assertDistribution(ParisNode, ParisSessId, ?DISTS([KrkId, ParisId], [Size, 0]), Guid),
    file_test_utils:replicate_by_read(KrkNode, ParisNode, ParisSessId, Guid),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    ?assertFilesInView(KrkNode, SpaceId, [Guid]),
    ?assertEqual(Size, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, SpaceId).

forcefully_started_autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    Guid = write_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, Size),
    ?assertDistribution(ParisNode, ParisSessId, ?DISTS([KrkId, ParisId], [Size, 0]), Guid),
    file_test_utils:replicate_by_read(KrkNode, ParisNode, ParisSessId, Guid),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    % Ensure that evicting provider has knowledge of remote provider blocks (through dbsync),
    % as otherwise it will skip eviction.
    % @TODO VFS-9498 not needed after replica_deletion uses fetched file location instead of dbsynced
    ?assertEqual({ok, [[0, Size]]},
        opt_file_metadata:get_local_knowledge_of_remote_provider_blocks(KrkNode, Guid, ParisId), ?ATTEMPTS),
    ?assertFilesInView(KrkNode, SpaceId, [Guid]),
    ?assertEqual(Size, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    {ok, ARId} = force_start(KrkNode, SpaceId),
    ?assertMatch({ok, [ARId]}, list(KrkNode, SpaceId), ?ATTEMPTS),
    ?assertRunFinished(KrkNode, ARId),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertReport({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, ARId).

restart_autocleaning_run_test(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    Target = 0,
    Threshold = Size - 1,

    enable_file_popularity(KrkNode, SpaceId),
    Guid = write_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, Size),

    % replicate file to the other provider
    ?assertDistribution(ParisNode, ParisSessId, ?DISTS([KrkId, ParisId], [Size, 0]), Guid),
    schedule_file_replication(ParisNode, ParisSessId, Guid, ParisId, Size),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    % Ensure that evicting provider has knowledge of remote provider blocks (through dbsync),
    % as otherwise it will skip eviction.
    % @TODO VFS-9498 not needed after replica_deletion uses fetched file location instead of dbsynced
    ?assertEqual({ok, [[0, Size]]},
        opt_file_metadata:get_local_knowledge_of_remote_provider_blocks(KrkNode, Guid, ParisId), ?ATTEMPTS),
    ?assertFilesInView(KrkNode, SpaceId, [Guid]),
    ?assertEqual(Size, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    % pretend that there is a stalled autocleaning_run
    Ctx = rpc:call(KrkNode, autocleaning_run, get_ctx, []),
    Doc = #document{
        value = #autocleaning_run{
            status = binary_to_atom(?ACTIVE, utf8),
            space_id = SpaceId,
            started_at = StartTime = rpc:call(KrkNode, global_clock, timestamp_seconds, []),
            bytes_to_release = Size - Target
        },
        scope = SpaceId
    },
    {ok, #document{key = ARId}} = rpc:call(KrkNode, datastore_model, create, [Ctx, Doc]),
    ok = rpc:call(KrkNode, autocleaning_run_links, add_link, [ARId, SpaceId, StartTime]),

    ok = configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => Target,
        threshold => Threshold
    }),
    {ok, _} = rpc:call(KrkNode, autocleaning, maybe_mark_current_run, [SpaceId, ARId]),
    {ok, ARId} = restart_autocleaning_run(KrkNode, SpaceId),
    ?assertMatch({ok, [ARId]}, list(KrkNode, SpaceId), ?ATTEMPTS),
    ?assertRunFinished(KrkNode, ARId),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertMatch({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, get_run_report(KrkNode, ARId)).

%%%===================================================================
%%% Eviction by replication method
%%%===================================================================

autocleaning_should_evict_file_replica_replicated_by_job(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),

    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),
    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    schedule_file_replication(KrkNode, KrkSessId, Guid, KrkId, Size),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    ?assertEqual(Size, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, SpaceId).

autocleaning_should_evict_file_replica_replicated_by_qos(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),

    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),
    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    {ok, QosEntryId} = opt_qos:add_qos_entry(KrkNode, KrkSessId, ?FILE_REF(Guid), <<"providerId=", KrkId/binary>>, 1),
    ?assertMatch({ok, {#{QosEntryId := _}, _}}, opt_qos:get_effective_file_qos(KrkNode, KrkSessId, ?FILE_REF(Guid))),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    ?assertEqual(Size, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    ok = opt_qos:remove_qos_entry(KrkNode, KrkSessId, QosEntryId),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, SpaceId).

%%%===================================================================
%%% Target and rules tests
%%%===================================================================

autocleaning_should_evict_file_replicas_until_it_reaches_configured_target(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),

    % NOTE: what this case is about is where the cleaning STOPS, so what matters is the
    % byte arithmetic, not the file count. The occupancy is therefore built from few big
    % files rather than many small ones - every evicted replica costs a round trip to the
    % supporting provider, and it is that count which drives both the runtime and its
    % spread (with 1000 files the case took anywhere between 2 and 9 minutes).
    FilesNum = 100,
    FileSize = 100,
    Target = 1000,
    Threshold = FilesNum * FileSize + 1,

    {ok, DirGuid} = lfm_proxy:mkdir(ParisNode, ParisSessId, SpaceGuid, <<"dir">>, ?DEFAULT_DIR_PERMS),
    Guids = write_files(ParisNode, ParisSessId, DirGuid, ?FILE_NAME, FileSize, FilesNum),

    ExtraFileSize = 1,
    EG = write_file(ParisNode, ParisSessId, SpaceGuid, <<"extra_file">>, ExtraFileSize),

    enable_file_popularity(KrkNode, SpaceId),
    % replicate all the files to the cleaning provider
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, Guids),

    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => Target,
        threshold => Threshold,
        rules => #{enabled => false}
    }),

    ?assertFilesInView(KrkNode, SpaceId, Guids),
    ?assertEqual(FilesNum * FileSize, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    % "On the fly" replication of the ExtraFile will cause occupancy to exceed the Threshold.
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, ExtraFileSize]), EG),
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, EG),
    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(KrkNode, SpaceId), ?ATTEMPTS),
    ?assertRunFinished(KrkNode, ARId, ?BULK_EVICTION_ATTEMPTS),
    ?assertEqual(true, opt_spaces:get_occupancy(KrkNode, SpaceId) =< Target, ?ATTEMPTS),
    % ensure that not all files will be cleaned
    ?assertEqual(true, opt_spaces:get_occupancy(KrkNode, SpaceId) >= 100, ?ATTEMPTS).

autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    ok = configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(0),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}),
    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, Guid),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, SpaceId).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_open_count_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(0),
            min_hours_since_last_open => ?RULE_SETTING(0),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_hours_since_last_open_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(2),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_file_size_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(1),
            min_file_size => ?RULE_SETTING(Size + 1),
            max_file_size => ?RULE_SETTING(100000000),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_file_size_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(1),
            min_file_size => ?RULE_SETTING(0),
            max_file_size => ?RULE_SETTING(Size - 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_hourly_moving_average_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(1),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(0),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_daily_moving_average_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(1),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(0),
            max_monthly_moving_average => ?RULE_SETTING(1)
        }}).

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_monthly_moving_average_rule(Config) ->
    Size = 10,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(1),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(0)
        }}).

autocleaning_should_evict_file_when_it_is_old_enough(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),
    ACConfig = #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(2),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
    }},
    ok = configure_autocleaning(KrkNode, SpaceId, ACConfig),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    schedule_file_replication(KrkNode, KrkSessId, Guid, KrkId, Size),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0
    }}, KrkNode, SpaceId),

    % pretend that file has not been opened for 2 hours
    CurrentTimestamp = rpc:call(KrkNode, global_clock, timestamp_hours, []),
    {ok, _} = change_last_open(KrkNode, Guid, CurrentTimestamp - 2),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, KrkNode, SpaceId).

autocleaning_should_not_evict_opened_file_replica(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    Size = 10,
    enable_file_popularity(KrkNode, SpaceId),
    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),
    ok = configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => Size - 1,
        rules => #{
            enabled => true,
            max_open_count => ?RULE_SETTING(1),
            min_hours_since_last_open => ?RULE_SETTING(0),
            min_file_size => ?RULE_SETTING(Size - 1),
            max_file_size => ?RULE_SETTING(Size + 1),
            max_hourly_moving_average => ?RULE_SETTING(1),
            max_daily_moving_average => ?RULE_SETTING(1),
            max_monthly_moving_average => ?RULE_SETTING(1)
    }}),

    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    % read file to be replicated and leave it opened
    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(KrkNode, KrkSessId, ?FILE_REF(Guid), read), ?ATTEMPTS),
    read_opened_file(KrkNode, KrkSessId, Guid, H, Size),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0,
        status := ?FAILED
    }}, KrkNode, SpaceId).

%%%===================================================================
%%% Lifecycle tests
%%%===================================================================

cancel_autocleaning_run(Config) ->
    % the traverse is gated in init_per_testcase, so that the run is cancelled at a
    % precisely chosen candidate
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),

    FilesNum = 50,
    % must leave enough candidates unprocessed for the run to be cancellable midway
    EvictedBeforeCancelNum = 10,
    FileSize = 10,
    Target = 0,
    Threshold = FilesNum * FileSize + 1,

    {ok, DirGuid} = lfm_proxy:mkdir(ParisNode, ParisSessId, SpaceGuid, <<"dir">>, ?DEFAULT_DIR_PERMS),
    Guids = write_files(ParisNode, ParisSessId, DirGuid, ?FILE_NAME, FileSize, FilesNum),

    ExtraFileSize = 1,
    EG = write_file(ParisNode, ParisSessId, SpaceGuid, <<"extra_file">>, ExtraFileSize),
    TotalSize = FilesNum * FileSize + ExtraFileSize,

    enable_file_popularity(KrkNode, SpaceId),
    % replicate all the files to the cleaning provider
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, Guids),

    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => Target,
        threshold => Threshold,
        rules => #{enabled => false}
    }),

    ?assertFilesInView(KrkNode, SpaceId, Guids),
    ?assertEqual(FilesNum * FileSize, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    % "On the fly" replication of the ExtraFile will cause occupancy to exceed the Threshold.
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, ExtraFileSize]), EG),
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, EG),
    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(KrkNode, SpaceId), ?ATTEMPTS),

    % the gate holds the traverse at its first candidate - wait for a job to park, which
    % proves the traverse is underway, then let a fixed number of candidates through
    await_gated_candidate_processing_job(),
    grant_candidate_processing_permits(KrkNode, EvictedBeforeCancelNum),

    ?assertEqual(run_progressed, case get_run_report(KrkNode, ARId) of
        {ok, #{
            released_bytes := __ReleasedBytes,
            bytes_to_release := TotalSize,
            files_number := __FilesNumber,
            status := ?ACTIVE
        }} when __ReleasedBytes > 0, __FilesNumber > 0 ->
            run_progressed;
        {ok, __Report} ->
            __Report
    end, ?ATTEMPTS),

    % the remaining candidates are still held at the gate, so the run cannot have finished
    cancel(KrkNode, SpaceId, ARId),

    % the parked jobs must be released for the traverse - and with it the run - to end;
    % they no longer evict anything, as process_row/3 checks autocleaning_run:is_active/1
    grant_candidate_processing_permits(KrkNode, all),
    ?assertRunFinished(KrkNode, ARId, ?ATTEMPTS),
    ?assertEqual(run_cancelled_midway, case get_run_report(KrkNode, ARId) of
        {ok, #{
            released_bytes := __ReleasedBytes,
            bytes_to_release := TotalSize,
            files_number := __FilesNumber,
            status := ?CANCELLED
        }} when __ReleasedBytes > 0, __ReleasedBytes < TotalSize,
                __FilesNumber > 0, __FilesNumber < FilesNum ->
            run_cancelled_midway;
        {ok, __Report} ->
            __Report
    end, ?ATTEMPTS).

time_warp_test(Config) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),

    FilesNum = 100,
    FileSize = 10,
    Target = 10,
    Threshold = FilesNum * FileSize + 1,

    {ok, DirGuid} = lfm_proxy:mkdir(ParisNode, ParisSessId, SpaceGuid, <<"dir">>, ?DEFAULT_DIR_PERMS),
    Guids = write_files(ParisNode, ParisSessId, DirGuid, ?FILE_NAME, FileSize, FilesNum),

    ExtraFileSize = 1,
    EG = write_file(ParisNode, ParisSessId, SpaceGuid, <<"extra_file">>, ExtraFileSize),
    TotalSize = FilesNum * FileSize + ExtraFileSize,
    BytesToRelease = TotalSize - Target,

    enable_file_popularity(KrkNode, SpaceId),
    % replicate all the files to the cleaning provider
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, Guids),

    configure_autocleaning(KrkNode, SpaceId, #{
        enabled => true,
        target => Target,
        threshold => Threshold,
        rules => #{enabled => false}
    }),

    ?assertFilesInView(KrkNode, SpaceId, Guids),
    ?assertEqual(FilesNum * FileSize, opt_spaces:get_occupancy(KrkNode, SpaceId), ?ATTEMPTS),
    StartTimeSeconds = 1000000000, % 10 ^ 9

    ok = time_test_utils:set_current_time_seconds(StartTimeSeconds),
    % "On the fly" replication of the ExtraFile will cause occupancy to exceed the Threshold.
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, ExtraFileSize]), EG),
    file_test_utils:replicate_by_read(ParisNode, KrkNode, KrkSessId, EG),
    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(KrkNode, SpaceId), ?ATTEMPTS),

    % pretend that there has been a 10 hour backward time warp
    BackwardTimeWarpSeconds = 36000,
    time_test_utils:simulate_seconds_passing(-BackwardTimeWarpSeconds),

    % the run evicts ~100 replicas, i.e. the same bulk as
    % autocleaning_should_evict_file_replicas_until_it_reaches_configured_target - each
    % eviction is a dbsync round trip to the supporting provider, so it needs the bulk budget
    ?assertRunFinished(KrkNode, ARId, ?BULK_EVICTION_ATTEMPTS),
    StartTimeIso8601 = time:seconds_to_iso8601(StartTimeSeconds),
    % stop time should be equal to start time as it cannot be lower
    ?assertEqual(true, begin
        {ok, #{
            released_bytes := ReleasedBytes,
            bytes_to_release := BytesToRelease,
            status := ?COMPLETED,
            started_at := StartTimeIso8601,
            stopped_at := StartTimeIso8601
        }} = get_run_report(KrkNode, ARId),
        BytesToRelease =< ReleasedBytes
    end, ?ATTEMPTS).

%%%===================================================================
%%% Test bases
%%%===================================================================

%% @private
-spec autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(
    test_config:config(), Size :: non_neg_integer(), autocleaning:config()
) ->
    ok | no_return().
autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, ACConfig) ->
    #{
        krk_node := KrkNode, paris_node := ParisNode,
        krk_session_id := KrkSessId, paris_session_id := ParisSessId,
        krk_provider_id := KrkId, paris_provider_id := ParisId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    enable_file_popularity(KrkNode, SpaceId),
    ok = configure_autocleaning(KrkNode, SpaceId, ACConfig),
    Guid = write_file(ParisNode, ParisSessId, SpaceGuid, ?FILE_NAME, Size),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [0, Size]), Guid),
    schedule_file_replication(KrkNode, KrkSessId, Guid, KrkId, Size),
    ?assertDistribution(KrkNode, KrkSessId, ?DISTS([KrkId, ParisId], [Size, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0,
        status := ?FAILED
    }}, KrkNode, SpaceId).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE, permit_gate_test_utils]} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {autocleaning_restart_runs, false},
            % the periodic check is disabled, so that the only thing that triggers
            % auto-cleaning is the space occupancy change the test case makes; the cases
            % that do rely on it enable it back for themselves (see init_per_testcase/2)
            {autocleaning_periodic_spaces_check_enabled, false},
            % NOTE: the interval must stay well below ?ATTEMPTS, otherwise a case relying
            % on the periodic check would be racing its own assertions. The timer ticks
            % (and does nothing) even while the check is disabled, so the value applies
            % from the very first tick after a case enables it.
            {autocleaning_periodic_spaces_check_interval, timer:seconds(1)},
            {autocleaning_view_batch_size, 1000},
            {replica_deletion_max_parallel_requests, 1000},
            % ensure that all file blocks will be public
            {public_block_size_threshold, 0},
            {public_block_percent_threshold, 0}
        ]}],
        posthook = fun(NewConfig) ->
            KrkNodes = oct_background:get_provider_nodes(krakow),
            % Undo whatever a previous, interrupted run of this suite may have left behind.
            % The deployment is reused between runs and the envs declared above reach the
            % nodes through the app config file written when it is started - they are NOT
            % reapplied per suite run (see oct_environment:setup_environment/2, which calls
            % set_custom_envs/1 only under coverage). Anything a test case sets up for
            % itself and undoes in end_per_testcase/2 therefore outlives a run that never
            % got to run that teardown, and does so silently.
            lists:foreach(fun(KrkNode) ->
                % a gate left closed would park every auto-cleaning traverse on the node,
                % in any space, forever - its permits are exhausted and the test process it
                % notifies is long dead (see mock_gated_candidate_processing/1)
                unmock_gated_candidate_processing(KrkNode),
                % a periodic check left enabled would trigger runs the cases do not expect,
                % which is what made this suite a coin flip before the env name was fixed
                ok = disable_periodic_spaces_autocleaning_check(KrkNode)
            end, KrkNodes),
            % a clock left frozen would break every case whose rules depend on time passing
            ok = time_test_utils:unfreeze_time(NewConfig),

            % Auto-cleaning check is triggered by a datastore posthook that can be executed before
            % the documents it accounts for are flushed. Wait for the flush to ensure the expected
            % behaviour occurs during the first check after any changes.
            % Only krakow is mocked - it is the provider that performs the cleaning, and the check
            % is always run by the provider whose space occupancy has changed.
            ok = test_utils:mock_new(KrkNodes, autocleaning_api, [passthrough]),
            ok = test_utils:mock_expect(KrkNodes, autocleaning_api, check, fun(SpaceId) ->
                wait_for_flush(),
                meck:passthrough([SpaceId])
            end),
            space_setup_utils:clean_up_after_previous_run(all(), [krakow, paris]),
            NewConfig
        end
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 20}),
    KrkNode = oct_background:get_random_provider_node(krakow),
    case Case of
        cancel_autocleaning_run ->
            % suspends the run at a chosen candidate, so that it is guaranteed to still be
            % ongoing when the case cancels it
            mock_gated_candidate_processing(KrkNode);
        periodical_autocleaning_should_evict_file_replica_when_it_is_replicated ->
            ok = enable_periodic_spaces_autocleaning_check(KrkNode);
        autocleaning_should_evict_file_when_it_is_old_enough ->
            ok = enable_periodic_spaces_autocleaning_check(KrkNode);
        time_warp_test ->
            time_test_utils:freeze_time(Config);
        _ ->
            ok
    end,
    SpaceId = set_up_space(Case),
    lfm_proxy:init([{space_id, SpaceId} | Config]).

end_per_testcase(Case, Config) ->
    #{krk_node := KrkNode, paris_node := ParisNode} = describe_test_env(Config),
    lfm_proxy:close_all(KrkNode),
    lfm_proxy:close_all(ParisNode),
    lfm_proxy:teardown(Config),
    case Case of
        cancel_autocleaning_run ->
            unmock_gated_candidate_processing(KrkNode);
        periodical_autocleaning_should_evict_file_replica_when_it_is_replicated ->
            ok = disable_periodic_spaces_autocleaning_check(KrkNode);
        autocleaning_should_evict_file_when_it_is_old_enough ->
            ok = disable_periodic_spaces_autocleaning_check(KrkNode);
        time_warp_test ->
            time_test_utils:unfreeze_time(Config);
        _ ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc
%% All the test cases share the same environment - a space set up for the test case
%% (see init_per_testcase/2) and supported by two providers, out of which krakow is
%% always the one that performs the cleaning and paris the one that keeps the second
%% replica, so that the cleaned replicas are never the last ones.
%% @end
-spec describe_test_env(test_config:config()) -> test_env().
describe_test_env(Config) ->
    SpaceId = ?config(space_id, Config),
    #{
        krk_node => oct_background:get_random_provider_node(krakow),
        paris_node => oct_background:get_random_provider_node(paris),
        krk_session_id => oct_background:get_user_session_id(user1, krakow),
        paris_session_id => oct_background:get_user_session_id(user1, paris),
        krk_provider_id => oct_background:get_provider_id(krakow),
        paris_provider_id => oct_background:get_provider_id(paris),
        space_id => SpaceId,
        space_guid => space_dir:guid(SpaceId)
    }.

%% @private
-spec set_up_space(Case :: atom()) -> od_space:id().
set_up_space(Case) ->
    space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = user1,
        supports = [
            #support_spec{provider = krakow, storage_spec = create_nulldevice_storage(krakow), size = 10737418240},
            #support_spec{provider = paris, storage_spec = create_nulldevice_storage(paris), size = 10737418240}
        ]
    }).

%% @private
% none of the test cases inspects the files on storage - they only check the file
% distribution and the auto-cleaning reports, so a null device storage is used to
% keep the storage out of the equation (some cases operate on thousands of files)
-spec create_nulldevice_storage(oct_background:entity_selector()) -> storage:id().
create_nulldevice_storage(ProviderSelector) ->
    space_setup_utils:create_storage(ProviderSelector, #nulldevice_storage_params{}).

%% @private
-spec disable_periodic_spaces_autocleaning_check(node()) -> ok.
disable_periodic_spaces_autocleaning_check(Worker) ->
    test_utils:set_env(Worker, ?APP_NAME, autocleaning_periodic_spaces_check_enabled, false).

%% @private
-spec enable_periodic_spaces_autocleaning_check(node()) -> ok.
enable_periodic_spaces_autocleaning_check(Worker) ->
    test_utils:set_env(Worker, ?APP_NAME, autocleaning_periodic_spaces_check_enabled, true).

%% @private
-spec write_file(
    node(), session:id(), ParentGuid :: file_id:file_guid(), file_meta:name(),
    Size :: non_neg_integer()
) ->
    file_id:file_guid().
write_file(Worker, SessId, ParentGuid, Name, Size) ->
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ParentGuid, Name, ?DEFAULT_FILE_PERMS),
    {ok, H} = lfm_proxy:open(Worker, SessId, ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker, H, 0, crypto:strong_rand_bytes(Size)),
    ok = lfm_proxy:close(Worker, H),
    Guid.

%% @private
-spec write_files(
    node(), session:id(), ParentGuid :: file_id:file_guid(), Prefix :: binary(),
    Size :: non_neg_integer(), Num :: non_neg_integer()
) ->
    [file_id:file_guid()].
write_files(Worker, SessId, ParentGuid, Prefix, Size, Num) ->
    lists_utils:pmap(fun(N) ->
        write_file(Worker, SessId, ParentGuid, <<Prefix/binary, (integer_to_binary(N))/binary>>, Size)
    end, lists:seq(1, Num), ?MAX_PARALLEL_FILE_CREATIONS).

%% @private
-spec read_opened_file(
    node(), session:id(), file_id:file_guid(), lfm_proxy:handle(), Size :: non_neg_integer()
) ->
    ok | no_return().
read_opened_file(Worker, SessId, Guid, Handle, Size) ->
    ?assertMatch({ok, #file_attr{size = Size}}, lfm_proxy:stat(Worker, SessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertEqual(Size, try
        {ok, Data} = lfm_proxy:read(Worker, Handle, 0, Size),
        byte_size(Data)
    catch
        Class:Reason ->
            {Class, Reason}
    end).

%% @private
-spec schedule_file_replication(
    node(), session:id(), file_id:file_guid(), od_provider:id(), ExpectedSize :: non_neg_integer()
) ->
    {ok, transfer:id()} | no_return().
schedule_file_replication(Worker, SessId, Guid, ProviderId, ExpectedSize) ->
    ?assertMatch({ok, #file_attr{size = ExpectedSize}}, lfm_proxy:stat(Worker, SessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    {ok, _} = opt_transfers:schedule_file_replication(Worker, SessId, ?FILE_REF(Guid), ProviderId).

%% @doc
%% Enables file popularity and waits until its view is not only created, but also
%% queryable. Enabling creates a couchbase design document per space, whose index is
%% built lazily - and auto-cleaning queries that view WITHOUT forcing it to catch up
%% (see autocleaning_view_traverse:65-70). A check triggered before the very first
%% build would therefore find no cleaning candidates, complete having released nothing,
%% and never be retried. The stale=false query below pays that first build here, in the
%% setup, instead of leaving it in the path of the tested mechanism.
%% NOTE: every test case runs in a space of its own, hence a cold view in every single
%% one of them - which is what makes this necessary.
%% @end
-spec enable_file_popularity(node(), od_space:id()) -> ok | no_return().
enable_file_popularity(Worker, SpaceId) ->
    ok = rpc:call(Worker, file_popularity_api, enable, [SpaceId]),
    ?assertMatch({ok, _}, rpc:call(Worker, index, query, [
        SpaceId, <<"file-popularity">>, [{limit, 1}, {stale, false}]
    ]), ?ATTEMPTS),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Mocks the auto-cleaning traverse so that processing of every cleaning candidate must
%% first acquire a permit. A job finding no free permit notifies the test process (see
%% await_gated_candidate_processing_job/0) and parks until permits are granted with
%% grant_candidate_processing_permits/2; no permits are available initially, so the run
%% is suspended at its very first candidate. This makes "the run is still ongoing when it
%% is cancelled" a property of the setup rather than a race against the eviction throughput.
%% Must be paired with unmock_gated_candidate_processing/1 in the test teardown.
%% @end
%%--------------------------------------------------------------------
-spec mock_gated_candidate_processing(node()) -> ok.
mock_gated_candidate_processing(Node) ->
    TestProcess = self(),
    ok = permit_gate_test_utils:install(?CANDIDATE_PROCESSING_GATE, Node),
    ok = test_utils:mock_new(Node, autocleaning_view_traverse, [passthrough]),
    ok = test_utils:mock_expect(Node, autocleaning_view_traverse, process_row, fun(
        Row, Info, RowNumber
    ) ->
        permit_gate_test_utils:acquire_permit(?CANDIDATE_PROCESSING_GATE, TestProcess),
        meck:passthrough([Row, Info, RowNumber])
    end).


-spec grant_candidate_processing_permits(node(), pos_integer() | all) -> ok.
grant_candidate_processing_permits(Node, CountOrAll) ->
    permit_gate_test_utils:grant_permits(?CANDIDATE_PROCESSING_GATE, Node, CountOrAll).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the notification a gated candidate job sends when it parks awaiting a permit -
%% proof that the traverse is underway and suspended. Only jobs that actually park notify,
%% so leftover mailbox messages cannot produce a false positive for an already-drained gate.
%% @end
%%--------------------------------------------------------------------
-spec await_gated_candidate_processing_job() -> ok.
await_gated_candidate_processing_job() ->
    permit_gate_test_utils:await_parked_job(?CANDIDATE_PROCESSING_GATE, ?ATTEMPTS).


%%--------------------------------------------------------------------
%% @doc
%% Removes the candidate processing gate. Tolerates the gate not being installed at all,
%% so that it can also be used to clear one left behind by an interrupted run.
%% @end
%%--------------------------------------------------------------------
-spec unmock_gated_candidate_processing(node()) -> ok.
unmock_gated_candidate_processing(Node) ->
    % cancelling a run does not release the jobs parked at the gate, and the traverse
    % slave job pool is shared by the whole node - jobs left parked would outlive this
    % test case and stall the auto-cleaning of every later one
    permit_gate_test_utils:uninstall(
        ?CANDIDATE_PROCESSING_GATE, Node, autocleaning_view_traverse
    ).


%% @private
%% @doc
%% Asserts that the change of space occupancy has triggered an auto-cleaning check.
%% Deliberately does NOT pin the number of checks: space_quota posthooks are debounced
%% by autocleaning_checker (a per-space gen_server that calls autocleaning_api:check/1
%% from its terminate/2 after ?CHECK_AUTOCLEANING_DELAY of silence), so the count equals
%% the number of bursts of document updates separated by more than that delay. A single
%% file write is one burst only as long as its constituent updates stay close enough in
%% time - under load they spread out and the very same write is reported as two or three
%% checks. Asserting equality is therefore a race that no retrying can win: once the
%% count exceeds the expectation it never comes back down.
%% @end
-spec assert_autocleaning_check_triggered(node(), od_space:id()) -> ok | no_return().
assert_autocleaning_check_triggered(Worker, SpaceId) ->
    ?assertEqual(true, begin
        rpc:call(Worker, meck, num_calls, [autocleaning_api, check, [SpaceId]]) >= 1
    end, ?ATTEMPTS).

%% @private
-spec configure_autocleaning(node(), od_space:id(), autocleaning:config()) ->
    {ok, od_space:id()} | {error, term()}.
configure_autocleaning(Worker, SpaceId, Configuration) ->
    rpc:call(Worker, autocleaning_api, configure, [SpaceId, Configuration]).

%% @private
-spec force_start(node(), od_space:id()) -> {ok, autocleaning_run:id()} | {error, term()}.
force_start(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, force_run, [SpaceId]).

%% @private
-spec cancel(node(), od_space:id(), autocleaning_run:id()) -> ok | {error, term()}.
cancel(Worker, SpaceId, AutocleaningRunId) ->
    rpc:call(Worker, autocleaning_api, cancel_run, [SpaceId, AutocleaningRunId]).

%% @private
-spec restart_autocleaning_run(node(), od_space:id()) -> ok | {error, term()}.
restart_autocleaning_run(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, restart_autocleaning_run, [SpaceId]).

%% @private
-spec list(node(), od_space:id()) -> {ok, [autocleaning_run:id()]} | {error, term()}.
list(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, list_reports, [SpaceId]).

%% @private
-spec get_run_report(node(), autocleaning_run:id()) -> {ok, map()} | {error, term()}.
get_run_report(Worker, ARId) ->
    rpc:call(Worker, autocleaning_api, get_run_report, [ARId]).

%% @private
-spec change_last_open(node(), file_id:file_guid(), NewLastOpen :: non_neg_integer()) ->
    {ok, file_popularity:doc()} | {error, term()}.
change_last_open(Worker, FileGuid, NewLastOpen) ->
    Uuid = file_id:guid_to_uuid(FileGuid),
    rpc:call(Worker, file_popularity, update, [Uuid, fun(FP) ->
        {ok, FP#file_popularity{last_open = NewLastOpen}}
    end]).

%% @private
%% @doc
%% Waits until all the datastore documents pending on this node have been sent to the
%% database. Runs on the provider node, in the process performing the auto-cleaning
%% check - which is called from the space_quota update posthook, so it must be cheap
%% and must never raise; a crash here fails every concurrent space_quota update.
%% NOTE: deliberately does not consult the database disk write queue
%% (couchbase_config:get_flush_queue_size/0) - it issues an HTTP request to the couchbase
%% REST API per bucket and hard-matches its answer, which under load times out.
%% @end
-spec wait_for_flush() -> ok.
wait_for_flush() ->
    wait_for_flush(?FLUSH_ATTEMPTS).

%% @private
-spec wait_for_flush(non_neg_integer()) -> ok.
wait_for_flush(0) ->
    ok;
wait_for_flush(AttemptsLeft) ->
    case count_pending_datastore_ops() of
        0 ->
            ok;
        _ ->
            % inspecting the queues is not free (it copies the tp size tables) - polling
            % without a pause would burn a core of the very provider that is expected
            % to do the cleaning
            timer:sleep(?FLUSH_ATTEMPT_INTERVAL_MILLIS),
            wait_for_flush(AttemptsLeft - 1)
    end.

%% @private
-spec count_pending_datastore_ops() -> non_neg_integer().
count_pending_datastore_ops() ->
    try
        QueueSizeSum = lists:foldl(fun(Bucket, Sum) ->
            {_Max, S} = couchbase_pool:get_worker_queue_size_stats(Bucket),
            Sum + S
        end, 0, couchbase_config:get_buckets()),
        QueueSizeSum + tp_router:get_process_size_sum()
    catch _:_ ->
        % the queues cannot be inspected - do not hold the check back any longer
        0
    end.

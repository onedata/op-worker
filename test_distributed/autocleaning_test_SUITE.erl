%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of auto-cleaning mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("distribution_assert.hrl").
-include("lfm_test_utils.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

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
    cancel_autocleaning_run/1]).

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
    cancel_autocleaning_run
].

-define(SPACE_ID, <<"space1">>).
-define(FILE_NAME, <<"file_", (atom_to_binary(?FUNCTION_NAME, utf8))/binary>>).

-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

-define(USER, <<"user1">>).
-define(SESSION(Worker, Config), ?SESS_ID(?USER, Worker, Config)).

-define(ATTEMPTS, 300).
-define(LIMIT, 10).
-define(MAX_LIMIT, 10000).
-define(MAX_VAL, 1000000000).

-define(RULE_SETTING(Value), ?RULE_SETTING(true, Value)).
-define(RULE_SETTING(Enabled, Value), #{enabled => Enabled, value => Value}).

-define(assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid, ?ATTEMPTS)).

-define(assertFilesInView(Worker, SpaceId, ExpectedGuids),
    ?assertMatch([], begin
        {ok, #{<<"rows">> := Rows}} = rpc:call(Worker, index, query, [SpaceId, <<"file-popularity">>, [{limit, ?MAX_LIMIT}, {stale, false}]]),
        __FileIds = [maps:get(<<"value">>, Row) || Row <- Rows],
        __Guids = [?id_to_guid(__F) || __F <- __FileIds],
        ExpectedGuids -- __Guids
    end, ?ATTEMPTS)).

-define(assertRunFinished(Worker, __ARId),
    ?assertEqual(true, begin
        {ok, Info} = get_run_report(Worker, __ARId),
        maps:get(stopped_at, Info) =/= null
    end, ?ATTEMPTS)).

-define(assertReport(Expected, Worker, __ARId),
    ?assertRunFinished(Worker, __ARId),
    ?assertMatch(Expected, get_run_report(Worker, __ARId))
).

-define(assertOneOfReports(Expected, Worker, SpaceId),
    ?assertOneOfReports(Expected, Worker, SpaceId, ?ATTEMPTS)).

-define(assertOneOfReports(Expected, Worker, SpaceId, Attempts),
    ?assertEqual(true, begin
        {ok, __ARIds} =  list(Worker, SpaceId),
        lists:foldl(fun
            (_, true) ->
                true;
            (__ARId, false) ->
                try
                   ?assertReport(Expected, Worker, __ARId),
                    true
                catch
                    _:_ ->
                        false
                end
        end, false, __ARIds)
    end, Attempts)).


-define(id_to_guid(ObjectId), begin
    {ok, __Guid} = file_id:objectid_to_guid(ObjectId),
    __Guid
end).

-define(FAILED, <<"failed">>).
-define(COMPLETED, <<"completed">>).
-define(ACTIVE, <<"active">>).
-define(CANCELLING, <<"cancelling">>).
-define(CANCELLED, <<"cancelled">>).

%%%===================================================================
%%% API
%%%===================================================================

autocleaning_run_should_not_start_when_file_popularity_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    test_utils:mock_new(W, autocleaning_api, [passthrough]),
    test_utils:mock_new(W, autocleaning_run, [passthrough]),
    write_file(W, SessId, ?FILE_PATH(FileName), Size),
    test_utils:mock_assert_num_calls(W, autocleaning_api, check, ['_'], 1, ?ATTEMPTS),
    test_utils:mock_assert_num_calls(W, autocleaning_run, start, ['_', '_', '_'], 0, ?ATTEMPTS).

forcefully_started_autocleaning_should_return_error_when_file_popularity_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(?ERROR_FILE_POPULARITY_DISABLED, force_start(W, ?SPACE_ID)).

autocleaning_run_should_not_start_when_autocleaning_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    test_utils:mock_new(W, autocleaning_api, [passthrough]),
    test_utils:mock_new(W, autocleaning_run, [passthrough]),
    enable_file_popularity(W, ?SPACE_ID),
    write_file(W, SessId, ?FILE_PATH(FileName), Size),
    test_utils:mock_assert_num_calls(W, autocleaning_api, check, ['_'], 1, ?ATTEMPTS),
    test_utils:mock_assert_num_calls(W, autocleaning_run, start, ['_', '_', '_'], 0, ?ATTEMPTS).

forcefully_started_autocleaning_should_return_error_when_autocleaning_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    enable_file_popularity(W, ?SPACE_ID),
    ?assertEqual(?ERROR_AUTO_CLEANING_DISABLED, force_start(W, ?SPACE_ID)).

autocleaning_should_not_evict_file_replica_when_it_is_not_replicated(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    enable_file_popularity(W1, ?SPACE_ID),
    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),
    ?assertDistribution(W1, SessId, ?DIST(DomainP1, Size), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0,
        status := ?FAILED
    }}, W1, ?SPACE_ID).

autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    ok = enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),
    ok = configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),

    % read file on W1 to replicate it
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [Size]), Guid),
    read_file(W1, SessId, Guid, Size),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, W1, ?SPACE_ID).

periodical_autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),
    ?assertDistribution(W2, SessId2, ?DISTS([DomainP1], [Size]), Guid),
    % read file on W2 to replicate it
    read_file(W2, SessId2, Guid, Size),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertFilesInView(W1, ?SPACE_ID, [Guid]),
    ?assertEqual(Size, current_size(W1, ?SPACE_ID), ?ATTEMPTS),
    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, W1, ?SPACE_ID).

forcefully_started_autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),
    ?assertDistribution(W2, SessId2, ?DISTS([DomainP1], [Size]), Guid),
    % read file on W2 to replicate it
    read_file(W2, SessId2, Guid, Size),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertFilesInView(W1, ?SPACE_ID, [Guid]),
    ?assertEqual(Size, current_size(W1, ?SPACE_ID), ?ATTEMPTS),
    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    {ok, ARId} = force_start(W1, ?SPACE_ID),
    ?assertMatch({ok, [ARId]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertReport({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, W1, ARId).

autocleaning_should_evict_file_replica_replicated_by_job(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    ProviderId1 = provider_id(W1),
    enable_file_popularity(W1, ?SPACE_ID),

    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),
    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [Size]), Guid),
    schedule_file_replication(W1, SessId, Guid, ProviderId1),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertEqual(Size, current_size(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, W1, ?SPACE_ID).

autocleaning_should_evict_file_replicas_until_it_reaches_configured_target(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),

    FilesNum = 1000,
    FileSize = 10,
    Target = 1000,
    Threshold = FilesNum * FileSize + 1,

    DirName = <<"dir">>,
    DirPath = ?FILE_PATH(DirName),
    {ok, _} = lfm_proxy:mkdir(W2, SessId2, DirPath),

    FilePrefix = ?FILE_NAME,
    Guids = write_files(W2, SessId2, DirPath, FilePrefix, FileSize, FilesNum),

    ExtraFile = <<"extra_file">>,
    ExtraFileSize = 1,
    EG = write_file(W2, SessId2, ?FILE_PATH(ExtraFile), ExtraFileSize),

    enable_file_popularity(W1, ?SPACE_ID),
    lists:foreach(fun(G) ->
        ?assertDistribution(W1, SessId, ?DIST(DomainP2, FileSize), G),
        schedule_file_replication(W1, SessId, G, DomainP1)
    end, Guids),

    lists:foreach(fun(G) ->
        ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [FileSize, FileSize]), G)
    end, Guids),

    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => Target,
        threshold => Threshold,
        rules => #{enabled => false}
    }),

    ?assertFilesInView(W1, ?SPACE_ID, Guids),
    ?assertEqual(FilesNum * FileSize, current_size(W1, ?SPACE_ID), ?ATTEMPTS),
    % "On the fly" replication of the ExtraFile will cause occupancy
    % to exceed the Threshold.
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [ExtraFileSize]), EG),
    read_file(W1, SessId, EG, ExtraFileSize),
    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertEqual(true, current_size(W1, ?SPACE_ID) =< Target, ?ATTEMPTS),
    % ensure that not all files will be cleaned
    ?assertEqual(true, current_size(W1, ?SPACE_ID) >= 100, ?ATTEMPTS).

autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    ok = configure_autocleaning(W1, ?SPACE_ID, #{
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
    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),

    % read file on W1 to replicate it
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [Size]), Guid),
    read_file(W1, SessId, Guid, Size),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, W1, ?SPACE_ID).

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

restart_autocleaning_run_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    Target = 0,
    Threshold = Size - 1,

    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),

    % replicate file to W2
    ?assertDistribution(W2, SessId2, ?DISTS([DomainP1], [Size]), Guid),
    schedule_file_replication(W2, SessId2, Guid, DomainP2),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertFilesInView(W1, ?SPACE_ID, [Guid]),
    ?assertEqual(Size, current_size(W1, ?SPACE_ID), ?ATTEMPTS),
    % pretend that there is stalled autocleaning_run
    Ctx = rpc:call(W1, autocleaning_run, get_ctx, []),
    Doc = #document{
        value = #autocleaning_run{
            status = binary_to_atom(?ACTIVE, utf8),
            space_id = ?SPACE_ID,
            started_at = StartTime = rpc:call(W1, global_clock, timestamp_seconds, []),
            bytes_to_release = Size - Target
        },
        scope = ?SPACE_ID
    },
    {ok, #document{key = ARId}} = rpc:call(W1, datastore_model, create, [Ctx, Doc]),
    ok = rpc:call(W1, autocleaning_run_links, add_link, [ARId, ?SPACE_ID, StartTime]),

    ok = configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => Target,
        threshold => Threshold
    }),
    {ok, _} = rpc:call(W1, autocleaning, maybe_mark_current_run, [?SPACE_ID, ARId]),
    {ok, ARId} = restart_autocleaning_run(W1, ?SPACE_ID),
    ?assertMatch({ok, [ARId]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertMatch({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }}, get_run_report(W1, ARId)).

autocleaning_should_evict_file_when_it_is_old_enough(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    Size = 10,
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),
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
    ok = configure_autocleaning(W1, ?SPACE_ID, ACConfig),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [Size]), Guid),
    schedule_file_replication(W1, SessId, Guid, DomainP1),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0
    }},  W1, ?SPACE_ID),

    % pretend that file has not been opened for 2 hours
    CurrentTimestamp = rpc:call(W1, global_clock, timestamp_hours, []),
    {ok, _} = change_last_open(W1, Guid, CurrentTimestamp - 2),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1,
        status := ?COMPLETED
    }},  W1, ?SPACE_ID).

autocleaning_should_not_evict_opened_file_replica(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),
    ok = configure_autocleaning(W1, ?SPACE_ID, #{
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

    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [Size]), Guid),
    % read file to be replicated and leave it opened
    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, Guid}, read), ?ATTEMPTS),
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:read(W1, H, 0, Size), ?ATTEMPTS),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0,
        status := ?FAILED
    }},  W1, ?SPACE_ID).

cancel_autocleaning_run(Config) ->
    % replica_deletion_max_parallel_requests is decreased in init_per_testcase
    % so that cleaning will be slower
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),

    FilesNum = 1000,
    FileSize = 10,
    Target = 0,
    Threshold = FilesNum * FileSize + 1,

    DirName = <<"dir">>,
    DirPath = ?FILE_PATH(DirName),
    {ok, _} = lfm_proxy:mkdir(W2, SessId2, DirPath),

    FilePrefix = ?FILE_NAME,
    Guids = write_files(W2, SessId2, DirPath, FilePrefix, FileSize, FilesNum),

    ExtraFile = <<"extra_file">>,
    ExtraFileSize = 1,
    EG = write_file(W2, SessId2, ?FILE_PATH(ExtraFile), ExtraFileSize),
    TotalSize = FilesNum * FileSize + ExtraFileSize,

    enable_file_popularity(W1, ?SPACE_ID),
    lists:foreach(fun(G) ->
        ?assertDistribution(W1, SessId, ?DIST(DomainP2, FileSize), G),
        schedule_file_replication(W1, SessId, G, DomainP1)
    end, Guids),

    lists:foreach(fun(G) ->
        ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [FileSize, FileSize]), G)
    end, Guids),

    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => Target,
        threshold => Threshold,
        rules => #{enabled => false}
    }),

    ?assertFilesInView(W1, ?SPACE_ID, Guids),
    ?assertEqual(FilesNum * FileSize, current_size(W1, ?SPACE_ID), ?ATTEMPTS),
    % "On the fly" replication of the ExtraFile will cause occupancy
    % to exceed the Threshold.
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [ExtraFileSize]), EG),
    read_file(W1, SessId, EG, ExtraFileSize),
    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),

    ?assertEqual(true, begin
        {ok, #{
            released_bytes := ReleasedBytes,
            bytes_to_release := TotalSize,
            files_number := FilesNumber,
            status := ?ACTIVE
        }} = get_run_report(W1, ARId),
        ReleasedBytes > 0 andalso FilesNumber > 0
    end, ?ATTEMPTS),

    cancel(W1, ?SPACE_ID, ARId),
    ?assertMatch({ok, #{status := ?CANCELLING}}, get_run_report(W1, ARId), ?ATTEMPTS),

    ?assertRunFinished(W1, ARId),
    ?assertEqual(true, begin
        {ok, #{
            released_bytes := ReleasedBytes,
            bytes_to_release := TotalSize,
            files_number := FilesNumber,
            status := ?CANCELLED
        }} = get_run_report(W1, ARId),
        ReleasedBytes > 0 andalso ReleasedBytes < TotalSize andalso
        FilesNumber > 0 andalso FilesNumber < FilesNum
    end, ?ATTEMPTS).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        application:start(ssl),
        hackney:start(),
        NewConfig2 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig),
        Workers = ?config(op_worker_nodes, NewConfig2),
        test_utils:set_env(Workers, op_worker, autocleaning_restart_runs, false),
        sort_workers(NewConfig2)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].

init_per_testcase(Case, Config) when
    Case =:= forcefully_started_autocleaning_should_evict_file_replica_when_it_is_replicated orelse
    Case =:= autocleaning_should_evict_file_replicas_until_it_reaches_configured_target orelse
    Case =:= restart_autocleaning_run_test
->
    [W | _] = ?config(op_worker_nodes, Config),
    disable_periodical_spaces_autocleaning_check(W),
    init_per_testcase(default, Config);

init_per_testcase(cancel_autocleaning_run, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(W, op_worker, autocleaning_view_batch_size, 10),
    ok = test_utils:set_env(W, op_worker, replica_deletion_max_parallel_requests, 10),
    disable_periodical_spaces_autocleaning_check(W),
    init_per_testcase(default, Config);

init_per_testcase(default, Config) ->
    ct:timetrap({minutes, 10}),
    Workers = [W | _] = ?config(op_worker_nodes, Config),
    % ensure that all file blocks will be public
    ok = test_utils:set_env(Workers, ?APP_NAME, public_block_size_treshold, 0),
    ok = test_utils:set_env(Workers, ?APP_NAME, public_block_percent_treshold, 0),
    ensure_controller_stopped(W, ?SPACE_ID),
    clean_autocleaning_run_model(W, ?SPACE_ID),
    Config2 = lfm_proxy:init(Config),
    lfm_test_utils:assert_space_and_trash_are_empty(Workers, ?SPACE_ID, ?ATTEMPTS),
    Config2;

init_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_periodical_spaces_autocleaning_check(W),
    init_per_testcase(default, Config).

end_per_testcase(cancel_autocleaning_run, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(W, op_worker, autocleaning_view_batch_size, 1000),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    [W | _] = Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:close_all(W),
    ensure_controller_stopped(W, ?SPACE_ID),
    clean_autocleaning_run_model(W, ?SPACE_ID),
    delete_file_popularity_config(W, ?SPACE_ID),
    delete_auto_cleaning_config(W, ?SPACE_ID),
    lfm_test_utils:clean_space(W, Workers, ?SPACE_ID, ?ATTEMPTS),
    ok = test_utils:set_env(W, op_worker, autocleaning_view_batch_size, 1000),
    ok = test_utils:set_env(W, op_worker, replica_deletion_max_parallel_requests, 1000),
%%    ensure_space_empty(?SPACE_ID, Config),
    lfm_proxy:teardown(Config).

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl).

%%%===================================================================
%%% Test bases
%%%===================================================================

autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_one_rule_test_base(Config, Size, ACConfig) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = ?FILE_NAME,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    ok = configure_autocleaning(W1, ?SPACE_ID, ACConfig),
    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP2], [Size]), Guid),
    schedule_file_replication(W1, SessId, Guid, DomainP1),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertOneOfReports({ok, #{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0,
        status := ?FAILED
    }},  W1, ?SPACE_ID).

%%%===================================================================
%%% Internal functions
%%%===================================================================

disable_periodical_spaces_autocleaning_check(Worker) ->
    test_utils:set_env(Worker, ?APP_NAME, autocleaning_periodical_spaces_check_enabled, false).

enable_periodical_spaces_autocleaning_check(Worker) ->
    test_utils:set_env(Worker, ?APP_NAME, autocleaning_periodical_spaces_check_enabled, true).

write_file(Worker, SessId, FilePath, Size) ->
    {ok, Guid} = lfm_proxy:create(Worker, SessId, FilePath, 8#664),
    {ok, H} = lfm_proxy:open(Worker, SessId, {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(Worker, H, 0, crypto:strong_rand_bytes(Size)),
    ok = lfm_proxy:close(Worker, H),
    Guid.

write_files(Worker, SessId, DirPath, FilePrefix, Size, Num) ->
    lists:map(fun(N) ->
        FilePath = filename:join(DirPath, <<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        write_file(Worker, SessId, FilePath, Size)
    end, lists:seq(1, Num)).

read_file(Worker, SessId, Guid, Size) ->
    ?assertEqual(Size, try
        {ok, H} = lfm_proxy:open(Worker, SessId, {guid, Guid}, read),
        {ok, Data} = lfm_proxy:read(Worker, H, 0, Size),
        ok = lfm_proxy:close(Worker, H),
        byte_size(Data)
    catch
        _:_ ->
            error
    end, ?ATTEMPTS).

schedule_file_replication(Worker, SessId, Guid, ProviderId) ->
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {guid, Guid}), ?ATTEMPTS),
    {ok, _} = lfm_proxy:schedule_file_replication(Worker, SessId, {guid, Guid}, ProviderId).

enable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, enable, [SpaceId]).

delete_file_popularity_config(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, delete_config, [SpaceId]).


clean_autocleaning_run_model(Worker, SpaceId) ->
    clean_autocleaning_run_model(Worker, SpaceId, undefined, 0, 1000).

clean_autocleaning_run_model(Worker, SpaceId, LinkId, Offset, Limit) ->
    {ok, ARIds} = list(Worker, SpaceId, LinkId, Offset, Limit),
    delete_autocleaning_runs(Worker, ARIds, SpaceId),
    case length(ARIds) < Limit of
        true ->
            ok;
        false ->
            clean_autocleaning_run_model(Worker, SpaceId, LinkId, Offset + Limit, Limit)
    end.

delete_autocleaning_runs(Worker, ARIds, SpaceId) ->
    lists:foreach(fun(ARId) ->
        ok = delete(Worker, SpaceId, ARId)
    end, ARIds).

%% autocleaning_api module rpc calls
configure_autocleaning(Worker, SpaceId, Configuration) ->
    rpc:call(Worker, autocleaning_api, configure, [SpaceId, Configuration]).

delete_auto_cleaning_config(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, delete_config, [SpaceId]).

force_start(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, force_run, [SpaceId]).

cancel(Worker, SpaceId, AutocleaningRunId) ->
    rpc:call(Worker, autocleaning_api, cancel_run, [SpaceId, AutocleaningRunId]).

ensure_controller_stopped(Worker, SpaceId) ->
    ?assertEqual(undefined,
        rpc:call(Worker, global, whereis_name, [{autocleaning_run_controller, SpaceId}]), ?ATTEMPTS).

restart_autocleaning_run(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, restart_autocleaning_run, [SpaceId]).

list(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, list_reports, [SpaceId]).

list(Worker, SpaceId, LinkId, Offset, Limit) ->
    rpc:call(Worker, autocleaning_api, list_reports, [SpaceId, LinkId, Offset, Limit]).

get_run_report(Worker, ARId) ->
    rpc:call(Worker, autocleaning_api, get_run_report, [ARId]).

delete(Worker, SpaceId, ARId) ->
    rpc:call(Worker, autocleaning_run, delete, [ARId, SpaceId]).

provider_id(Worker) ->
    rpc:call(Worker, oneprovider, get_id, []).

current_size(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, current_size, [SpaceId]).

change_last_open(Worker, FileGuid, NewLastOpen) ->
    Uuid = file_id:guid_to_uuid(FileGuid),
    rpc:call(Worker, file_popularity, update, [Uuid, fun(FP) ->
        {ok, FP#file_popularity{last_open = NewLastOpen}}
    end]).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).
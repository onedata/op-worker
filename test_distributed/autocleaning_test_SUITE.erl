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
-include("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    autocleaning_run_should_not_start_when_file_popularity_is_disabled/1,
    force_start_of_autocleaning_should_return_error_when_file_popularity_is_disabled/1,
    autocleaning_run_should_not_start_when_autocleaning_is_disabled/1,
    force_start_of_autocleaning_should_return_error_when_autocleaning_is_disabled/1,
    autocleaning_should_not_evict_file_replica_when_it_is_not_replicated/1,
    autocleaning_should_evict_file_replica_when_it_is_replicated/1,
    autocleaning_should_not_evict_file_replica_if_it_has_never_been_opened/1,
    autocleaning_should_evict_file_replicas_until_it_reaches_configured_target/1,
    autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_open_count_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_hours_since_last_open_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_file_size_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_file_size_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_hourly_moving_average_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_daily_moving_average_rule/1,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_monthly_moving_average_rule/1
]).

all() -> [
    autocleaning_run_should_not_start_when_file_popularity_is_disabled,
    force_start_of_autocleaning_should_return_error_when_file_popularity_is_disabled,
    autocleaning_run_should_not_start_when_autocleaning_is_disabled,
    force_start_of_autocleaning_should_return_error_when_autocleaning_is_disabled,
    autocleaning_should_not_evict_file_replica_when_it_is_not_replicated,
    autocleaning_should_evict_file_replica_when_it_is_replicated,
    autocleaning_should_not_evict_file_replica_if_it_has_never_been_opened,
    autocleaning_should_evict_file_replicas_until_it_reaches_configured_target,
    autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_open_count_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_hours_since_last_open_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_min_file_size_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_file_size_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_hourly_moving_average_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_daily_moving_average_rule,
    autocleaning_should_not_evict_file_replica_when_it_does_not_satisfy_max_monthly_moving_average_rule
].

-define(SPACE_ID, <<"space1">>).

-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

-define(USER, <<"user1">>).
-define(SESSION(Worker, Config), ?SESSION(?USER, Worker, Config)).
-define(SESSION(User, Worker, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).

-define(ATTEMPTS, 60).
-define(LIMIT, 10).
-define(MAX_LIMIT, 10000).
-define(MAX_VAL, 1000000000).

-define(RULE_SETTING(Value), ?RULE_SETTING(true, Value)).
-define(RULE_SETTING(Enabled, Value), #{enabled => Enabled, value => Value}).

-define(DIST(ProviderId, Size),
    case Size == 0 of
        true ->
            [#{<<"providerId">> => ProviderId, <<"blocks">> => []}];
        false ->
            [#{<<"providerId">> => ProviderId, <<"blocks">> => [[0, Size]]}]
    end).

-define(DISTS(ProviderIds, Sizes), lists:flatmap(fun({PId, Size}) ->
    ?DIST(PId, Size)
end, lists:zip(ProviderIds, Sizes))).

-define(normalizeDistribution(__Distributions), lists:sort(lists:map(fun(__Distribution) ->
    __Distribution#{
        <<"totalBlocksSize">> => lists:foldl(fun([_Offset, __Size], __SizeAcc) ->
            __SizeAcc + __Size
        end, 0, maps:get(<<"blocks">>, __Distribution))
    }
end, __Distributions))).

-define(assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertEqual(?normalizeDistribution(ExpectedDistribution), begin
        {ok, __FileBlocks} = lfm_proxy:get_file_distribution(Worker, SessionId, {guid, FileGuid}),
        lists:sort(__FileBlocks)
    end, ?ATTEMPTS)
).

-define(assertFilesInView(Worker, SpaceId, ExpectedGuids),
    ?assertMatch([], begin
        StartKey = lists:duplicate(6, ?MAX_VAL),
        EndKey = lists:duplicate(6, 0),
        Token = rpc:call(Worker, file_popularity_api, initial_token, [StartKey, EndKey]),
        {FileCtxs, _} = rpc:call(Worker, file_popularity_api, query, [SpaceId, Token, ?MAX_LIMIT]),
        __Guids = [file_ctx:get_guid_const(F) || F <- FileCtxs],
        ExpectedGuids -- __Guids
    end, ?ATTEMPTS)).

-define(assertRunFinished(Worker, ARId),
    ?assertEqual(true, begin
        Info = get_info(Worker, ARId),
        maps:get(stopped_at, Info) =/= null
    end, ?ATTEMPTS)).

%%%===================================================================
%%% API
%%%===================================================================

autocleaning_run_should_not_start_when_file_popularity_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W, Config),
    FileName = <<"file">>,
    Size = 10,
    test_utils:mock_new(W, autocleaning_api, [passthrough]),
    test_utils:mock_new(W, autocleaning_run, [passthrough]),
    write_file(W, SessId, ?FILE_PATH(FileName), Size),
    test_utils:mock_assert_num_calls(W, autocleaning_api, maybe_start, ['_', '_'], 1, ?ATTEMPTS),
    test_utils:mock_assert_num_calls(W, autocleaning_run, start, ['_', '_', '_'], 0, ?ATTEMPTS).

force_start_of_autocleaning_should_return_error_when_file_popularity_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({error, file_popularity_disabled}, force_start(W, ?SPACE_ID)).

autocleaning_run_should_not_start_when_autocleaning_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W, Config),
    FileName = <<"file">>,
    Size = 10,
    test_utils:mock_new(W, autocleaning_api, [passthrough]),
    test_utils:mock_new(W, autocleaning_run, [passthrough]),
    enable_file_popularity(W, ?SPACE_ID),
    write_file(W, SessId, ?FILE_PATH(FileName), Size),
    test_utils:mock_assert_num_calls(W, autocleaning_api, maybe_start, ['_', '_'], 1, ?ATTEMPTS),
    test_utils:mock_assert_num_calls(W, autocleaning_run, start, ['_', '_', '_'], 0, ?ATTEMPTS).

force_start_of_autocleaning_should_return_error_when_autocleaning_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    enable_file_popularity(W, ?SPACE_ID),
    ?assertEqual({error, autocleaning_disabled}, force_start(W, ?SPACE_ID)).

autocleaning_should_not_evict_file_replica_when_it_is_not_replicated(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    FileName = <<"file">>,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    enable_file_popularity(W1, ?SPACE_ID),

    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),

    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DIST(DomainP1, Size), Guid),
    ?assertMatch(#{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0
    }, get_info(W1, ARId)).

autocleaning_should_evict_file_replica_when_it_is_replicated(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = <<"file">>,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),
    % read file on W2 to replicate it
    read_file(W2, SessId2, Guid, Size),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertFilesInView(W1, ?SPACE_ID, [Guid]),
    ?assertEqual(Size, rpc:call(W1, space_quota, current_size, [?SPACE_ID]), ?ATTEMPTS),
    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ok = force_start(W1, ?SPACE_ID),

    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertMatch(#{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1
    }, get_info(W1, ARId)).

autocleaning_should_not_evict_file_replica_if_it_has_never_been_opened(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = <<"file">>,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    ProviderId1 = provider_id(W1),
    enable_file_popularity(W1, ?SPACE_ID),

    Guid = write_file(W2, SessId2, ?FILE_PATH(FileName), Size),
    schedule_file_replication(W1, SessId, Guid, ProviderId1),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),

    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => 0,
        threshold => Size - 1
    }),
    ok = force_start(W1, ?SPACE_ID),

    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),

    ?assertMatch(#{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0
    }, get_info(W1, ARId)).

autocleaning_should_evict_file_replicas_until_it_reaches_configured_target(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),

    Target = 1000,
    Threshold = 10001,

    DirName = <<"dir">>,
    DirPath = ?FILE_PATH(DirName),
    {ok, _} = lfm_proxy:mkdir(W2, SessId2, DirPath),

    FilesNum = 100,
    FileSize = 100,
    FilePrefix = <<"file_">>,
    Guids = write_files(W2, SessId2, DirPath, FilePrefix, FileSize, FilesNum),

    ExtraFile = <<"extra_file">>,
    ExtraFileSize = 1,
    EG = write_file(W2, SessId2, ?FILE_PATH(ExtraFile), ExtraFileSize),

    lists:foreach(fun(G) ->
        ?assertDistribution(W1, SessId, ?DIST(DomainP2, FileSize), G),
        schedule_file_replication(W1, SessId, G, DomainP1)
    end, Guids),


    lists:foreach(fun(G) ->
        ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [FileSize, FileSize]), G)
    end, Guids),

    enable_file_popularity(W1, ?SPACE_ID),
    configure_autocleaning(W1, ?SPACE_ID, #{
        enabled => true,
        target => Target,
        threshold => Threshold,
        rules => #{enabled => false}
    }),

    % open each file so that it will be visible in the view
    lists:foreach(fun(G) ->
        read_file(W1, SessId, G, FileSize)
    end, Guids),
    ?assertFilesInView(W1, ?SPACE_ID, Guids),

    % this sleep is necessary to ensure that autocleaning_check will be
    % started after replication of an ExtraFile
    timer:sleep(timer:seconds(1)),
    read_file(W1, SessId, EG, ExtraFileSize),

    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertEqual(true, current_size(W1, ?SPACE_ID) =< Target, ?ATTEMPTS).

autocleaning_should_evict_file_replica_when_it_satisfies_all_enabled_rules(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(W1, Config),
    SessId2 = ?SESSION(W2, Config),
    FileName = <<"file">>,
    Size = 10,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),

    % read file on W2 to replicate it
    read_file(W2, SessId2, Guid, Size),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertFilesInView(W1, ?SPACE_ID, [Guid]),
    ?assertEqual(Size, rpc:call(W1, space_quota, current_size, [?SPACE_ID]), ?ATTEMPTS),
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
    ok = force_start(W1, ?SPACE_ID),

    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [0, Size]), Guid),
    ?assertMatch(#{
        released_bytes := Size,
        bytes_to_release := Size,
        files_number := 1
    }, get_info(W1, ARId)).

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
            min_hours_since_last_open => ?RULE_SETTING(1),
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

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    clean_autocleaning_run_model(W, ?SPACE_ID),
    disable_file_popularity(W, ?SPACE_ID),
    disable_autocleaning(W, ?SPACE_ID),
    clean_space(?SPACE_ID, Config),
    reset_autocleaning_check_timestamp(W, ?SPACE_ID),
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
    FileName = <<"file">>,
    DomainP1 = ?GET_DOMAIN_BIN(W1),
    DomainP2 = ?GET_DOMAIN_BIN(W2),
    enable_file_popularity(W1, ?SPACE_ID),
    Guid = write_file(W1, SessId, ?FILE_PATH(FileName), Size),

    % read file on W2 to replicate it
    read_file(W2, SessId2, Guid, Size),

    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertFilesInView(W1, ?SPACE_ID, [Guid]),
    ?assertEqual(Size, rpc:call(W1, space_quota, current_size, [?SPACE_ID]), ?ATTEMPTS),
    ok = configure_autocleaning(W1, ?SPACE_ID, ACConfig),
    ok = force_start(W1, ?SPACE_ID),

    {ok, [ARId]} = ?assertMatch({ok, [_]}, list(W1, ?SPACE_ID), ?ATTEMPTS),
    ?assertRunFinished(W1, ARId),
    ?assertDistribution(W1, SessId, ?DISTS([DomainP1, DomainP2], [Size, Size]), Guid),
    ?assertMatch(#{
        released_bytes := 0,
        bytes_to_release := Size,
        files_number := 0
    }, get_info(W1, ARId)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    ?assertEqual(Size, begin
        try
            {ok, H} = lfm_proxy:open(Worker, SessId, {guid, Guid}, read),
            {ok, Data} = lfm_proxy:read(Worker, H, 0, Size),
            ok = lfm_proxy:close(Worker, H),
            byte_size(Data)
        catch
            _:_ ->
                error
        end
    end,
        ?ATTEMPTS).

schedule_file_replication(Worker, SessId, Guid, ProviderId) ->
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {guid, Guid}), ?ATTEMPTS),
    {ok, _} = lfm_proxy:schedule_file_replication(Worker, SessId, {guid, Guid}, ProviderId).

enable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, enable, [SpaceId]).

disable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, disable, [SpaceId]).

clean_space(SpaceId, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(Worker, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    clean_space(Worker, SessId, SpaceGuid, 0, BatchSize).

clean_space(Worker, SessId, SpaceGuid, Offset, BatchSize) ->
    {ok, GuidsAndPaths} = lfm_proxy:ls(Worker, SessId, {guid, SpaceGuid}, Offset, BatchSize),
    FilesNum = length(GuidsAndPaths),
    delete_files(Worker, SessId, GuidsAndPaths),
    case FilesNum < BatchSize of
        true ->
            ok;
        false ->
            clean_space(Worker, SessId, SpaceGuid, Offset + BatchSize, BatchSize)
    end.

delete_files(Worker, SessId, GuidsAndPaths) ->
    lists:foreach(fun({G, P}) ->
        ok = lfm_proxy:rm_recursive(Worker, SessId, {guid, G})
    end, GuidsAndPaths).

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

enable_autocleaning(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, configure, [SpaceId, #{enabled => true}]).

disable_autocleaning(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, disable, [SpaceId]).

force_start(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, force_start, [SpaceId]).

list(Worker, SpaceId) ->
    rpc:call(Worker, autocleaning_api, list, [SpaceId]).

list(Worker, SpaceId, LinkId, Offset, Limit) ->
    rpc:call(Worker, autocleaning_api, list, [SpaceId, LinkId, Offset, Limit]).

%% autocleaning_run module rpc calls
get_info(Worker, ARId) ->
    rpc:call(Worker, autocleaning_run, get_info, [ARId]).

delete(Worker, SpaceId, ARId) ->
    rpc:call(Worker, autocleaning_run, delete, [ARId, SpaceId]).

reset_autocleaning_check_timestamp(Worker, SpaceId) ->
    ok = rpc:call(Worker, space_quota, update_last_check_timestamp, [SpaceId, 0]).

provider_id(Worker) ->
    rpc:call(Worker, oneprovider, get_id, []).

current_size(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, current_size, [SpaceId]).
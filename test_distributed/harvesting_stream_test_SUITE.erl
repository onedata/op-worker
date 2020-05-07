%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests harvesting_worker supervision tree.
%%% @end
%%%--------------------------------------------------------------------
-module(harvesting_stream_test_SUITE).
-author("Jakub Kudzia").

-include("logic_tests_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-include("modules/harvesting/harvesting.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").


%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    stream_sup_should_be_started/1,
    stream_supervisor_should_be_restarted/1,
    main_harvesting_stream_should_not_be_started_if_space_is_not_supported/1,
    main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_harvesters/1,
    main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_indices/1,
    only_one_main_harvesting_stream_should_be_started_for_supported_space/1,
    only_one_main_harvesting_stream_should_be_started_for_supported_space_one_harvester_many_indices/1,
    only_one_main_harvesting_stream_should_be_started_for_supported_space_many_harvesters_many_indices/1,
    main_harvesting_stream_should_be_started_for_each_supported_space_with_a_harvester/1,
    adding_space_to_harvester_should_start_new_main_harvesting_stream/1,
    main_harvesting_stream_should_be_stopped_when_harvester_is_deleted/1,
    main_harvesting_stream_should_be_stopped_when_there_are_no_indices_in_the_harvester/1,
    main_harvesting_stream_should_be_stopped_when_space_is_deleted_from_harvester/1,
    start_stop_streams_mixed_test/1,
    main_stream_should_persist_last_successfully_processed_seq/1,
    adding_index_should_start_aux_stream_to_catch_up_with_main_stream/1,
    aux_stream_should_be_started_test/1,
    aux_stream_should_not_be_started_test/1,
    aux_stream_should_eventually_catch_up_with_main_stream/1,
    adding_harvester_should_start_aux_stream_to_catch_up_with_main_stream/1,
    aux_stream_should_be_started_on_index_level_error/1,
    aux_stream_should_be_started_on_harvester_level_error/1,
    backoff_should_be_used_on_space_level_error/1,
    error_mix_test/1,
    error_mix_test2/1,
    error_mix_test3/1,
    error_mix_test4/1,
    harvesting_stream_flush_test/1,
    harvesting_stream_batch_test/1,
    only_one_change_per_fileid_should_be_harvested_in_one_batch_test/1
]).

all() -> ?ALL([
    stream_sup_should_be_started,
    stream_supervisor_should_be_restarted,
    main_harvesting_stream_should_not_be_started_if_space_is_not_supported,
    main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_harvesters,
    main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_indices,
    only_one_main_harvesting_stream_should_be_started_for_supported_space,
    only_one_main_harvesting_stream_should_be_started_for_supported_space_one_harvester_many_indices,
    only_one_main_harvesting_stream_should_be_started_for_supported_space_many_harvesters_many_indices,
    main_harvesting_stream_should_be_started_for_each_supported_space_with_a_harvester,
    adding_space_to_harvester_should_start_new_main_harvesting_stream,
    main_harvesting_stream_should_be_stopped_when_harvester_is_deleted,
    main_harvesting_stream_should_be_stopped_when_there_are_no_indices_in_the_harvester,
    main_harvesting_stream_should_be_stopped_when_space_is_deleted_from_harvester,
    start_stop_streams_mixed_test,
    main_stream_should_persist_last_successfully_processed_seq,
    adding_index_should_start_aux_stream_to_catch_up_with_main_stream,
    aux_stream_should_be_started_test,
    aux_stream_should_not_be_started_test,
    aux_stream_should_eventually_catch_up_with_main_stream,
    adding_harvester_should_start_aux_stream_to_catch_up_with_main_stream,
    aux_stream_should_be_started_on_index_level_error,
    aux_stream_should_be_started_on_harvester_level_error,
    backoff_should_be_used_on_space_level_error,
    error_mix_test,
    error_mix_test2,
    error_mix_test3,
    error_mix_test4,
    harvesting_stream_flush_test,
    harvesting_stream_batch_test,
    only_one_change_per_fileid_should_be_harvested_in_one_batch_test
]).

-define(OD_SPACE_POSTHOOK_EXECUTED(SpaceId),
    {od_space_posthook_executed, SpaceId}).

-define(HARVEST_METADATA_CALLED(SpaceId, Destination, Batch, HarvestingStreamPid),
    {harvest_metadata_called, SpaceId, Destination, Batch, HarvestingStreamPid}
).

-define(SPACE_ID(N), ?ID(<<"space_">>, N)).
-define(INDEX_ID(N), ?ID(<<"index_">>, N)).
-define(HARVESTER_ID(N), ?ID(<<"harvester_">>, N)).
-define(ID(Prefix, N), <<Prefix/binary, (integer_to_binary(N))/binary>>).
-define(PROVIDER_ID(__Node), rpc:call(__Node, oneprovider, get_id, [])).

-define(ATTEMPTS, 60).

-define(assertHarvestMetadataCalled(ExpSpaceId, ExpDestination, ExpSeqs, ExpHarvestingStreamPid),
    ?assertHarvestMetadataCalled(ExpSpaceId, ExpDestination, ExpSeqs, ExpHarvestingStreamPid, ?ATTEMPTS)).
-define(assertHarvestMetadataCalled(ExpSpaceId, ExpDestination, ExpSeqs,
    ExpHarvestingStreamPid, Timeout
), (
    (fun
        AssertFun(__SpaceId, __Destination, [], __HarvestingStreamPid, __Timeout) ->
            % all expected changes has been received
            ok;
        AssertFun(__SpaceId, __Destination, __Seqs, __HarvestingStreamPid, __Timeout) ->
            __TimeoutInMillis = timer:seconds(__Timeout),
            receive
                ?HARVEST_METADATA_CALLED(
                    __SpaceId,
                    __Destination,
                    __ReceivedBatch,
                    __HarvestingStreamPid
                ) ->
                    __ReceivedSeqs = [__Seq || #{<<"seq">> := __Seq} <- __ReceivedBatch],
                    AssertFun(__SpaceId, __Destination, sequential_subtract(__Seqs, __ReceivedSeqs),
                        __HarvestingStreamPid, __Timeout)
            after
                __TimeoutInMillis ->
                    __Args = [{module, ?MODULE},
                        {line, ?LINE},
                        {expected, {__SpaceId, __Destination, __Seqs, __HarvestingStreamPid, __Timeout}},
                        {value, timeout}],
                    ct:print("assertHarvestMetadataCalled_failed: ~lp~n", [__Args]),
                    erlang:error({assertHarvestMetadataCalled_failed, __Args})
            end
    end)(ExpSpaceId, ExpDestination, ExpSeqs, ExpHarvestingStreamPid, Timeout)
)).

-define(assertHarvestMetadataNotCalled(ExpSpaceId, ExpDestination, ExpSeqs, ExpHarvestingStreamPid),
    ?assertHarvestMetadataNotCalled(ExpSpaceId, ExpDestination, ExpSeqs, ExpHarvestingStreamPid, ?ATTEMPTS)).
-define(assertHarvestMetadataNotCalled(ExpSpaceId, ExpDestination, ExpSeqs,
    ExpHarvestingStreamPid, Timeout
), (
    (fun AssertFun(__SpaceId, __Destination, __Seqs, __HarvestingStreamPid, __Timeout) ->
        Start = time_utils:system_time_seconds(),
        __TimeoutInMillis = timer:seconds(__Timeout),
        receive
            __HM = ?HARVEST_METADATA_CALLED(
                __SpaceId,
                __Destination,
                __ReceivedBatch,
                __HarvestingStreamPid
            ) ->
                ElapsedTime = time_utils:system_time_seconds() - Start,
                __ReceivedSeqs = [__Seq || #{<<"seq">> := __Seq} <- __ReceivedBatch],
                case sequential_subtract(__Seqs, __ReceivedSeqs) of
                    __Seqs ->
                        AssertFun(__SpaceId, __Destination, __Seqs, __HarvestingStreamPid, max(__Timeout - ElapsedTime, 0));
                    _ ->
                        __Args = [
                            {module, ?MODULE},
                            {line, ?LINE}
                        ],
                        ct:print("assertHarvestMetadataNotCalled_failed: ~lp~n"
                            "Unexpectedly received: ~p~n", [__Args, __HM]),
                        erlang:error({assertHarvestMetadataNotCalled_failed, __Args})
                end
        after
            __TimeoutInMillis ->
                ok
        end
    end)(ExpSpaceId, ExpDestination, ExpSeqs, ExpHarvestingStreamPid, Timeout)
)).

%%%===================================================================
%%% Test functions
%%%===================================================================

stream_sup_should_be_started(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) ->
        stream_sup_should_be_started(Config, Node) end, Nodes).

stream_supervisor_should_be_restarted(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) ->
        stream_supervisor_should_be_restarted(Config, Node) end, Nodes).

main_harvesting_stream_should_not_be_started_if_space_is_not_supported(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Node = lists_utils:random_element(Nodes),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    [Space] = maps:keys(SpacesConfig),
    mock_harvester_logic_get(Nodes, HarvestersConfig),
    Harvesters = maps:keys(HarvestersConfig),
    {ok, DS = #document{value = ODS}} = get_space_doc(Node, Space),
    % trigger od_space posthooks
    put_into_cache(Node, DS#document{value = ODS#od_space{harvesters = Harvesters}}),
    wait_until_od_space_posthook_is_executed(Space),
    ?assertMatch(0, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_harvesters(Config) ->
    main_stream_test_base(Config, #{}, #{?SPACE_ID(1) => []}).

main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_indices(Config) ->
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 0, spaces => 1}
    }),
    main_stream_test_base(Config, HarvestersConfig, SpacesConfig).

only_one_main_harvesting_stream_should_be_started_for_supported_space(Config) ->
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    main_stream_test_base(Config, HarvestersConfig, SpacesConfig).

only_one_main_harvesting_stream_should_be_started_for_supported_space_one_harvester_many_indices(Config) ->
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 10, spaces => 1}
    }),
    main_stream_test_base(Config, HarvestersConfig, SpacesConfig).

only_one_main_harvesting_stream_should_be_started_for_supported_space_many_harvesters_many_indices(Config) ->
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 10, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 100, spaces => 1}
    }),
    main_stream_test_base(Config, HarvestersConfig, SpacesConfig).

main_harvesting_stream_should_be_started_for_each_supported_space_with_a_harvester(Config) ->
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    main_stream_test_base(Config, HarvestersConfig, SpacesConfig).

adding_space_to_harvester_should_start_new_main_harvesting_stream(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    {HarvestersConfig2, SpacesConfig2} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 2}
    }),
    update_harvesters_structure(Config, HarvestersConfig2, SpacesConfig2),
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

main_harvesting_stream_should_be_stopped_when_harvester_is_deleted(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    update_harvesters_structure(Config, #{}, #{?SPACE_ID(1) => []}),
    ?assertMatch(0, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

main_harvesting_stream_should_be_stopped_when_there_are_no_indices_in_the_harvester(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    {HarvestersConfig2, SpacesConfig2} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 0, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig2, SpacesConfig2),
    ?assertMatch(0, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

main_harvesting_stream_should_be_stopped_when_space_is_deleted_from_harvester(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    {HarvestersConfig2, _} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 0}
    }),
    update_harvesters_structure(Config, HarvestersConfig2, #{?SPACE_ID(1) => []}),
    ?assertMatch(0, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

start_stop_streams_mixed_test(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 10}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(10, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    {HarvestersConfig2, SpacesConfig2} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 25}
    }),

    update_harvesters_structure(Config, HarvestersConfig2, SpacesConfig2),
    ?assertMatch(25, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    {HarvestersConfig3, SpacesConfig3} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(2) => #{indices => 3, spaces => 5}
    }),

    update_harvesters_structure(Config, HarvestersConfig3, SpacesConfig3),
    DeletedSpaces = maps:keys(SpacesConfig2) -- maps:keys(SpacesConfig3),

    lists:foreach(fun(SpaceId) ->
        pretend_space_deletion(lists_utils:random_element(Nodes), SpaceId)
    end, DeletedSpaces),

    ?assertMatch(5, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    update_harvesters_structure(Config, #{}, #{}),
    lists:foreach(fun(SpaceId) ->
        pretend_space_deletion(lists_utils:random_element(Nodes), SpaceId)
    end, maps:keys(SpacesConfig3)),
    ?assertMatch(0, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

main_stream_should_persist_last_successfully_processed_seq(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    mock_harvest_metadata_success(Nodes),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 100, 64, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),
    Max = get_max_seq(Changes),
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid),

    % check whether maximal Seq from Changes list was persisted as processed by harvesting stream
    ?assertMatch({ok, Max}, harvesting_state_get_seen_seq(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(1)), ?ATTEMPTS).

adding_index_should_start_aux_stream_to_catch_up_with_main_stream(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    mock_harvest_metadata_success(Nodes),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 61, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 79, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),
    RelevantSeqs2 = relevant_seqs(Changes2, false),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid),

    % add index to harvester
    {HarvestersConfig2, SpacesConfig2} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig2, SpacesConfig2),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(2)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),
    % assert that aux_stream has harvested missing metadata
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        RelevantSeqs, AuxStreamPid
    ),
    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    %Changes2 should be streamed to both indices
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        RelevantSeqs2, MainStreamPid
    ).

aux_stream_should_be_started_test(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Node = lists_utils:random_element(Nodes),
    ok = rpc:call(Node, harvesting_state, ensure_created, [?SPACE_ID(1)]),
    Dest = harvesting_destination:init(?HARVESTER_ID(1), ?INDEX_ID(1)),
    ok = rpc:call(Node, harvesting_state, set_seen_seq, [?SPACE_ID(1), Dest, 1000000]),

    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1}
    }),
    % aux_stream should be started as 2 index has never been harvested
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

aux_stream_should_not_be_started_test(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Node = lists_utils:random_element(Nodes),
    ok = rpc:call(Node, harvesting_state, ensure_created, [?SPACE_ID(1)]),
    Dest = harvesting_destination:init(?HARVESTER_ID(1), [?INDEX_ID(1), ?INDEX_ID(2)]),
    ok = rpc:call(Node, harvesting_state, set_seen_seq, [?SPACE_ID(1), Dest, 1000000]),

    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    % only main_stream should be started as 2 index is not in the harvester
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

aux_stream_should_eventually_catch_up_with_main_stream(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    mock_harvest_metadata_success(Nodes),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % generate changes
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 67, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    % stream Changes1 from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid),

    % add index to harvester
    {HarvestersConfig2, SpacesConfig2} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig2, SpacesConfig2),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(2)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid
    ),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        RelevantSeqs, AuxStreamPid
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes2),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        RelevantSeqs2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        RelevantSeqs3, MainStreamPid
    ).

adding_harvester_should_start_aux_stream_to_catch_up_with_main_stream(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    mock_harvest_metadata_success(Nodes),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),
    RelevantSeqs2 = relevant_seqs(Changes2, false),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid
    ),

    % add harvester to space
    {HarvestersConfig2, SpacesConfig2} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig2, SpacesConfig2),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    % assert that aux_stream has harvested missing metadata

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RelevantSeqs, AuxStreamPid
    ),
    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),

    %Changes2 should be streamed to both indices
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs2, MainStreamPid).


aux_stream_should_be_started_on_index_level_error(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1}
    }),

    % generate changes
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantChanges1 = relevant_changes(Changes, true),
    RelevantSeqs1 = get_seqs(RelevantChanges1),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    % choose random seq on which harvesting will fail
    FailedSeq = random_custom_metadata_seq(Changes),
    RetriedChanges = strip_after(RelevantChanges1, FailedSeq),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        case in_destination(?HARVESTER_ID(1), ?INDEX_ID(2), Destination) of
            true -> {ok, #{?HARVESTER_ID(1) => #{?INDEX_ID(2) => FailedSeq}}};
            false -> {ok, #{}}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        RelevantSeqs1, MainStreamPid
    ),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(2)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    mock_harvest_metadata_success(Nodes), %"fix" Index2

    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        get_seqs(RetriedChanges), AuxStreamPid
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes2),

    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        RelevantSeqs2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        RelevantSeqs3, MainStreamPid
    ).

aux_stream_should_be_started_on_harvester_level_error(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        case in_destination(?HARVESTER_ID(2), Destination) of
            true -> {ok, #{?HARVESTER_ID(2) => {error, test_error}}};
            false -> {ok, #{}}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % generate changes
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 67, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs, MainStreamPid),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    mock_harvest_metadata_success(Nodes), %"fix" Index2

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RelevantSeqs, AuxStreamPid
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RelevantSeqs2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs3, MainStreamPid).

backoff_should_be_used_on_space_level_error(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 2, spaces => 1}
    }),

    % generate changes
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 68, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),

    RelevantSeqs1 = relevant_seqs(Changes, true),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    mock_harvest_metadata(Nodes, fun(_SpaceId, _Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        {error, test_error}
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1), ?INDEX_ID(2)]
    },
        RelevantSeqs1, MainStreamPid
    ),

    % aux_stream_should not be started
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, these changes shouldn't be harvested
    % until previous batch is sent
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataNotCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1), ?INDEX_ID(2)]
    },
        RelevantSeqs2, MainStreamPid
    ),

    %"fix" harvesting
    mock_harvest_metadata_success(Nodes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1), ?INDEX_ID(2)]
    },
        RelevantSeqs1 ++ RelevantSeqs2, MainStreamPid
    , 120),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1), ?INDEX_ID(2)]
    },
        RelevantSeqs3, MainStreamPid
    , 120).

error_mix_test(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantChanges1 = relevant_changes(Changes, true),
    RelevantSeqs1 = get_seqs(RelevantChanges1),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    % choose random seq on which harvesting will fail
    FailedSeq = random_custom_metadata_seq(Changes),
    RetriedChanges = strip_after(RelevantChanges1, FailedSeq),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        InDest11 = in_destination(?HARVESTER_ID(1), ?INDEX_ID(1), Destination),
        InDest12 = in_destination(?HARVESTER_ID(1), ?INDEX_ID(2), Destination),
        InDest2 = in_destination(?HARVESTER_ID(2), Destination),
        case {InDest11, InDest12, InDest2} of
            {true, true, true} ->
                {ok, #{
                    ?HARVESTER_ID(1) => #{?INDEX_ID(2) => FailedSeq},
                    ?HARVESTER_ID(2) => {error, test_error}
                }};
            {true, false, false} ->
                {ok, #{}};
            {false, true, false} ->
                {ok, #{?HARVESTER_ID(1) => #{?INDEX_ID(2) => FailedSeq}}};
            {false, false, true} ->
                {error, test_error}
        end
    end),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs1, MainStreamPid),

    % aux_streams_should be started to catch up with main stream
    ?assertMatch(3, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(2)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),

    AuxStreamPid2 = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid2),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes),

    %"fix" aux_streams
    mock_harvest_metadata_success(Nodes),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        get_seqs(RetriedChanges), AuxStreamPid
    ),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RelevantSeqs1, AuxStreamPid2
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(3, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid12 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid12, Changes2),

    AuxChangesStreamPid22 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid2),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid22, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        RelevantSeqs2, AuxStreamPid
    ),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RelevantSeqs2, AuxStreamPid2
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs3, MainStreamPid).

error_mix_test2(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantChanges1 = relevant_changes(Changes, true),
    RelevantSeqs1 = get_seqs(RelevantChanges1),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    % choose random seq on which harvesting will fail
    % ensure that it is not first custom_metadata seq in the batch as it is handled
    % another way and it is checked in another test
    [_ | RestRelevantChanges] = relevant_changes(Changes, true),
    FailedSeq = random_custom_metadata_seq(RestRelevantChanges),
    RetriedSeqsMain = get_seqs(strip_after(RelevantChanges1, FailedSeq)),
    RetriedSeqsAux = get_seqs(strip_before(RelevantChanges1, FailedSeq)),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        InDest11 = in_destination(?HARVESTER_ID(1), ?INDEX_ID(1), Destination),
        InDest2 = in_destination(?HARVESTER_ID(2), Destination),
        case {InDest11, InDest2} of
            {true, true} ->
                {ok, #{
                    ?HARVESTER_ID(1) => #{?INDEX_ID(1) => FailedSeq},
                    ?HARVESTER_ID(2) => {error, test_error}
                }};
            {true, false} ->
                {ok, #{}};
            {false, true} ->
                {error, test_error}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs1, MainStreamPid),

    % aux_stream_should be started to catch up with main stream
    % main_stream will retry starting from FailedSeq
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RetriedSeqsMain, MainStreamPid
    ),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),

    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    %"fix" aux_stream
    mock_harvest_metadata_success(Nodes),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RetriedSeqsAux ++ RetriedSeqsMain ++ RelevantSeqs2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs3, MainStreamPid).

error_mix_test3(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantSeqs1 = relevant_seqs(Changes, true),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        InDest1 = in_destination(?HARVESTER_ID(1), Destination),
        InDest2 = in_destination(?HARVESTER_ID(2), Destination),
        case {InDest1, InDest2} of
            {true, true} ->
                {ok, #{
                    ?HARVESTER_ID(1) => {error, test_error},
                    ?HARVESTER_ID(2) => {error, test_error}
                }};
            {true, false} ->
                {ok, #{?HARVESTER_ID(1) => {error, test_error}}};
            {false, true} ->
                {ok, #{?HARVESTER_ID(2) => {error, test_error}}}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs1, MainStreamPid),

    % aux_streams should not be started
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),

    %"fix" aux_stream
    mock_harvest_metadata_success(Nodes),

    % aux_streams should not be started
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs2 ++ RelevantSeqs3, MainStreamPid).

error_mix_test4(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5, 0.3, ?PROVIDER_ID(N)),
    RelevantChanges1 = relevant_changes(Changes, true),
    RelevantSeqs1 = get_seqs(RelevantChanges1),
    RelevantSeqs2 = relevant_seqs(Changes2, false),
    RelevantSeqs3 = relevant_seqs(Changes3, false),

    % choose random seq on which harvesting will fail
    [FailedSeq, FailedSeq2] = random_custom_metadata_seqs(Changes, 2),
    RetriedSeqsAux = get_seqs(strip_after(RelevantChanges1, FailedSeq)),
    RetriedSeqsMain = get_seqs(strip_after(RelevantChanges1, FailedSeq2)),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        InDest11 = in_destination(?HARVESTER_ID(1), ?INDEX_ID(1), Destination),
        InDest2 = in_destination(?HARVESTER_ID(2), Destination),
        case {InDest11, InDest2} of
            {true, true} ->
                {ok, #{
                    ?HARVESTER_ID(1) => #{?INDEX_ID(1) => FailedSeq},
                    ?HARVESTER_ID(2) => #{?INDEX_ID(1) => FailedSeq2}
                }};
            {true, false} ->
                {ok, #{?HARVESTER_ID(1) => #{?INDEX_ID(1) => FailedSeq}}};
            {false, true} ->
                {ok, #{?HARVESTER_ID(2) => #{?INDEX_ID(1) => FailedSeq2}}}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs1, MainStreamPid),

    % aux_stream_should be started to catch up with main stream
    % main_stream will retry starting from FailedSeq
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RetriedSeqsMain, MainStreamPid
    ),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        InDest2 = in_destination(?HARVESTER_ID(2), Destination),
        case InDest2 of
            true ->
                {ok, #{}};
            false ->
                {ok, #{?HARVESTER_ID(1) => #{?INDEX_ID(1) => FailedSeq}}}
        end
    end),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(1)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    %"fix" aux_stream
    mock_harvest_metadata_success(Nodes),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RetriedSeqsAux ++ RelevantSeqs2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, RelevantSeqs3, MainStreamPid).

harvesting_stream_flush_test(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    mock_harvest_metadata_success(Nodes),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 2, 1, 1, 0, 0, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),

    FlushTimeout = 10,
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid, FlushTimeout).

harvesting_stream_batch_test(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    mock_harvest_metadata_success(Nodes),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    BatchSize = 1000,

    Changes = couchbase_changes_stream_mock:generate_changes(1, BatchSize, BatchSize - 1, 1, 0, 0, ?PROVIDER_ID(N)),
    RelevantSeqs = relevant_seqs(Changes, true),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataNotCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid),

    Changes2 = couchbase_changes_stream_mock:generate_changes(BatchSize, BatchSize + 1, 1, 1, 0, 0, ?PROVIDER_ID(N)),
    RelevantSeqs2 = relevant_seqs(Changes ++ Changes2, true),

    ?assertHarvestMetadataNotCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs2, MainStreamPid).

only_one_change_per_fileid_should_be_harvested_in_one_batch_test(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    mock_harvest_metadata_success(Nodes),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 1001, 1000, 1, 0, 0, ?PROVIDER_ID(N), <<"fileid">>),
    RelevantSeqs = [lists:last(get_seqs(Changes))],

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId, #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        RelevantSeqs, MainStreamPid).


%%%===================================================================
%%% Test bases functions
%%%===================================================================

stream_sup_should_be_started(_Config, Node) ->
    ?assertNotEqual(undefined, whereis(Node, harvesting_stream_sup)),
    ?assertMatch([
        {specs, 1},
        {active, 1},
        {supervisors, 1},
        {workers, 0}
    ], rpc:call(Node, supervisor, count_children, [harvesting_worker_sup])).

stream_supervisor_should_be_restarted(_Config, Node) ->
    SupervisorPid = whereis(Node, harvesting_stream_sup),
    exit(SupervisorPid, kill),
    ?assertMatch([
        {specs, 1},
        {active, 1},
        {supervisors, 1},
        {workers, 0}
    ], rpc:call(Node, supervisor, count_children, [harvesting_worker_sup]), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvesting_stream_sup), ?ATTEMPTS),
    ?assertNotEqual(SupervisorPid, whereis(Node, harvesting_stream_sup), ?ATTEMPTS).

main_stream_test_base(Config, HarvestersConfig, SpacesConfig) ->
    Nodes = ?config(op_worker_nodes, Config),
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    MainHarvestingStreamNum = count_main_harvesting_streams(SpacesConfig, HarvestersConfig),
    ?assertMatch(MainHarvestingStreamNum, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, couchbase_changes_stream_mock,
        couchbase_changes_stream_mock_registry]} | Config].

init_per_testcase(harvesting_stream_batch_test, Config) ->
    ct:timetrap({minutes, 10}),
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(Nodes, op_worker, harvesting_flush_timeout_seconds, 3600),
    init_per_testcase(default, Config);

init_per_testcase(_, Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    couchbase_changes_stream_mock_registry_start_link(N),
    ok = mock_harvesting_stream_changes_stream_start_link(Nodes),
    ok = mock_changes_stream(Nodes),
    add_mocked_od_space_synchronization_posthook(Nodes),
    ?assertEqual(0, count_active_children(Nodes, harvesting_stream_sup)),
    initializer:communicator_mock(Nodes),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).

end_per_testcase(harvesting_stream_batch_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(Nodes, op_worker, harvesting_flush_timeout_seconds, 10),
    end_per_testcase(default, Config);

end_per_testcase(_, Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Nodes, harvester_logic),
    ok = test_utils:mock_unload(Nodes, space_logic),
    lists:foreach(fun(Node) ->
        Children = rpc:call(Node, supervisor, which_children, [harvesting_stream_sup]),
        lists:foreach(fun({_Id, Child, _Type, _Modules}) ->
            gen_server2:call(Child, ?TERMINATE, infinity)
        end, Children),
        % TODO - dlaczego terminate_child nie zamyka ladnie stream'ow
        ok = rpc:call(Node, supervisor, terminate_child, [harvesting_worker_sup, harvesting_stream_sup]),
        {ok, _} = rpc:call(Node, supervisor, restart_child, [harvesting_worker_sup, harvesting_stream_sup])
    end, Nodes),
    ?assertMatch(0, catch count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    ok = test_utils:mock_unload(Nodes, harvesting_stream),
    ok = test_utils:mock_unload(Nodes, couchbase_changes_stream),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    couchbase_changes_stream_mock_registry_stop(N),
    delete_harvesting_docs(N),
    test_utils:mock_validate_and_unload(Nodes, [communicator]).

end_per_suite(_Config) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

update_harvesters_structure(Config, HarvestersConfig, SpacesConfig) ->
    Nodes = ?config(op_worker_nodes, Config),
    Node = lists_utils:random_element(Nodes),
    Spaces = maps:keys(SpacesConfig),
    mock_harvester_logic_get(Nodes, HarvestersConfig),
    mock_space_logic_get_harvesters(Nodes, SpacesConfig),
    mock_provider_logic_supports_space(Nodes, Spaces),

    maps:fold(fun(SpaceId, Harvesters, _) ->
        {ok, DS = #document{value = ODS}} = get_space_doc(Node, SpaceId),
        % trigger od_space posthooks
        put_into_cache(Node, DS#document{value = ODS#od_space{harvesters = Harvesters}})
    end, undefined, SpacesConfig),

    maps:fold(fun(_HarvesterId, HarvesterDoc, _) ->
        % trigger od_harvester posthooks
        put_into_cache(Node, HarvesterDoc)
    end, undefined, HarvestersConfig).

count_main_harvesting_streams(SpacesConfig, HarvestersConfig) ->
    maps:fold(fun(_SpaceId, Harvesters, AccIn) ->
        HastAtLeastOneIndex = lists:foldl(fun
            (HarvesterId, false) ->
                #document{value = Harvester} = maps:get(HarvesterId, HarvestersConfig),
                Harvester#od_harvester.indices =/= [];
            (_HarvesterId, true) ->
                true
        end, false, Harvesters),
        case HastAtLeastOneIndex of
            true -> AccIn + 1;
            false -> AccIn
        end
    end, 0, SpacesConfig).


harvesters_and_spaces_config(HarvestersDescription) ->
    {HC, SC} = maps:fold(fun(HarvesterId, HarvesterConfig, {HarvestersConfigIn, SpacesConfigIn}) ->

        {Spaces, SpacesConfig2} = lists:foldl(fun(I, {SpacesIn, SpacesConfigIn2}) ->
            SpaceId = ?SPACE_ID(I),
            {[SpaceId | SpacesIn], maps:update_with(SpaceId, fun(Harvesters) ->
                [HarvesterId | Harvesters]
            end, [HarvesterId], SpacesConfigIn2)}
        end, {[], SpacesConfigIn}, lists:seq(1, maps:get(spaces, HarvesterConfig, 0))),

        Indices = [?INDEX_ID(I) || I <- lists:seq(1, maps:get(indices, HarvesterConfig, 0))],
        HarvestersConfig2 = HarvestersConfigIn#{
            HarvesterId => #document{
                key = HarvesterId,
                value = #od_harvester{
                    indices = Indices,
                    spaces = Spaces
                }}},
        {HarvestersConfig2, SpacesConfig2}
    end, {#{}, #{}}, HarvestersDescription),
    {HC, SC}.

mock_harvesting_stream_changes_stream_start_link(Nodes) ->
    ok = test_utils:mock_new(Nodes, harvesting_stream),
    ok = test_utils:mock_expect(Nodes, harvesting_stream, changes_stream_start_link,
        fun(_SpaceId, Callback, Since, Until) ->
            {ok, _} = couchbase_changes_stream_mock:mocked_start_link(Callback, Since, Until)
        end
    ).

mock_changes_stream(Nodes) ->
    ok = test_utils:mock_new(Nodes, couchbase_changes_stream),
    ok = test_utils:mock_expect(Nodes, couchbase_changes_stream, stop_async, fun(StreamPid) ->
        couchbase_changes_stream_mock:mocked_stop(StreamPid)
    end),
    ok = test_utils:mock_expect(Nodes, couchbase_changes_stream, get_seq_safe, fun(_, _) ->
        100
    end).

mock_provider_logic_supports_space(Nodes, Spaces) ->
    ok = test_utils:mock_expect(Nodes, provider_logic, supports_space, fun(SpaceId) ->
        lists:member(SpaceId, Spaces)
    end).

mock_harvester_logic_get(Nodes, HarvestersConfig) ->
    ok = test_utils:mock_expect(Nodes, harvester_logic, get, fun(HarvesterId) ->
        case maps:get(HarvesterId, HarvestersConfig, undefined) of
            undefined ->
                ?ERROR_NOT_FOUND;
            Doc = #document{} ->
                {ok, Doc}
        end
    end).

mock_space_logic_get_harvesters(Nodes, SpacesConfig) ->
    ok = test_utils:mock_expect(Nodes, space_logic, get_harvesters, fun(SpaceId) ->
        case maps:get(SpaceId, SpacesConfig, undefined) of
            undefined ->
                ?ERROR_NOT_FOUND;
            Harvesters when is_list(Harvesters) ->
                {ok, Harvesters}
        end
    end).

mock_harvest_metadata_success(Nodes) ->
    mock_harvest_metadata(Nodes, fun(_SpaceId, _Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        {ok, #{}}
    end).

notify_harvest_metadata_called(SpaceId, Destination, Batch, TestMasterPid) ->
    TestMasterPid ! ?HARVEST_METADATA_CALLED(SpaceId, sort_destination(Destination), Batch, self()).

mock_harvest_metadata(Nodes, Expected) ->
    TestMasterPid = self(),
    test_utils:mock_expect(Nodes, space_logic, harvest_metadata, fun(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq) ->
        Result = Expected(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq),
        notify_harvest_metadata_called(SpaceId, Destination, Batch, TestMasterPid),
        Result
    end).

count_active_children(Nodes, Ref) ->
    lists:foldl(fun(Node, Sum) ->
        Result = rpc:call(Node, supervisor, count_children, [Ref]),
        Sum + proplists:get_value(active, Result)
    end, 0, utils:ensure_list(Nodes)).

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

get_space_doc(Node, SpaceId) ->
    rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, SpaceId]).

put_into_cache(Node, Doc) ->
    rpc:call(Node, initializer, put_into_cache, [Doc]).

add_mocked_od_space_synchronization_posthook(Nodes) ->
    Self = self(),
    ok = test_utils:mock_new(Nodes, od_space),
    ok = test_utils:mock_expect(Nodes, od_space, get_posthooks, fun() ->
        % posthooks are executed sequentially, so we add simple fun on the
        % end of posthooks list, that will inform as that all posthooks were
        % executed
        meck:passthrough([]) ++ [fun
            (_F, _A, {ok, #document{key = SpaceId}}) ->
                Self ! ?OD_SPACE_POSTHOOK_EXECUTED(SpaceId);
            (_F, _A, _R) ->
                ok
        end]
    end).

wait_until_od_space_posthook_is_executed(SpaceId) ->
    receive ?OD_SPACE_POSTHOOK_EXECUTED(SpaceId) -> ok end.

get_main_stream_pid(Node, SpaceId) ->
    rpc:call(Node, global, whereis_name, [?MAIN_HARVESTING_STREAM(SpaceId)]).

get_aux_stream_pid(Node, SpaceId, HarvesterId, IndexId) ->
    rpc:call(Node, global, whereis_name, [?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId)]).

harvesting_state_get_seen_seq(Node, SpaceId, HarvesterId, IndexId) ->
    rpc:call(Node, harvesting_state, get_seen_seq, [SpaceId, HarvesterId, IndexId]).

couchbase_changes_stream_mock_registry_start_link(Node) ->
    {ok, _} = rpc:call(Node, couchbase_changes_stream_mock_registry, start_link, []).

couchbase_changes_stream_mock_registry_stop(Node) ->
    ok = rpc:call(Node, couchbase_changes_stream_mock_registry, stop, []).

couchbase_changes_stream_mock_registry_get(Node, StreamPid) ->
    couchbase_changes_stream_mock_registry_get(Node, StreamPid, ?ATTEMPTS).

couchbase_changes_stream_mock_registry_get(Node, StreamPid, 0) ->
    case rpc:call(Node, couchbase_changes_stream_mock_registry, get, [StreamPid]) of
        Pid when is_pid(Pid) -> Pid;
        Other ->
            ct:fail("couchbase_changes_stream_mock_registry:get returned ~p", [Other])
    end;
couchbase_changes_stream_mock_registry_get(Node, StreamPid, Attempts) ->
    case rpc:call(Node, couchbase_changes_stream_mock_registry, get, [StreamPid]) of
        Pid when is_pid(Pid) ->
            Pid;
        _ ->
            timer:sleep(timer:seconds(1)),
            couchbase_changes_stream_mock_registry_get(Node, StreamPid, Attempts - 1)
    end.

random_custom_metadata_seq(Changes) ->
    [Seq] = random_custom_metadata_seqs(Changes, 1),
    Seq.

random_custom_metadata_seqs(Changes, Count) when Count =< length(Changes) ->
    lists:sort(random_custom_metadata_seqs(relevant_changes(Changes, true), Count, [])).

random_custom_metadata_seqs(_RelevantChanges, 0, RandomSeqs) ->
    RandomSeqs;
random_custom_metadata_seqs(RelevantChanges, Count, RandomSeqs) ->
    Doc = #document{seq = Seq} = lists_utils:random_element(RelevantChanges),
    random_custom_metadata_seqs(RelevantChanges -- [Doc], Count - 1, [Seq | RandomSeqs]).


get_max_seq(Changes) ->
    #document{seq = Max} = lists:last(Changes),
    Max.

pretend_space_deletion(Nodes, SpaceId) ->
    SpaceGRI = #gri{type = od_space, id = SpaceId, aspect = instance, scope = private},
    PushMessage = #gs_push_graph{gri = SpaceGRI, change_type = deleted},
    rpc:call(Nodes, gs_client_worker, process_push_message, [PushMessage]).

delete_harvesting_docs(Nodes) ->
    lists:foreach(fun(I) ->
        case rpc:call(Nodes, harvesting_state, delete, [?SPACE_ID(I)]) of
            ok -> ok;
            {error, not_found} -> ok
        end
    end, lists:seq(1, 100)).

sort_destination(Destination) ->
    harvesting_destination:fold(fun(HarvesterId, Indices, AccIn) ->
        AccIn#{HarvesterId => lists:sort(Indices)}
    end, #{}, Destination).

strip_before(Changes, StripBeforeSeq) ->
    lists:filter(fun(#document{seq = Seq}) ->
        Seq < StripBeforeSeq
    end, Changes).

strip_after(Changes, StripAfterSeq) ->
    lists:filter(fun(#document{seq = Seq}) ->
        Seq >= StripAfterSeq
    end, Changes).

relevant_seqs(Changes, IgnoreDeleted) ->
    get_seqs(relevant_changes(Changes, IgnoreDeleted)).

relevant_changes(Changes, __IgnoreDeleted = false) ->
    filter_custom_metadata_changes(Changes);
relevant_changes(Changes, __IgnoreDeleted = true) ->
    CustomMetadataChanges = filter_custom_metadata_changes(Changes),
    filter_deleted_changes_before_first_not_deleted(CustomMetadataChanges).

get_seq(#document{seq = Seq}) ->
    Seq.

get_seqs(Changes) ->
    [get_seq(Change) || Change <- Changes].

filter_custom_metadata_changes(Changes) ->
    lists:filter(fun
        (#document{value = #custom_metadata{}}) -> true;
        (_) -> false
    end, Changes).

filter_deleted_changes_before_first_not_deleted(Changes) ->
    {_, ChangesAfterFirstNotDeleted} = lists:foldl(fun
        (Change = #document{deleted = false}, {true, ChangesAcc}) ->
            {false, [Change | ChangesAcc]};
        (Change, {false, ChangesAcc}) ->
            {false, [Change | ChangesAcc]};
        (_Change, AccIn) ->
            AccIn
    end, {true, []}, Changes),
    ChangesAfterFirstNotDeleted,
    lists:reverse(ChangesAfterFirstNotDeleted).

in_destination(HarvesterId, Destination) ->
    Harvesters = harvesting_destination:get_harvesters(Destination),
    lists:member(HarvesterId, Harvesters).

in_destination(HarvesterId, IndexId, Destination) ->
    Harvesters = harvesting_destination:get_harvesters(Destination),
    case lists:member(HarvesterId, Harvesters) of
        true ->
            Indices = harvesting_destination:get(HarvesterId, Destination),
            lists:member(IndexId, Indices);
        false ->
            false
    end.

sequential_subtract(L1, []) ->
    L1;
sequential_subtract([], _L2) ->
    [];
sequential_subtract([H | T], [H | T2]) ->
    sequential_subtract(T, T2);
sequential_subtract(L1, _L2) ->
    L1.
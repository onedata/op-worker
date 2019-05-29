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
-module(harvest_manager_test_SUITE).
% todo harvesting_stream_management_test_SUITE remember to rename job on bamboo !!!
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
    adding_harvester_should_start_aux_stream_to_catch_up_with_main_stream/1,
    aux_stream_should_eventually_catch_up_with_main_stream/1,
    aux_stream_should_be_started_on_index_level_error/1,
    aux_stream_should_be_started_on_harvester_level_error/1,
    error_mix_test/1, error_mix_test2/1, aux_stream_should_be_started_test/1, aux_stream_should_not_be_started_test/1]).

all() -> ?ALL([
%%    stream_sup_should_be_started,
%%    stream_supervisor_should_be_restarted,
%%    main_harvesting_stream_should_not_be_started_if_space_is_not_supported,
%%    main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_harvesters,
%%    main_harvesting_stream_should_not_be_started_if_space_does_not_have_any_indices,
%%    only_one_main_harvesting_stream_should_be_started_for_supported_space,
%%    only_one_main_harvesting_stream_should_be_started_for_supported_space_one_harvester_many_indices,
%%    only_one_main_harvesting_stream_should_be_started_for_supported_space_many_harvesters_many_indices,
%%    main_harvesting_stream_should_be_started_for_each_supported_space_with_a_harvester,
%%    adding_space_to_harvester_should_start_new_main_harvesting_stream,
%%    main_harvesting_stream_should_be_stopped_when_harvester_is_deleted,
%%    main_harvesting_stream_should_be_stopped_when_there_are_no_indices_in_the_harvester,
%%    main_harvesting_stream_should_be_stopped_when_space_is_deleted_from_harvester,
%%    start_stop_streams_mixed_test,
%%    main_stream_should_persist_last_successfully_processed_seq,
%%    adding_index_should_start_aux_stream_to_catch_up_with_main_stream,
%%    aux_stream_should_eventually_catch_up_with_main_stream,
%%    adding_harvester_should_start_aux_stream_to_catch_up_with_main_stream,
    aux_stream_should_be_started_on_index_level_error,
    aux_stream_should_be_started_on_harvester_level_error
%%    ,
%%    error_mix_test,
%%    error_mix_test2
]).

-define(OD_SPACE_POSTHOOK_EXECUTED(SpaceId),
    {od_space_posthook_executed, SpaceId}).
-define(HARVEST_METADATA_CALLED(SpaceId, Destination, FirstSeq, LastSeq, StreamPid),
    {harvest_metadata_called, SpaceId, Destination, FirstSeq, LastSeq, StreamPid}
).

-define(SPACE_ID(N), ?ID(<<"space_">>, N)).
-define(INDEX_ID(N), ?ID(<<"index_">>, N)).
-define(HARVESTER_ID(N), ?ID(<<"harvester_">>, N)).
-define(ID(Prefix, N), <<Prefix/binary, (integer_to_binary(N))/binary>>).


-define(ATTEMPTS, 60).
-define(assertHarvestMetadataCalled(SpaceId, Destination, FirstSeq, LastSeq, StreamPid),
    ?assertHarvestMetadataCalled(SpaceId, Destination, FirstSeq, LastSeq, StreamPid, Timeout)).

-define(assertHarvestMetadataCalled(SpaceId, Destination, FirstSeq, LastSeq, StreamPid, Timeout),
    ?assertReceivedEqual(
        ?HARVEST_METADATA_CALLED(SpaceId, Destination, FirstSeq, LastSeq, StreamPid),
        timer:seconds(?ATTEMPTS)
    )
).
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
    Node = random_element(Nodes),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1}
    }),
    [Space] = maps:keys(SpacesConfig),
    mock_harvester_logic_get(Nodes, HarvestersConfig),
    Harvesters = maps:keys(HarvestersConfig),
    {ok, DS = #document{value = ODS}} = get_space_doc(Node, Space),
    save_space_doc(Node, DS#document{value = ODS#od_space{harvesters = Harvesters}}),
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
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 100}
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
        pretend_space_deletion(random_element(Nodes), SpaceId)
    end, DeletedSpaces),

    ?assertMatch(5, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    update_harvesters_structure(Config, #{}, #{}),
    lists:foreach(fun(SpaceId) ->
        pretend_space_deletion(random_element(Nodes), SpaceId)
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

    Changes = couchbase_changes_stream_mock:generate_changes(1, 100, 64, 0.3, 0.5),
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),
    Max = get_max_seq(Changes),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, MainStreamPid
    ),
    % check whether maximal Seq from Changes list was persisted as processed by harvesting stream
    ?assertMatch({ok, Max}, harvesting_get_main_seen_seq(N, SpaceId), ?ATTEMPTS).

adding_index_should_start_aux_stream_to_catch_up_with_main_stream(Config) ->
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

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 61, 0.3, 0.5),
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, MainStreamPid
    ),

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
    ok = couchbase_changes_stream_mock:stop(AuxChangesStreamPid),

    % assert that aux_stream has harvested missing metadata
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FirstSeq, LastSeq, AuxStreamPid
    ),
    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 79, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),

    %Changes2 should be streamed to both indices
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        FirstSeq2, LastSeq2, MainStreamPid
    ).

aux_stream_should_be_started_test(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Node = random_element(Nodes),
    MainIdentifier = rpc:call(Node, harvesting, main_identifier, [?SPACE_ID(1)]),
    {ok, _} = rpc:call(Node, harvesting, ensure_created, [?SPACE_ID(1)]),
    Dest = harvesting_destination:init(?HARVESTER_ID(1), ?INDEX_ID(1)),
    ok = rpc:call(Node, harvesting, set_seen_seq, [MainIdentifier, Dest, 1000000]),

    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1}
    }),
    % aux_stream should be started as 2 index has never been harvested
    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS).

aux_stream_should_not_be_started_test(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Node = random_element(Nodes),
    MainIdentifier = rpc:call(Node, harvesting, main_identifier, [?SPACE_ID(1)]),
    {ok, _} = rpc:call(Node, harvesting, ensure_created, [?SPACE_ID(1)]),
    Dest = harvesting_destination:init(?HARVESTER_ID(1), [?INDEX_ID(1), ?INDEX_ID(2)]),
    ok = rpc:call(Node, harvesting, set_seen_seq, [MainIdentifier, Dest, 1000000]),

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
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5),
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5),
    {FirstSeq3, LastSeq3} = get_custom_metadata_batch_seq_range(Changes3, false),

    % stream Changes1 from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, MainStreamPid
    ),

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
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, MainStreamPid
    ),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FirstSeq, LastSeq, AuxStreamPid
    ),
%%    ok = couchbase_changes_stream_mock:stop(AuxChangesStreamPid),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes2),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FirstSeq2, LastSeq2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        FirstSeq3, LastSeq3, MainStreamPid
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

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 61, 0.3, 0.5),
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, MainStreamPid
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
    ok = couchbase_changes_stream_mock:stop(AuxChangesStreamPid),

    % assert that aux_stream has harvested missing metadata
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, AuxStreamPid
    ),
    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 79, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),

    %Changes2 should be streamed to both indices
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq2, LastSeq2, MainStreamPid).


aux_stream_should_be_started_on_index_level_error(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1}
    }),

    % generate 1st batch of changes
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5),
    FailedSeq = random_custom_metadata_seq(Changes),
    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        Indices = harvesting_destination:get(?HARVESTER_ID(1), Destination),
        case lists:member(?INDEX_ID(2), Indices) of
            true -> {ok, #{?HARVESTER_ID(1) => #{?INDEX_ID(2) => FailedSeq}}};
            false -> {ok, #{}}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % generate changes
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5),
    {FirstSeq3, LastSeq3} = get_custom_metadata_batch_seq_range(Changes3, false),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        FirstSeq, LastSeq, MainStreamPid
    ),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(2)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    mock_harvest_metadata_success(Nodes),   %"fix" Index2

%%
%%    ct:pal("AuxStreamPid: ~p", [AuxStreamPid]),
%%    ct:pal("FailedSeq: ~p", [FailedSeq]),
%%    ct:pal("LastSeq: ~p", [LastSeq]),
%%
%%    receive ANY = ?HARVEST_METADATA_CALLED(_, _, _, _, _) -> ct:pal("ANY: ~p", [ANY]) end,

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FailedSeq, LastSeq, AuxStreamPid
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FirstSeq2, LastSeq2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)]},
        FirstSeq3, LastSeq3, MainStreamPid
    ).

aux_stream_should_be_started_on_harvester_level_error(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    mock_harvest_metadata(Nodes, fun(_SpaceId, Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        case lists:member(?HARVESTER_ID(2), maps:keys(Destination)) of
            true -> {ok, #{?HARVESTER_ID(2) => {error, test_error}}};
            false -> {ok, #{}}
        end
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % generate changes
    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5),
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5),
    {FirstSeq3, LastSeq3} = get_custom_metadata_batch_seq_range(Changes3, false),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq, LastSeq, MainStreamPid),

    % aux_stream_should be started to catch up with main stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    mock_harvest_metadata_success(Nodes),   %"fix" Index2

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, AuxStreamPid
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq3, LastSeq3, MainStreamPid).

error_mix_test(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 2, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5),
    FailedSeq = random_custom_metadata_seq(Changes),
    mock_harvest_metadata(Nodes, fun(_SpaceId, _Destination, _Batch,  _MaxStreamSeq, _MaxSeq) ->
        {ok, #{
            ?HARVESTER_ID(1) => #{?INDEX_ID(2) => FailedSeq},
            ?HARVESTER_ID(2) => {error, test_error}
        }}
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % generate changes
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5),
    {FirstSeq3, LastSeq3} = get_custom_metadata_batch_seq_range(Changes3, false),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq, LastSeq, MainStreamPid),

    % aux_streams_should be started to catch up with main stream
    ?assertMatch(3, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes2),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(1), ?INDEX_ID(2)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),

    AuxStreamPid2 = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid2),

    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes),

    mock_harvest_metadata_success(Nodes),   %"fix" aux_streams

    %%    ct:pal("FirstSeq: ~p", [FirstSeq]),
    %%    ct:pal("FailedSeq: ~p", [FailedSeq]),
    %%    ct:pal("LastSeq: ~p", [LastSeq]),
    %%    ct:pal("LastSeq: ~p", [LastSeq]),
    %%
    %%    receive ?SPACE_HARVESTERS(_, _, _, _, _) = ANY -> ct:pal("ANY: ~p", [ANY]) end,
    %%
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FailedSeq, LastSeq, AuxStreamPid
    ),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq, LastSeq, AuxStreamPid2
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(3, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid12 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid12, Changes2),

    AuxChangesStreamPid22 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid2),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid22, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(2)]},
        FirstSeq2, LastSeq2, AuxStreamPid
    ),
    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, AuxStreamPid2
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1), ?INDEX_ID(2)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq3, LastSeq3, MainStreamPid).

error_mix_test2(Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(1),
    {HarvestersConfig, SpacesConfig} = harvesters_and_spaces_config(#{
        ?HARVESTER_ID(1) => #{indices => 1, spaces => 1},
        ?HARVESTER_ID(2) => #{indices => 1, spaces => 1}
    }),

    Changes = couchbase_changes_stream_mock:generate_changes(1, 101, 73, 0.3, 0.5),
    FailedSeq = random_custom_metadata_seq(Changes),
    mock_harvest_metadata(Nodes, fun(_SpaceId, _Destination, _Batch,  _MaxStreamSeq, _MaxSeq) ->
        {ok, #{
            ?HARVESTER_ID(1) => #{?INDEX_ID(1) => FailedSeq},
            ?HARVESTER_ID(2) => {error, test_error}
        }}
    end),

    update_harvesters_structure(Config, HarvestersConfig, SpacesConfig),

    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
    MainStreamPid = get_main_stream_pid(N, SpaceId),
    MainChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % generate changes
    {FirstSeq, LastSeq} = get_custom_metadata_batch_seq_range(Changes),
    Changes2 = couchbase_changes_stream_mock:generate_changes(101, 201, 59, 0.3, 0.5),
    {FirstSeq2, LastSeq2} = get_custom_metadata_batch_seq_range(Changes2, false),
    Changes3 = couchbase_changes_stream_mock:generate_changes(201, 301, 85, 0.3, 0.5),
    {FirstSeq3, LastSeq3} = get_custom_metadata_batch_seq_range(Changes3, false),

    % stream changes from mocked changes_stream to harvesting_stream
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid, Changes),

    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq, LastSeq, MainStreamPid),

    % aux_stream_should be started to catch up with main stream
    % main_stream will retry starting from FailedSeq
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    mock_harvest_metadata(Nodes, fun(_SpaceId, _Destination, _Batch,  _MaxStreamSeq, _MaxSeq) ->
        case self() =:= MainStreamPid of
            true -> {ok, #{}};
            false ->
                {ok, #{?HARVESTER_ID(2) => {error, test_error}}}
        end
    end),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FailedSeq, LastSeq, MainStreamPid
    ),

    MainChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, MainStreamPid),

    % stream next changes to main stream, so that aux stream won't catch up with previously set Until
    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid2, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(1) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, MainStreamPid
    ),

    AuxStreamPid = get_aux_stream_pid(N, SpaceId, ?HARVESTER_ID(2), ?INDEX_ID(1)),
    AuxChangesStreamPid = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid, Changes),

    mock_harvest_metadata_success(Nodes),   %"fix" aux_streams
    LastSeqBeforeFailed = get_custom_metadata_predecessor_seq(Changes, FailedSeq),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq, LastSeqBeforeFailed, AuxStreamPid
    ),

    % aux_stream should not be stopped as it hasn't caught up with main_stream
    ?assertMatch(2, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    AuxChangesStreamPid2 = couchbase_changes_stream_mock_registry_get(N, AuxStreamPid),
    couchbase_changes_stream_mock:stream_changes(AuxChangesStreamPid2, Changes2),

    ?assertHarvestMetadataCalled(SpaceId,
        #{?HARVESTER_ID(2) => [?INDEX_ID(1)]},
        FirstSeq2, LastSeq2, AuxStreamPid
    ),

    % main_stream should takeover responsibility of harvesting to index2, aux_stream should be stopped
    ?assertMatch(1, count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),

    couchbase_changes_stream_mock:stream_changes(MainChangesStreamPid2, Changes3),
    ?assertHarvestMetadataCalled(SpaceId, #{
        ?HARVESTER_ID(1) => [?INDEX_ID(1)],
        ?HARVESTER_ID(2) => [?INDEX_ID(1)]
    }, FirstSeq3, LastSeq3, MainStreamPid).

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
    [{?LOAD_MODULES, [initializer, couchbase_changes_stream_mock,
        couchbase_changes_stream_mock_registry]} | Config].

init_per_testcase(_, Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    couchbase_changes_stream_mock_registry_start_link(N),
    %%    ok = test_utils:mock_new(Nodes, harvester_logic),

    ok = mock_harvesting_stream_changes_stream_start_link(Nodes),
    ok = mock_changes_stream(Nodes),
    add_mocked_od_space_synchronization_posthook(Nodes),
    ?assertEqual(0, count_active_children(Nodes, harvesting_stream_sup)),
    initializer:communicator_mock(Nodes),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).

end_per_testcase(_, Config) ->
    [N | _] = Nodes = ?config(op_worker_nodes, Config),
    %%    ok = test_utils:mock_unload(Nodes, harvester_logic),
    ok = test_utils:mock_unload(Nodes, harvesting_stream),
    ok = test_utils:mock_unload(Nodes, couchbase_changes_stream),
    lists:foreach(fun(Node) ->
        true = rpc:call(Node, erlang, exit, [whereis(Node, harvesting_stream_sup), kill])
    end, Nodes),
    ?assertMatch(0, catch count_active_children(Nodes, harvesting_stream_sup), ?ATTEMPTS),
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
    Node = random_element(Nodes),
    Spaces = maps:keys(SpacesConfig),
    mock_harvester_logic_get(Nodes, HarvestersConfig),
    mock_space_logic_get_harvesters(Nodes, SpacesConfig),
    mock_provider_logic_supports_space(Nodes, Spaces),

    maps:fold(fun(SpaceId, Harvesters, _) ->
        {ok, DS = #document{value = ODS}} = get_space_doc(Node, SpaceId),
        % trigger call to od_space:save_to_cache
        save_space_doc(Node, DS#document{value = ODS#od_space{harvesters = Harvesters}})
    end, undefined, SpacesConfig),

    maps:fold(fun(_HarvesterId, HarvesterDoc, _) ->
        save_harvester_doc(Node, HarvesterDoc)
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
            couchbase_changes_stream_mock:mocked_start_link(Callback, Since, Until)
        end).

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
        %%        od_harvester:save_to_cache(maps:get(SpaceId, SpacesConfig)),
        {ok, maps:get(SpaceId, SpacesConfig, ?ERROR_NOT_FOUND)}
    end).

mock_harvest_metadata_success(Nodes) ->
    mock_harvest_metadata(Nodes, fun(_SpaceId, _Destination, _Batch, _MaxStreamSeq, _MaxSeq) ->
        {ok, #{}}
    end).

notify_harvest_metadata_called(SpaceId, Destination, [], Self) ->
    Self ! ?HARVEST_METADATA_CALLED(SpaceId, sort_destination(Destination), undefined, undefined, self());
notify_harvest_metadata_called(SpaceId, Destination, Batch, Self) ->
    #{<<"seq">> := FirstSeq} = hd(Batch),
    #{<<"seq">> := LastSeq} = lists:last(Batch),
    Self ! ?HARVEST_METADATA_CALLED(SpaceId, sort_destination(Destination), FirstSeq, LastSeq, self()).

mock_harvest_metadata(Nodes, Expected) ->
    Self = self(),
    test_utils:mock_expect(Nodes, space_logic, harvest_metadata, fun(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq) ->
        Result = Expected(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq),
        notify_harvest_metadata_called(SpaceId, Destination, Batch, Self),
        Result
    end).

count_active_children(Nodes, Ref) ->
    lists:foldl(fun(Node, Sum) ->
        Result = rpc:call(Node, supervisor, count_children, [Ref]),
        Sum + proplists:get_value(active, Result)
    end, 0, Nodes).

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

random_element(List) ->
    lists:nth(rand:uniform(length(List)), List).

get_harvester_doc(Node, HarvesterId) ->
    rpc:call(Node, harvester_logic, get, [HarvesterId]).

get_space_doc(Node, SpaceId) ->
    rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, SpaceId]).

save_space_doc(Node, Doc) ->
    rpc:call(Node, od_space, save_to_cache, [Doc]).

save_harvester_doc(Node, Doc) ->
    rpc:call(Node, od_harvester, save_to_cache, [Doc]).

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

harvesting_main_identifier(Node, SpaceId) ->
    rpc:call(Node, harvesting, main_identifier, [SpaceId]).

harvesting_get_main_seen_seq(Node, SpaceId) ->
    rpc:call(Node, harvesting, get_main_seen_seq, [SpaceId]).

couchbase_changes_stream_mock_registry_start_link(Node) ->
    {ok, _} = rpc:call(Node, couchbase_changes_stream_mock_registry, start_link, []).

couchbase_changes_stream_mock_registry_stop(Node) ->
    ok = rpc:call(Node, couchbase_changes_stream_mock_registry, stop, []).

couchbase_changes_stream_mock_registry_get(Node, StreamPid) ->
    couchbase_changes_stream_mock_registry_get(Node, StreamPid, ?ATTEMPTS).

couchbase_changes_stream_mock_registry_get(Node, StreamPid, 0) ->
    case rpc:call(Node, couchbase_changes_stream_mock_registry, get, [StreamPid]) of
        Pid when is_pid(Pid) -> Pid;
        Other -> ct:fail("couchbase_changes_stream_mock_registry:get returned ~p", [Other])
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
    {_, CustomMetadataChangesAfterFirstNotDeleted} = lists:foldl(fun
        (Change = #document{deleted = false, value = #custom_metadata{}}, {true, ChangesAcc}) ->
            {false, [Change | ChangesAcc]};
        (Change = #document{value = #custom_metadata{}}, {false, ChangesAcc}) ->
            {false, [Change | ChangesAcc]};
        (_Change, AccIn) ->
            AccIn
    end, {true, []}, Changes),
    #document{seq = Seq} = random_element(CustomMetadataChangesAfterFirstNotDeleted),
    Seq.

get_custom_metadata_batch_seq_range(Changes) ->
    get_custom_metadata_batch_seq_range(Changes, true).

get_custom_metadata_batch_seq_range(Changes, _IgnoreDeleted = true) ->
    lists:foldl(fun
        (#document{value = #custom_metadata{}, deleted = true}, AccIn = {undefined, _}) ->
            AccIn;
        (#document{seq = Seq, value = #custom_metadata{}, deleted = false}, {undefined, _}) ->
            {Seq, Seq};
        (#document{seq = Seq, value = #custom_metadata{}}, {FirstSeqIn, _}) ->
            {FirstSeqIn, Seq};
        (#document{}, AccIn) ->
            AccIn
    end, {undefined, undefined}, Changes);
get_custom_metadata_batch_seq_range(Changes, _IgnoreDeleted = false) ->
    lists:foldl(fun
        (#document{seq = Seq, value = #custom_metadata{}}, {undefined, _}) ->
            {Seq, Seq};
        (#document{seq = Seq, value = #custom_metadata{}}, {FirstSeqIn, _}) ->
            {FirstSeqIn, Seq};
        (#document{}, AccIn) ->
            AccIn
    end, {undefined, undefined}, Changes).

get_max_seq(Changes) ->
    #document{seq = Max} = lists:last(Changes),
    Max.

get_custom_metadata_predecessor_seq(Changes, BeforeSeq) ->
    lists:foldl(fun
        (#document{seq = Seq, value = #custom_metadata{}}, _) when Seq < BeforeSeq ->
            Seq;
        (_Change, Predecessor) ->
            Predecessor
    end, -1, Changes).

get_max_custom_metadata_seq(Changes) ->
    lists:foldl(fun
        (#document{value = #custom_metadata{}, seq = Seq}, _) -> Seq;
        (#document{}, AccIn) -> AccIn
    end, undefined, Changes).

pretend_space_deletion(Nodes, SpaceId) ->
    SpaceGRI = #gri{type = od_space, id = SpaceId, aspect = instance, scope = private},
    PushMessage = #gs_push_graph{gri = SpaceGRI, change_type = deleted},
    rpc:call(Nodes, gs_client_worker, process_push_message, [PushMessage]).

delete_harvesting_docs(Nodes) ->
    lists:foreach(fun(I) ->
        case rpc:call(Nodes, harvesting, delete, [?SPACE_ID(I)]) of
            ok -> ok;
            {error, not_found} -> ok
        end
    end, lists:seq(1, 100)).

sort_destination(Destination) ->
    harvesting_destination:fold(fun(HarvesterId, Indices, AccIn) ->
        AccIn#{HarvesterId => lists:sort(Indices)}
    end, #{}, Destination).
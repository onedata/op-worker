%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for replica_finder module.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_finder_test).

-ifdef(TEST).

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").


-define(LOCATION(ProviderId, Blocks), ?LOCATION(ProviderId, Blocks, undefined)).
-define(LOCATION(ProviderId, Blocks, Size), ?LOCATION(undefined, ProviderId, Blocks, Size, #{})).
-define(LOCATION(LocationId, ProviderId, Blocks, Size, VV), #document{
    key = LocationId,
    value = #file_location{
        provider_id = ProviderId,
        blocks = Blocks,
        size = Size,
        version_vector = VV
    }}).
-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).
-define(VV(LocationId, ProviderId, Version), #{{LocationId, ProviderId} => Version}).

-define(LOCAL_PID, <<"local">>).

-define(PID1, <<"provider1">>).
-define(PID2, <<"provider2">>).
-define(PID3, <<"provider3">>).

-define(LOC1, <<"provider1">>).
-define(LOC2, <<"provider2">>).
-define(LOC3, <<"provider3">>).
-define(LOC4, <<"provider4">>).

%%%===================================================================
%%% Test generators
%%%===================================================================

get_blocks_for_sync_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun finder_should_return_empty_list_for_empty_blocks/1,
            fun finder_should_return_empty_list_for_empty_locations/1,
            fun finder_should_return_shorter_list_for_request_exceeding_file/1,
            fun finder_should_find_data_in_one_location/1,
            fun finder_should_find_data_in_many_locations/1,
            fun finder_should_find_data_in_many_providers/1,
            fun finder_should_minimize_returned_blocks/1,
            fun finder_should_not_return_data_available_locally_in_one_location/1,
            fun finder_should_not_return_data_available_locally_in_many_locations/1,
            fun finder_should_not_return_duplicated_blocks_if_file_is_not_local/1,
            fun finder_should_not_return_duplicated_blocks_if_file_is_not_fully_replicated/1,
            fun finder_should_return_duplicated_blocks_if_file_is_fully_replicated/1,
            fun finder_should_return_duplicated_blocks_if_file_is_fully_replicated2/1,
            fun finder_should_return_duplicated_blocks_if_file_is_fully_replicated3/1,
            fun finder_should_return_duplicated_blocks_if_file_is_fully_replicated4/1,
            fun finder_should_return_duplicated_blocks_only_for_provider_where_file_is_fully_replicated/1,
            fun finder_should_not_return_duplicated_blocks_if_remote_replica_has_lesser_version/1,
            fun finder_should_not_return_duplicated_blocks_if_remote_replica_has_concurrent_version/1,
            fun finder_should_return_duplicated_blocks_if_remote_replica_has_equal_version/1,
            fun finder_should_return_duplicated_blocks_if_remote_replica_has_greater_version/1,
            fun finder_should_return_duplicated_blocks_only_from_providers_with_equal_or_greater_versions/1
        ]}.

%%%===================================================================
%%% Test functions
%%%===================================================================

finder_should_return_empty_list_for_empty_blocks(_) ->
    erase(),
    Location1 = ?LOCATION(<<"1">>, [?BLOCK(0,1)]),
    Location2 = ?LOCATION(<<"2">>, [?BLOCK(1,5)]),
    Locations = [Location1, Location2],
    BlocksToSync = [],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([], Ans).

finder_should_return_empty_list_for_empty_locations(_) ->
    Locations = [],
    BlocksToSync = [?BLOCK(0,5)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([], Ans).

finder_should_return_shorter_list_for_request_exceeding_file(_) ->
    Location1 = ?LOCATION(?PID1, [?BLOCK(0,2)]),
    Locations = [Location1, ?LOCATION(?LOCAL_PID, [])],
    BlocksToSync = [?BLOCK(1,5)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertMatch([{?PID1, [?BLOCK(1,1)], _}], Ans).

finder_should_find_data_in_one_location(_) ->
    Locations = [?LOCATION(?PID1, [?BLOCK(1,5), ?BLOCK(7,9)]), ?LOCATION(?LOCAL_PID, [])],
    BlocksToSync = [?BLOCK(2,8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertMatch([{?PID1, [?BLOCK(2,4), ?BLOCK(7,3)], _}], Ans).


finder_should_find_data_in_many_locations(_) ->
    Location1_1 = ?LOCATION(?PID1, [?BLOCK(0, 2)]),
    Location1_2 = ?LOCATION(?PID1, [?BLOCK(2, 5)]),
    Location1_3 = ?LOCATION(?PID1, [?BLOCK(7, 3)]),
    LocalLocation = ?LOCATION(?LOCAL_PID, [], 9),
    Locations = [Location1_1, Location1_2, Location1_3, LocalLocation],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertMatch([{?PID1, [?BLOCK(1, 8)], _}], Ans).


finder_should_find_data_in_many_providers(_) ->
    Location1_1 = ?LOCATION(?PID1, [?BLOCK(0, 1)]),
    Location1_2 = ?LOCATION(?PID1, [?BLOCK(1, 1)]),
    Location1_3 = ?LOCATION(?PID1, [?BLOCK(2, 1)]),
    Location2_1 = ?LOCATION(?PID2, [?BLOCK(3, 1)]),
    Location2_2 = ?LOCATION(?PID2, [?BLOCK(4, 1)]),
    Location2_3 = ?LOCATION(?PID2, [?BLOCK(5, 1)]),
    Location3_1 = ?LOCATION(?PID3, [?BLOCK(6, 1)]),
    Location3_2 = ?LOCATION(?PID3, [?BLOCK(7, 1)]),
    Location3_3 = ?LOCATION(?PID3, [?BLOCK(8, 1)]),
    LocalLocation = ?LOCATION(?LOCAL_PID, []),
    Locations = [LocalLocation,
        Location1_1, Location1_2, Location1_3,
        Location2_1, Location2_2, Location2_3,
        Location3_1, Location3_2, Location3_3
    ],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertMatch([
        {?PID1, [?BLOCK(1, 2)], _},
        {?PID2, [?BLOCK(3, 3)], _},
        {?PID3, [?BLOCK(6, 3)], _}
    ], Ans).


finder_should_minimize_returned_blocks(_) ->
    Location1 = ?LOCATION(?PID3, [?BLOCK(0, 3)]),
    Location2 = ?LOCATION(?PID2, [?BLOCK(0, 6)]),
    Location3 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    LocalLocation = ?LOCATION(?LOCAL_PID, []),
    Locations = [LocalLocation, Location1, Location2, Location3],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertMatch([
        {?PID1, [?BLOCK(1, 8)], _}
    ], Ans).


finder_should_not_return_data_available_locally_in_one_location(_) ->
    LocalLocation = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 10)]),
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Locations = [LocalLocation, Location1],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([], Ans).

finder_should_not_return_data_available_locally_in_many_locations(_) ->
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 2)], 2),
    LocalLocation2 = ?LOCATION(?LOCAL_PID, [?BLOCK(2, 5)], 7),
    LocalLocation3 = ?LOCATION(?LOCAL_PID, [?BLOCK(7, 3)], 9),
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Locations = [LocalLocation1, LocalLocation2, LocalLocation3, Location1],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([], Ans).

finder_should_not_return_duplicated_blocks_if_file_is_not_local(_) ->
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Location2 = ?LOCATION(?PID1, [?BLOCK(15, 20)]),
    Location3 = ?LOCATION(?PID2, [?BLOCK(7, 13)]),
    Locations = [Location1, Location2, Location3],
    ?_assertEqual(undefined, replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_not_return_duplicated_blocks_if_file_is_not_fully_replicated(_) ->
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 11)]),
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_if_file_is_fully_replicated(_) ->
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 10)]),
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([{?PID1, [?BLOCK(0, 10)]}], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_if_file_is_fully_replicated2(_) ->
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 10)]),
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 11)]),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([{?PID1, [?BLOCK(0, 11)]}], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_if_file_is_fully_replicated3(_) ->
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 11)]),
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 1), ?BLOCK(2, 1), ?BLOCK(4, 1), ?BLOCK(6, 1), ?BLOCK(8, 1)]),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([{?PID1, [?BLOCK(0, 11)]}], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_if_file_is_fully_replicated4(_) ->
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 1), ?BLOCK(2, 1), ?BLOCK(4, 1), ?BLOCK(6, 1), ?BLOCK(8, 1)]),
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 1), ?BLOCK(2, 1), ?BLOCK(4, 1), ?BLOCK(6, 1), ?BLOCK(8, 1)]),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([{?PID1, [?BLOCK(0, 1), ?BLOCK(2, 1), ?BLOCK(4, 1), ?BLOCK(6, 1), ?BLOCK(8, 1)]}], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_only_for_provider_where_file_is_fully_replicated(_) ->
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Location2 = ?LOCATION(?PID2, [?BLOCK(0, 9)]),
    Location3 = ?LOCATION(?PID3, [?BLOCK(0, 11)]),
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 2), ?BLOCK(4, 6)]),
    Locations = [Location1, Location2, Location3, LocalLocation1],
    ?_assertEqual([
        {?PID1, [?BLOCK(0, 10)]},
        {?PID3, [?BLOCK(0, 11)]}
    ], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_not_return_duplicated_blocks_if_remote_replica_has_concurrent_version(_) ->
    LocalLocation1 = ?LOCATION(?LOC1, ?LOCAL_PID, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Location1 = ?LOCATION(?LOC2, ?PID1, [?BLOCK(0, 10)], 10, ?VV(?LOC2, ?PID1, 1)),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_not_return_duplicated_blocks_if_remote_replica_has_lesser_version(_) ->
    LocalLocation1 = ?LOCATION(?LOC1, ?LOCAL_PID, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Location1 = ?LOCATION(?LOC2, ?PID1, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 1)),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_if_remote_replica_has_equal_version(_) ->
    LocalLocation1 = ?LOCATION(?LOC1, ?LOCAL_PID, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Location1 = ?LOCATION(?LOC2, ?PID1, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([{?PID1, [?BLOCK(0, 10)]}], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_if_remote_replica_has_greater_version(_) ->
    LocalLocation1 = ?LOCATION(?LOC1, ?LOCAL_PID, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Location1 = ?LOCATION(?LOC2, ?PID1, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 3)),
    Locations = [Location1, LocalLocation1],
    ?_assertEqual([{?PID1, [?BLOCK(0, 10)]}], replica_finder:get_remote_duplicated_blocks(Locations)).

finder_should_return_duplicated_blocks_only_from_providers_with_equal_or_greater_versions(_) ->
    LocalLocation1 = ?LOCATION(?LOC1, ?LOCAL_PID, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Location1 = ?LOCATION(?LOC2, ?PID1, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 1)),
    Location2 = ?LOCATION(?LOC3, ?PID2, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 2)),
    Location3 = ?LOCATION(?LOC4, ?PID3, [?BLOCK(0, 10)], 10, ?VV(?LOC1, ?LOCAL_PID, 3)),
    Locations = [Location1, Location2, Location3, LocalLocation1],
    ?_assertEqual([
        {?PID2, [?BLOCK(0, 10)]},
        {?PID3, [?BLOCK(0, 10)]}
    ], replica_finder:get_remote_duplicated_blocks(Locations)).

%%%===================================================================
%%% Test fixtures
%%%===================================================================


start() ->
    meck:new([oneprovider]),
    meck:expect(oneprovider, get_id, fun() -> ?LOCAL_PID end),
    application:set_env(?APP_NAME, synchronizer_block_suiting, false),
    ok.


stop(_) ->
    application:unset_env(?APP_NAME, synchronizer_block_suiting),
    ?assert(meck:validate([oneprovider])),
    meck:unload().

-endif.

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

-include("proto/oneclient/common_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

-define(LOCATION(ProviderId, Blocks), #document{value = #file_location{provider_id = ProviderId, blocks = Blocks}}).
-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).
-define(LOCAL_PID, <<"local">>).
-define(PID1, <<"1">>).
-define(PID2, <<"2">>).
-define(PID3, <<"3">>).


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
            fun finder_should_not_return_data_available_locally_in_many_locations/1
        ]}.

%%%===================================================================
%%% Test functions
%%%===================================================================

finder_should_return_empty_list_for_empty_blocks(_) ->
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
    Location1 = ?LOCATION(<<"1">>, [?BLOCK(0,2)]),
    Locations = [Location1],
    BlocksToSync = [?BLOCK(1,5)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([{?PID1, [?BLOCK(1,1)]}], Ans).

finder_should_find_data_in_one_location(_) ->
    Locations = [?LOCATION(?PID1, [?BLOCK(1,5), ?BLOCK(7,9)])],
    BlocksToSync = [?BLOCK(2,8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([{?PID1, [?BLOCK(2,4), ?BLOCK(7,3)]}], Ans).


finder_should_find_data_in_many_locations(_) ->
    Location1_1 = ?LOCATION(?PID1, [?BLOCK(0, 2)]),
    Location1_2 = ?LOCATION(?PID1, [?BLOCK(2, 5)]),
    Location1_3 = ?LOCATION(?PID1, [?BLOCK(7, 3)]),
    LocalLocation = ?LOCATION(?LOCAL_PID, []),
    Locations = [Location1_1, Location1_2, Location1_3, LocalLocation],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([{?PID1, [?BLOCK(1, 8)]}], Ans).


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
    ?_assertEqual([
        {?PID3, [?BLOCK(6, 3)]},
        {?PID2, [?BLOCK(3, 3)]},
        {?PID1, [?BLOCK(1, 2)]}
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
    ?_assertEqual([
        {?PID3, [?BLOCK(1, 2)]},
        {?PID2, [?BLOCK(3, 3)]},
        {?PID1, [?BLOCK(6, 3)]}
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
    LocalLocation1 = ?LOCATION(?LOCAL_PID, [?BLOCK(0, 2)]),
    LocalLocation2 = ?LOCATION(?LOCAL_PID, [?BLOCK(2, 5)]),
    LocalLocation3 = ?LOCATION(?LOCAL_PID, [?BLOCK(7, 3)]),
    Location1 = ?LOCATION(?PID1, [?BLOCK(0, 10)]),
    Locations = [LocalLocation1, LocalLocation2, LocalLocation3, Location1],
    BlocksToSync = [?BLOCK(1, 8)],

    % when
    Ans = replica_finder:get_blocks_for_sync(Locations, BlocksToSync),

    %then
    ?_assertEqual([], Ans).

%%%===================================================================
%%% Test fixtures
%%%===================================================================


start() ->
    meck:new([oneprovider]),
    meck:expect(oneprovider, get_provider_id, 0, ?LOCAL_PID),
    ok.


stop(_) ->
    ?assert(meck:validate([oneprovider])),
    meck:unload().

-endif.
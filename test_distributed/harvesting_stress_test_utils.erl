%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains utils function used in stress tests of harvesting.
%%% @end
%%%--------------------------------------------------------------------
-module(harvesting_stress_test_utils).
-author("Jakub Kudzia").

-include("harvesting_stress_test_utils.hrl").

%% API
-export([mock_harvesting/1, mock_harvesting_stopped/1, harvesting_receive_loop/1,
    revise_all_spaces/1, revise_space_harvesters/2, delete_harvesting_state/2]).

%%%===================================================================
%%% API
%%%===================================================================

mock_harvesting(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, space_logic, get_harvesters,
        fun(_SpaceId) -> {ok, [?HARVESTER_ID]} end),
    ok = test_utils:mock_expect(Node, harvester_logic, get_indices,
        fun(_SpaceId) -> {ok, [?INDEX_ID]} end),
    ok = test_utils:mock_expect(Node, space_logic, harvest_metadata,
        fun(_SpaceId, _Destination, Batch, _MaxStreamSeq, _MaxSeq) ->
            Self ! ?HARVEST_METADATA(length(Batch)),
            {ok, #{}}
        end
    ).

mock_harvesting_stopped(Node) ->
    ok = test_utils:mock_expect(Node, space_logic, get_harvesters,
        fun(_SpaceId) -> {ok, []} end),
    ok = test_utils:mock_expect(Node, harvester_logic, get_indices,
        fun(_SpaceId) -> {ok, []} end).

harvesting_receive_loop(0) ->
    ok;
harvesting_receive_loop(ExpChangesNum) ->
    receive
        ?HARVEST_METADATA(Size) ->
            harvesting_receive_loop(ExpChangesNum - Size)
    after
        ?TIMEOUT ->
            ct:fail("harvesting_receive_loop timeout with ~p changes left.", [ExpChangesNum])
    end.

revise_space_harvesters(Node, SpaceId) ->
    ok = rpc:call(Node, main_harvesting_stream, revise_space_harvesters, [SpaceId, []]).

revise_all_spaces(Node) ->
    ok = rpc:call(Node, main_harvesting_stream, revise_all_spaces, []).

delete_harvesting_state(Worker, SpaceId) ->
    ok = rpc:call(Worker, harvesting_state, delete, [SpaceId]).

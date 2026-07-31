%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Helpers for harvesting stress test suites. Every space is mocked to be
%%% harvested by a single harvester with a single index and the ids of files
%%% submitted for harvesting are sent back to the test process.
%%% @end
%%%--------------------------------------------------------------------
-module(harvesting_stress_test_utils).
-author("Jakub Kudzia").


-define(HARVESTER_ID, <<"harvester1">>).
-define(INDEX_ID, <<"index1">>).

-define(HARVESTED_FILE_IDS(FileIds), {harvested_file_ids, FileIds}).

-define(TIMEOUT, timer:minutes(15)).

%% API
-export([mock_harvesting/1, mock_harvesting_stopped/1, await_files_harvested/1,
    revise_all_spaces/1, revise_space_harvesters/2, delete_harvesting_state/2, count_active_children/2]).

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
            FileIds = [maps:get(<<"fileId">>, Entry) || Entry <- Batch],
            Self ! ?HARVESTED_FILE_IDS(FileIds),
            {ok, #{}}
        end
    ).

mock_harvesting_stopped(Node) ->
    ok = test_utils:mock_expect(Node, space_logic, get_harvesters,
        fun(_SpaceId) -> {ok, []} end),
    ok = test_utils:mock_expect(Node, harvester_logic, get_indices,
        fun(_SpaceId) -> {ok, []} end).

await_files_harvested(ExpectedFilesToHarvestCount) ->
    await_files_harvested(sets:new(), ExpectedFilesToHarvestCount).

await_files_harvested(HarvestedFileIds, ExpectedFilesToHarvestCount) ->
    case sets:size(HarvestedFileIds) =:= ExpectedFilesToHarvestCount of
        true ->
            ok;
        false ->
            receive
                ?HARVESTED_FILE_IDS(FileIds) ->
                    NewHarvestedFileIds = sets:union(HarvestedFileIds, sets:from_list(FileIds)),
                    await_files_harvested(NewHarvestedFileIds, ExpectedFilesToHarvestCount)
            after
                ?TIMEOUT ->
                    ct:print("await_files_harvested timeout with ~tp changes left.",
                        [ExpectedFilesToHarvestCount - sets:size(HarvestedFileIds)]),
                    ct:fail("await_files_harvested timeout")
            end
    end.

revise_space_harvesters(Node, SpaceId) ->
    ok = rpc:call(Node, main_harvesting_stream, revise_space_harvesters, [SpaceId, []]).

revise_all_spaces(Node) ->
    ok = rpc:call(Node, main_harvesting_stream, revise_all_spaces, []).

delete_harvesting_state(Worker, SpaceId) ->
    ok = rpc:call(Worker, harvesting_state, delete, [SpaceId]).

count_active_children(Nodes, SupervisorName) ->
    lists:foldl(fun(Node, Sum) ->
        Result = rpc:call(Node, supervisor, count_children, [SupervisorName]),
        Sum + proplists:get_value(active, Result)
    end, 0, utils:ensure_list(Nodes)).
%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module containing common functions used across archivisation 
%%% traverse modules. 
%%% @end
%%%-------------------------------------------------------------------
-module(archive_traverse_common).
-author("Michal Stanisz").

-include("modules/datastore/datastore_runner.hrl").

%% API
-export([update_children_count/4, take_children_count/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec update_children_count(tree_traverse:pool(), tree_traverse:id(), file_meta:uuid(), non_neg_integer()) ->
    ok.
update_children_count(PoolName, TaskId, DirUuid, ChildrenCount) ->
    ?extract_ok(traverse_task:update_additional_data(traverse_task:get_ctx(), PoolName, TaskId,
        fun(AD) ->
            PrevCountMap = get_count_map(AD),
            UpdatedMap = case PrevCountMap of
                #{DirUuid := PrevCountBin} ->
                    PrevCountMap#{
                        DirUuid => integer_to_binary(binary_to_integer(PrevCountBin) + ChildrenCount)
                    };
                _ ->
                    PrevCountMap#{
                        DirUuid => integer_to_binary(ChildrenCount)
                    }
            end,
            {ok, set_count_map(AD, UpdatedMap)}
        end
    )).


-spec take_children_count(tree_traverse:pool(), tree_traverse:id(), file_meta:uuid()) ->
    non_neg_integer().
take_children_count(PoolName, TaskId, DirUuid) ->
    {ok, AdditionalData} = traverse_task:get_additional_data(PoolName, TaskId),
    ChildrenCount = maps:get(DirUuid, get_count_map(AdditionalData)),
    ok = ?extract_ok(traverse_task:update_additional_data(traverse_task:get_ctx(), PoolName, TaskId,
        fun(AD) ->
            CountMap = get_count_map(AD),
            {ok, set_count_map(AD, maps:without([DirUuid], CountMap))}
        end
    )),
    binary_to_integer(ChildrenCount).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_count_map(traverse:additional_data()) -> map().
get_count_map(AD) ->
    CountMapBin = maps:get(<<"children_count_map">>, AD, term_to_binary(#{})),
    binary_to_term(CountMapBin).


-spec set_count_map(traverse:additional_data(), map()) -> traverse:additional_data().
set_count_map(AD, CountMap) ->
    AD#{<<"children_count_map">> => term_to_binary(CountMap)}.

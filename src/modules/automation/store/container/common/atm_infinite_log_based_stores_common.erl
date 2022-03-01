%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common helpers for infinite log based stores (containers and container iterators).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_infinite_log_based_stores_common).
-author("Lukasz Opiola").

-include_lib("ctool/include/errors.hrl").

%% API
-export([assert_numerical_index/1]).
-export([get_next_batch/4]).

%@formatter:off
-type listing_postprocessor() :: json_infinite_log_model:listing_postprocessor(
    {atm_store_api:index(), {ok, atm_value:expanded()} | errors:error()}
).
%@formatter:on

-export_type([listing_postprocessor/0]).

%%%===================================================================
%%% API
%%%===================================================================


%% TODO spec
assert_numerical_index(Index) ->
    try
        binary_to_integer(Index),
        true
    catch _:_ ->
        throw(?ERROR_BAD_DATA(<<"index">>, <<"not numerical">>))
    end.


-spec get_next_batch(
    atm_store_container_iterator:batch_size(),
    json_infinite_log_model:id(),
    json_infinite_log_model:entry_index(),
    listing_postprocessor()
) ->
    {ok, [atm_value:expanded()], json_infinite_log_model:entry_index()} | stop.
get_next_batch(BatchSize, BackendId, LastListedIndex, ListingPostprocessor) ->
    {ok, {ProgressMarker, EntrySeries}} = json_infinite_log_model:list_and_postprocess(BackendId, #{
        start_from => {index_exclusive, LastListedIndex},
        limit => BatchSize
    }, ListingPostprocessor),
    case {EntrySeries, ProgressMarker} of
        {[], done} ->
            stop;
        _ ->
            {NewLastListedIndex, _} = lists:last(EntrySeries),
            FilteredEntries = lists:filtermap(fun
                ({_Index, {ok, Value}}) -> {true, Value};
                ({_Index, {error, _}}) -> false
            end, EntrySeries),
            {ok, FilteredEntries, NewLastListedIndex}
    end.

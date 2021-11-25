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
-export([browse_content/4]).
-export([get_next_batch/4]).

-type store_type() :: list_store | audit_log_store.

%@formatter:off
-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    start_timestamp => time:millis(),  % allowed only for audit log store
    offset => atm_store_api:offset()
}.

-type listing_postprocessor() :: json_based_infinite_log_model:listing_postprocessor(
    {atm_store_api:index(), {ok, automation:item()} | errors:error()}
).
%@formatter:on

-export_type([listing_postprocessor/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec browse_content(
    store_type(),
    json_based_infinite_log_model:id(),
    browse_options(),
    listing_postprocessor()
) ->
    atm_store_api:browse_result() | no_return().
browse_content(StoreType, BackendId, BrowseOpts, ListingPostprocessor) ->
    SanitizedBrowseOpts = sanitize_browse_options(StoreType, BrowseOpts),
    {ok, {ProgressMarker, EntrySeries}} = json_based_infinite_log_model:list_and_postprocess(BackendId, #{
        start_from => infer_start_from(StoreType, SanitizedBrowseOpts),
        offset => maps:get(offset, SanitizedBrowseOpts, 0),
        limit => maps:get(limit, SanitizedBrowseOpts)
    }, ListingPostprocessor),
    {EntrySeries, ProgressMarker =:= done}.


-spec get_next_batch(
    atm_store_container_iterator:batch_size(),
    json_based_infinite_log_model:id(),
    json_based_infinite_log_model:entry_index(),
    listing_postprocessor()
) ->
    {ok, [atm_value:expanded()], json_based_infinite_log_model:entry_index()} | stop.
get_next_batch(BatchSize, BackendId, LastListedIndex, ListingPostprocessor) ->
    {ok, {ProgressMarker, EntrySeries}} = json_based_infinite_log_model:list_and_postprocess(BackendId, #{
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec sanitize_browse_options(store_type(), browse_options()) -> browse_options().
sanitize_browse_options(StoreType, BrowseOpts) ->
    TimestampOption = case StoreType of
        audit_log_store ->
            #{start_timestamp => {integer, {not_lower_than, 0}}};
        list_store ->
            #{}
    end,
    middleware_sanitizer:sanitize_data(BrowseOpts, #{
        required => #{
            limit => {integer, {not_lower_than, 1}}
        },
        at_least_one => TimestampOption#{
            offset => {integer, any},
            start_index => {binary, any}
        }
    }).


%% @private
-spec infer_start_from(store_type(), atm_store_api:browse_options()) ->
    undefined | {index, infinite_log:entry_index()} | {timestamp, infinite_log:timestamp()}.
infer_start_from(_, #{start_index := <<>>}) ->
    undefined;
infer_start_from(_, #{start_index := StartIndexBin}) ->
    try
        binary_to_integer(StartIndexBin)
    catch _:_ ->
        throw(?ERROR_BAD_DATA(<<"index">>, <<"not numerical">>))
    end,
    {index, StartIndexBin};
infer_start_from(audit_log_store, #{start_timestamp := StartTimestamp}) ->
    {timestamp, StartTimestamp};
infer_start_from(_, _) ->
    undefined.

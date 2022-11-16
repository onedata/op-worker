%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Proxy module for infinite_log based automation stores extracting common logic.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_container_infinite_log_backend).
-author("Lukasz Opiola").

-include_lib("ctool/include/errors.hrl").

%% CRUD API
-export([
    create/0,
    delete/1,

    append/2,

    sanitize_listing_opts/2,
    list_entries/3,

    entry_to_json/1
]).
%% Iterator API
-export([
    iterator_start_index/0,
    iterator_get_next_batch/4
]).


-type id() :: json_infinite_log_model:id().

-define(MAX_LISTING_LIMIT, 1000).
-define(DEFAULT_LISTING_LIMIT, 1000).

-type index() :: json_infinite_log_model:entry_index().
-type timestamp_millis() :: time:millis().
-type offset() :: integer().
-type limit() :: 1..?MAX_LISTING_LIMIT.

-type timestamp_agnostic_listing_opts() :: #{
    start_from => undefined | {index, index()},
    offset => offset(),
    limit => limit()
}.
-type timestamp_aware_listing_opts() :: #{
    start_from => undefined | {index, index()} | {timestamp, timestamp_millis()},
    offset => offset(),
    limit => limit()
}.
-type listing_opts() :: timestamp_agnostic_listing_opts() | timestamp_aware_listing_opts().

-type entry() :: {index(), {ok, atm_value:expanded()} | errors:error()}.
-type listing_postprocessor() :: json_infinite_log_model:listing_postprocessor(entry()).

-export_type([
    id/0,
    index/0, timestamp_millis/0, offset/0, limit/0,
    timestamp_agnostic_listing_opts/0, timestamp_aware_listing_opts/0, listing_opts/0,
    entry/0, listing_postprocessor/0
]).


%%%===================================================================
%%% CRUD API
%%%===================================================================


-spec create() -> id().
create() ->
    {ok, Id} = json_infinite_log_model:create(#{
        %% TODO VFS-9934 implement dynamic slot management
        max_entries_per_node => 100
    }),
    Id.


-spec delete(id()) -> ok.
delete(Id) ->
    json_infinite_log_model:destroy(Id).


-spec append(id(), atm_value:compressed()) -> ok.
append(Id, CompressedItem) ->
    ok = json_infinite_log_model:append(Id, CompressedItem).


-spec sanitize_listing_opts
    (json_utils:json_map(), timestamp_agnostic) -> timestamp_agnostic_listing_opts() | no_return();
    (json_utils:json_map(), timestamp_aware) -> timestamp_aware_listing_opts() | no_return().
sanitize_listing_opts(Data, SupportedOptionsType) ->
    TimestampAgnosticOptions = #{
        <<"index">> => {binary, fun(IndexBin) ->
            try
                _ = binary_to_integer(IndexBin),
                true
            catch _:_ ->
                throw(?ERROR_BAD_DATA(<<"index">>, <<"not numerical">>))
            end
        end},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_LISTING_LIMIT}}
    },
    AllOptions = case SupportedOptionsType of
        timestamp_aware ->
            TimestampAgnosticOptions#{<<"timestamp">> => {integer, {not_lower_than, 0}}};
        timestamp_agnostic ->
            TimestampAgnosticOptions
    end,
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{optional => AllOptions}),

    #{
        start_from => infer_start_from(SanitizedData),
        offset => maps:get(<<"offset">>, SanitizedData, 0),
        limit => maps:get(<<"limit">>, SanitizedData, ?DEFAULT_LISTING_LIMIT)
    }.


-spec list_entries(id(), listing_opts(), listing_postprocessor()) ->
    {ok, {infinite_log_browser:progress_marker(), [entry()]}} | {error, term()}.
list_entries(Id, ListingOpts, ListingPostprocessor) ->
    json_infinite_log_model:list_and_postprocess(Id, ListingOpts, ListingPostprocessor).


-spec entry_to_json(entry()) -> json_utils:json_map().
entry_to_json({Index, {ok, Value}}) ->
    #{
        <<"index">> => Index,
        <<"success">> => true,
        <<"value">> => Value
    };
entry_to_json({Index, {error, _} = Error}) ->
    #{
        <<"index">> => Index,
        <<"success">> => false,
        <<"error">> => errors:to_json(Error)
    }.


%%%===================================================================
%%% Iterator API
%%%===================================================================


-spec iterator_start_index() -> index().
iterator_start_index() ->
    json_infinite_log_model:default_start_index(exclusive).


-spec iterator_get_next_batch(
    atm_store_container_iterator:batch_size(),
    id(),
    index(),
    listing_postprocessor()
) ->
    {ok, [atm_value:expanded()], index()} | stop.
iterator_get_next_batch(BatchSize, Id, LastListedIndex, ListingPostprocessor) ->
    {ok, {ProgressMarker, EntrySeries}} = json_infinite_log_model:list_and_postprocess(Id, #{
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
-spec infer_start_from(json_utils:json_map()) ->
    undefined | {index, index()} | {timestamp, timestamp_millis()}.
infer_start_from(#{<<"timestamp">> := Timestamp}) -> {timestamp, Timestamp};
infer_start_from(#{<<"index">> := Index}) -> {index, Index};
infer_start_from(_) -> undefined.

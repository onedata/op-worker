%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing aggregated statistics about transfers
%%% featuring given space and target provider.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfer_stats).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    key/2, key/3,
    get/1, get/2, get/3,
    get_last_update/1,
    update/3, update_with_cache/3,
    delete/1, delete/2
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type size() :: pos_integer().
-type space_transfer_stats() :: #space_transfer_stats{}.
-type doc() :: datastore_doc:doc(space_transfer_stats()).

-export_type([space_transfer_stats/0, doc/0]).

% Some functions from transfer_histograms module require specifying
% start time parameter. But there is no conception of start time for
% space_transfer_stats doc. So a long past value like 0 (year 1970) is used.
-define(START_TIME, 0).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats id based on specified transfer type and
%% space (provider is assumed to be the calling one).
%% @end
%%-------------------------------------------------------------------
-spec key(TransferType :: binary(), SpaceId :: od_space:id()) -> binary().
key(TransferType, SpaceId) ->
    key(oneprovider:get_id(), TransferType, SpaceId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats id based on specified provider id,
%% transfer type and space id.
%% @end
%%-------------------------------------------------------------------
-spec key(ProviderId :: od_provider:id(), TransferType :: binary(),
    SpaceId :: od_space:id()) -> binary().
key(ProviderId, TransferType, SpaceId) ->
    datastore_key:adjacent_from_digest([ProviderId, TransferType], SpaceId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats for specified transfer stats id.
%% @end
%%-------------------------------------------------------------------
-spec get(TransferStatsId :: binary()) -> doc() | {error, term()}.
get(TransferStatsId) ->
    datastore_model:get(?CTX, TransferStatsId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats for given transfer type, space and provider
%% calling this fun.
%% @end
%%-------------------------------------------------------------------
-spec get(TransferType :: binary(), SpaceId :: od_space:id()) ->
    doc() | {error, term()}.
get(TransferType, SpaceId) ->
    ?MODULE:get(key(TransferType, SpaceId)).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats for given transfer type, provider and space.
%% @end
%%-------------------------------------------------------------------
-spec get(od_provider:id(), TransferType :: binary(), od_space:id()) ->
    doc() | {error, term()}.
get(ProviderId, TransferType, SpaceId) ->
    ?MODULE:get(key(ProviderId, TransferType, SpaceId)).


-spec get_last_update(space_transfer_stats()) -> transfer_histograms:timestamp().
get_last_update(#space_transfer_stats{last_update = LastUpdateMap}) ->
    maps:fold(fun(_ProviderId, LastUpdate, Acc) ->
        max(LastUpdate, Acc)
    end, ?START_TIME, LastUpdateMap).


%%--------------------------------------------------------------------
%% @doc
%% Sends stats to onf_transfer_stats_aggregator process instead of updating doc
%% manually.
%% @end
%%--------------------------------------------------------------------
-spec update_with_cache(TransferType :: binary(), SpaceId :: od_space:id(),
    BytesPerProvider :: #{od_provider:id() => size()}) -> ok.
update_with_cache(?ON_THE_FLY_TRANSFERS_TYPE, SpaceId, BytesPerProvider) ->
    transfer_onf_stats_aggregator:update_statistics(SpaceId, BytesPerProvider).


%%--------------------------------------------------------------------
%% @doc
%% Updates space transfers stats document for given transfer type, space id
%% (provider is assumed to be the calling one) or creates it
%% if one doesn't exists already.
%% @end
%%--------------------------------------------------------------------
-spec update(TransferType :: binary(), SpaceId :: od_space:id(),
    BytesPerProvider :: #{od_provider:id() => size()}
) ->
    ok | {error, term()}.
update(TransferType, SpaceId, BytesPerProvider) ->
    Key = key(TransferType, SpaceId),
    Diff = fun(SpaceTransferStats = #space_transfer_stats{
        last_update = LastUpdates,
        min_hist = MinHistograms,
        hr_hist = HrHistograms,
        dy_hist = DyHistograms,
        mth_hist = MthHistograms
    }) ->
        CurrentMonotonicTime = transfer_histograms:get_current_monotonic_time(LastUpdates, ?START_TIME),
        ApproxCurrentTime = transfer_histograms:monotonic_timestamp_value(CurrentMonotonicTime),
        NewTimestamps = maps:map(fun(_, _) -> ApproxCurrentTime end, BytesPerProvider),
        {ok, SpaceTransferStats#space_transfer_stats{
            last_update = maps:merge(LastUpdates, NewTimestamps),
            min_hist = transfer_histograms:update(
                BytesPerProvider, MinHistograms, ?MINUTE_PERIOD,
                LastUpdates, ?START_TIME, CurrentMonotonicTime
            ),
            hr_hist = transfer_histograms:update(
                BytesPerProvider, HrHistograms, ?HOUR_PERIOD,
                LastUpdates, ?START_TIME, CurrentMonotonicTime
            ),
            dy_hist = transfer_histograms:update(
                BytesPerProvider, DyHistograms, ?DAY_PERIOD,
                LastUpdates, ?START_TIME, CurrentMonotonicTime
            ),
            mth_hist = transfer_histograms:update(
                BytesPerProvider, MthHistograms, ?MONTH_PERIOD,
                LastUpdates, ?START_TIME, CurrentMonotonicTime
            )
        }}
    end,
    CurrentTime = transfer_histograms:monotonic_timestamp_value(
        transfer_histograms:get_current_monotonic_time(?START_TIME)
    ),
    Default = #document{
        scope = SpaceId,
        key = Key,
        value = #space_transfer_stats{
            last_update = maps:map(fun(_, _) -> CurrentTime end, BytesPerProvider),
            min_hist = transfer_histograms:new(BytesPerProvider, ?MINUTE_PERIOD),
            hr_hist = transfer_histograms:new(BytesPerProvider, ?HOUR_PERIOD),
            dy_hist = transfer_histograms:new(BytesPerProvider, ?DAY_PERIOD),
            mth_hist = transfer_histograms:new(BytesPerProvider, ?MONTH_PERIOD)
        }
    },
    ?extract_ok(datastore_model:update(?CTX, Key, Diff, Default)).


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer stats document for specified transfer stats id.
%% @end
%%-------------------------------------------------------------------
-spec delete(TransferStatsId :: binary()) -> ok | {error, term()}.
delete(TransferStatsId) ->
    datastore_model:delete(?CTX, TransferStatsId).


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer stats document for given space and transfer type.
%% @end
%%-------------------------------------------------------------------
-spec delete(TransferType :: binary(), SpaceId :: od_space:id()) ->
    ok | {error, term()}.
delete(TransferType, SpaceId) ->
    delete(key(TransferType, SpaceId)).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]}.

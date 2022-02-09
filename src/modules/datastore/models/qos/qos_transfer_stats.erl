%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module is responsible for storing statistics of QoS transfers.
%%% It uses time_series_collection structure under the hood. For each QoS 
%%% entry statistics are kept in time series per storage ID along with an 
%%% additional "total" time series, which stores a sum for all storages.
%%% Time series (except "total") are created dynamically, when statistics 
%%% for such storage are gathered. 
%%% For each QoS entry there are 2 collections: 
%%%     * bytes - stores sum of transferred bytes from given storage 
%%%     * files - stores number of transferred files to given storage. 
%%%               Files do not have to be unique - 2 different transfers 
%%%               of the same file result in 2 updates of statistic.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_transfer_stats).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on document using datastore model API
-export([ensure_exists/1, delete/1, update/3]).
-export([list_time_series_ids/2, list_windows/2, list_windows/4]).

%% datastore model callbacks
-export([get_ctx/0]).

-type type() :: ?BYTES_STATS | ?FILES_STATS.
-export_type([type/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(COLLECTION_ID(QosEntryId, Type), <<QosEntryId/binary, Type/binary>>).
-define(MAX_UPDATE_RETRIES, 3).

-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API
%%%===================================================================

-spec ensure_exists(qos_entry:id()) -> ok.
ensure_exists(QosEntryId) ->
    ok = ensure_exists_internal(?COLLECTION_ID(QosEntryId, ?BYTES_STATS)),
    ok = ensure_exists_internal(?COLLECTION_ID(QosEntryId, ?FILES_STATS)).


-spec delete(qos_entry:id()) -> ok.
delete(QosEntryId) ->
    ok = datastore_time_series_collection:delete(?CTX, ?COLLECTION_ID(QosEntryId, ?BYTES_STATS)),
    ok = datastore_time_series_collection:delete(?CTX, ?COLLECTION_ID(QosEntryId, ?FILES_STATS)).


-spec list_time_series_ids(qos_entry:id(), type()) ->
    {ok, [time_series_collection:time_series_id()]} | {error, term()}.
list_time_series_ids(QosEntryId, Type) ->
    datastore_time_series_collection:list_time_series_ids(?CTX, ?COLLECTION_ID(QosEntryId, Type)).


-spec list_windows(qos_entry:id(), type()) ->
    {ok, time_series_collection:windows_map()} | {error, term()}.
list_windows(QosEntryId, Type) ->
    datastore_time_series_collection:list_windows(?CTX, ?COLLECTION_ID(QosEntryId, Type), #{}).


-spec list_windows(qos_entry:id(), type(), time_series_collection:request_range(), ts_windows:list_options()) ->
    {ok, ts_windows:descending_windows_list() | time_series_collection:windows_map()} | {error, term()}.
list_windows(QosEntryId, Type, RequestRange, Options) ->
    datastore_time_series_collection:list_windows(?CTX, ?COLLECTION_ID(QosEntryId, Type), RequestRange, Options).


-spec update(qos_entry:id(), type(), #{od_storage:id() => non_neg_integer()}) -> 
    ok | {error, term()}.
update(QosEntryId, Type, ValuesPerStorage) ->
    TotalValue = maps:fold(fun(_Key, Value, Acc) ->
        Acc + Value
    end, 0, ValuesPerStorage),
    update_internal(?COLLECTION_ID(QosEntryId, Type), ValuesPerStorage#{?TOTAL_TIME_SERIES_ID => TotalValue},
        ?MAX_UPDATE_RETRIES).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec ensure_exists_internal(time_series_collection:collection_id()) -> ok | {error, term()}.
ensure_exists_internal(CollectionId) ->
    Config = #{?TOTAL_TIME_SERIES_ID => supported_metrics()},
    case datastore_time_series_collection:create(?CTX, CollectionId, Config) of
        ok -> ok;
        {error, collection_already_exists} -> ok;
        Error -> Error
    end.


-spec update_internal(time_series_collection:collection_id(), 
    #{od_storage:id() | binary() => non_neg_integer()}, non_neg_integer()) -> ok.
update_internal(CollectionId, _ValuesPerStorage, 0) ->
    ?warning(
        "Could not update QoS transfer statistics in collection ~p due to 
        exceeded number of retries", [CollectionId]
    );
update_internal(CollectionId, ValuesPerStorage, Retries) ->
    case datastore_time_series_collection:check_and_update(?CTX, CollectionId, ?NOW(), maps:to_list(ValuesPerStorage)) of
        ok -> 
            ok;
        ?ERROR_NOT_FOUND ->
            % There is a chance that transfer started for legacy QoS entry for which time 
            % series collection was not initialized. Create it and try again.
            ok = ensure_exists_internal(CollectionId),
            update_internal(CollectionId, ValuesPerStorage, Retries - 1);
        {error, time_series_not_found} ->
            {ok, ExistingTimeSeries} = datastore_time_series_collection:list_time_series_ids(
                ?CTX, CollectionId),
            MissingTimeSeries = lists_utils:subtract(maps:keys(ValuesPerStorage), ExistingTimeSeries),
            ok = create_missing_time_series(CollectionId, MissingTimeSeries),
            update_internal(CollectionId, ValuesPerStorage, Retries - 1)
    end.


-spec supported_metrics() -> #{ts_metric:id() => ts_metric:config()}.
supported_metrics() -> #{
    ?MINUTE_METRIC_ID => #metric_config{
        resolution = ?MINUTE_RESOLUTION,
        retention = 120,
        aggregator = sum
    },
    ?HOUR_METRIC_ID => #metric_config{
        resolution = ?HOUR_RESOLUTION,
        retention = 48,
        aggregator = sum
    },
    ?DAY_METRIC_ID => #metric_config{
        resolution = ?DAY_RESOLUTION,
        retention = 60,
        aggregator = sum
    },
    ?MONTH_METRIC_ID => #metric_config{
        resolution = ?MONTH_RESOLUTION,
        retention = 12,
        aggregator = sum
    }
}.


-spec create_missing_time_series(time_series_collection:collection_id(), [od_provider:id()]) -> 
    ok | {error, term()}.
create_missing_time_series(_CollectionId, []) -> 
    ok;
create_missing_time_series(CollectionId, MissingTimeSeries) -> 
    Configs = lists:foldl(fun(TimeSeriesId, Acc) ->
        Acc#{TimeSeriesId => supported_metrics()}
    end, #{}, MissingTimeSeries),
    case datastore_time_series_collection:add_metrics(?CTX, CollectionId, Configs, #{}) of
        ok -> ok;
        {error, time_series_already_exists} -> ok;
        {error, metric_already_exists} -> ok;
        Error -> Error
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

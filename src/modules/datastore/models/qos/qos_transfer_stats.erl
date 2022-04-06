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

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on document using datastore model API
-export([ensure_exists/1, delete/1, report/3]).
-export([get_layout/2, get_slice/4]).

%% datastore model callbacks
-export([get_ctx/0]).

-type type() :: binary().  % ?BYTES_STATS | ?FILES_STATS
-export_type([type/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(COLLECTION_ID(QosEntryId, Type), <<QosEntryId/binary, Type/binary>>).
-define(MAX_CONSUME_MEASUREMENTS_RETRIES, 3).

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


-spec get_layout(qos_entry:id(), type()) ->
    {ok, time_series_collection:layout()} | {error, term()}.
get_layout(QosEntryId, Type) ->
    datastore_time_series_collection:get_layout(?CTX, ?COLLECTION_ID(QosEntryId, Type)).


-spec get_slice(qos_entry:id(), type(), time_series_collection:layout(), ts_windows:list_options()) ->
    {ok, time_series_collection:slice()} | {error, term()}.
get_slice(QosEntryId, Type, SliceLayout, ListWindowsOptions) ->
    datastore_time_series_collection:get_slice(?CTX, ?COLLECTION_ID(QosEntryId, Type), SliceLayout, ListWindowsOptions).


-spec report(qos_entry:id(), type(), #{od_storage:id() => non_neg_integer()}) ->
    ok | {error, term()}.
report(QosEntryId, Type, ValuesPerStorage) ->
    TotalValue = maps:fold(fun(_Key, Value, Acc) ->
        Acc + Value
    end, 0, ValuesPerStorage),
    CompleteValuesPerStorage = ValuesPerStorage#{?QOS_TOTAL_TIME_SERIES_NAME => TotalValue},
    Timestamp = ?NOW(),
    ConsumeSpec = maps:map(fun(_TimeSeriesName, Value) -> #{all => [{Timestamp, Value}]} end, CompleteValuesPerStorage),
    consume_measurements(?COLLECTION_ID(QosEntryId, Type), ConsumeSpec, ?MAX_CONSUME_MEASUREMENTS_RETRIES).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec ensure_exists_internal(time_series_collection:id()) -> ok | {error, term()}.
ensure_exists_internal(CollectionId) ->
    Config = config_with_time_series([?QOS_TOTAL_TIME_SERIES_NAME]),
    case datastore_time_series_collection:create(?CTX, CollectionId, Config) of
        ok -> ok;
        {error, already_exists} -> ok;
        Error -> Error
    end.


-spec consume_measurements(time_series_collection:id(), time_series_collection:consume_spec(), non_neg_integer()) ->
    ok.
consume_measurements(CollectionId, _ConsumeSpec, 0) ->
    ?warning(
        "Could not update QoS transfer statistics in collection ~p due to 
        exceeded number of retries", [CollectionId]
    );
consume_measurements(CollectionId, ConsumeSpec, Retries) ->
    case datastore_time_series_collection:consume_measurements(?CTX, CollectionId, ConsumeSpec) of
        ok ->
            ok;
        ?ERROR_NOT_FOUND ->
            % There is a chance that transfer started for legacy QoS entry for which time 
            % series collection was not initialized. Create it and try again.
            ok = ensure_exists_internal(CollectionId),
            consume_measurements(CollectionId, ConsumeSpec, Retries - 1);
        ?ERROR_TSC_MISSING_LAYOUT(MissingLayout) ->
            MissingConfig = config_with_time_series(maps:keys(MissingLayout)),
            ok = datastore_time_series_collection:incorporate_config(?CTX, CollectionId, MissingConfig),
            consume_measurements(CollectionId, ConsumeSpec, Retries - 1)
    end.


-spec supported_metrics() -> time_series:metric_composition().
supported_metrics() -> #{
    ?QOS_MINUTE_METRIC_NAME => #metric_config{
        resolution = ?MINUTE_RESOLUTION,
        retention = 120,
        aggregator = sum
    },
    ?QOS_HOUR_METRIC_NAME => #metric_config{
        resolution = ?HOUR_RESOLUTION,
        retention = 48,
        aggregator = sum
    },
    ?QOS_DAY_METRIC_NAME => #metric_config{
        resolution = ?DAY_RESOLUTION,
        retention = 60,
        aggregator = sum
    },
    ?QOS_MONTH_METRIC_NAME => #metric_config{
        resolution = ?MONTH_RESOLUTION,
        retention = 12,
        aggregator = sum
    }
}.


%% @private
-spec config_with_time_series([time_series_collection:time_series_name()]) -> time_series_collection:config().
config_with_time_series(TimeSeriesNames) ->
    maps_utils:generate_from_list(fun(TimeSeriesName) ->
        {TimeSeriesName, supported_metrics()}
    end, TimeSeriesNames).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

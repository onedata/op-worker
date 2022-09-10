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
-export([ensure_exists/1, delete/1]).
-export([get_collection_schema/1]).
-export([browse/3]).
-export([report/3]).

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


-spec get_collection_schema(type()) -> time_series_collection_schema:record().
get_collection_schema(?BYTES_STATS) ->
    ?QOS_BYTES_STATS_COLLECTION_SCHEMA;
get_collection_schema(?FILES_STATS) ->
    ?QOS_FILES_STATS_COLLECTION_SCHEMA.


-spec browse(qos_entry:id(), type(), ts_browse_request:record()) ->
    {ok, ts_browse_result:record()} | {error, term()}.
browse(QosEntryId, Type, TsBrowseRequest) ->
    datastore_time_series_collection:browse(?CTX, ?COLLECTION_ID(QosEntryId, Type), TsBrowseRequest).


-spec report(qos_entry:id(), type(), #{od_storage:id() => non_neg_integer()}) ->
    ok | {error, term()}.
report(QosEntryId, Type, ValuesPerStorage) ->
    Timestamp = ?NOW(),
    ConsumeSpecWithoutTotal = maps_utils:map_key_value(fun(StorageId, Value) ->
        {?QOS_STORAGE_TIME_SERIES_NAME(StorageId), #{?ALL_METRICS => [{Timestamp, Value}]}}
    end, ValuesPerStorage),

    TotalValue = maps:fold(fun(_Key, Value, Acc) ->
        Acc + Value
    end, 0, ValuesPerStorage),
    ConsumeSpec = ConsumeSpecWithoutTotal#{
        ?QOS_TOTAL_TIME_SERIES_NAME => #{
            ?ALL_METRICS => [{Timestamp, TotalValue}]
        }
    },
    consume_measurements(?COLLECTION_ID(QosEntryId, Type), ConsumeSpec, ?MAX_CONSUME_MEASUREMENTS_RETRIES).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec ensure_exists_internal(time_series_collection:id()) -> ok | {error, term()}.
ensure_exists_internal(CollectionId) ->
    Config = config_with_time_series([?QOS_TOTAL_TIME_SERIES_NAME]),
    case datastore_time_series_collection:create(?CTX, CollectionId, Config) of
        ok -> ok;
        {error, already_exists} -> ok;
        Error -> Error
    end.


%% @private
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


%% @private
-spec config_with_time_series([time_series:name()]) -> time_series_collection:config().
config_with_time_series(TimeSeriesNames) ->
    maps_utils:generate_from_list(fun(TimeSeriesName) ->
        {TimeSeriesName, ?QOS_STATS_METRICS}
    end, TimeSeriesNames).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

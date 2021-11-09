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
%%%               Files can not be unique, therefore 2 different transfers 
%%%               of the same file result in 2 updates of statistic.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_transfer_stats).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/ts_metric_config.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on document using datastore model API
-export([create/1, delete/1, get/2, update/3]).

%% datastore model callbacks
-export([get_ctx/0]).

-type type() :: bytes | files.
-export_type([type/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(COLLECTION_ID(QosEntryId, Type), <<QosEntryId/binary, (atom_to_binary(Type))/binary>>).
-define(MAX_UPDATE_RETRIES, 3).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(qos_entry:id()) -> ok.
create(QosEntryId) ->
    ok = create_internal(?COLLECTION_ID(QosEntryId, bytes)),
    ok = create_internal(?COLLECTION_ID(QosEntryId, files)).


-spec delete(qos_entry:id()) -> ok.
delete(QosEntryId) ->
    ok = datastore_time_series_collection:delete(?CTX, ?COLLECTION_ID(QosEntryId, bytes)),
    ok = datastore_time_series_collection:delete(?CTX, ?COLLECTION_ID(QosEntryId, files)).


-spec get(qos_entry:id(), type()) -> time_series_collection:windows_map() | {error, term()}.
get(QosEntryId, Type) ->
    datastore_time_series_collection:list_windows(?CTX, ?COLLECTION_ID(QosEntryId, Type), #{}).


-spec update(qos_entry:id(), type(), #{od_storage:id() => non_neg_integer()}) -> 
    ok | {error, term()}.
update(QosEntryId, Type, ValuesPerStorage) ->
    TotalValue = maps:fold(fun(_Key, Value, Acc) ->
        Acc + Value
    end, 0, ValuesPerStorage),
    update_internal(?COLLECTION_ID(QosEntryId, Type), ValuesPerStorage#{<<"total">> => TotalValue}, 
        ?MAX_UPDATE_RETRIES).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_internal(time_series_collection:collection_id()) -> ok | {error, term()}.
create_internal(CollectionId) ->
    Config = #{<<"total">> => supported_metrics()},
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
    case datastore_time_series_collection:check_and_update(?CTX, CollectionId, 
        global_clock:timestamp_millis(), maps:to_list(ValuesPerStorage)) 
    of
        ok -> 
            ok;
        {error, time_series_not_found} ->
            {ok, ExistingTimeSeries} = list_time_series_internal(CollectionId),
            MissingTimeSeries = lists_utils:subtract(maps:keys(ValuesPerStorage), ExistingTimeSeries),
            ok = create_missing_time_series(CollectionId, MissingTimeSeries),
            update_internal(CollectionId, ValuesPerStorage, Retries - 1)
    end.


-spec list_time_series_internal(time_series_collection:collection_id()) -> 
    {ok, [time_series_collection:time_series_id()]} | {error, term()}.
list_time_series_internal(CollectionId) ->
    case datastore_time_series_collection:list_time_series_ids(?CTX, CollectionId) of
        {ok, TimeSeries} -> 
            {ok, TimeSeries};
        ?ERROR_NOT_FOUND ->
            % There is a chance that transfer started for legacy QoS entry for which time 
            % series collection was not initialized. Create it and try again.
            ok = create_internal(CollectionId),
            datastore_time_series_collection:list_time_series_ids(?CTX, CollectionId)
    end.


-spec supported_metrics() -> #{ts_metric:id() => ts_metric:config()}.
supported_metrics() -> #{
    <<"minute">> => #metric_config{
        resolution = timer:minutes(1),
        retention = 120,
        aggregator = sum
    },
    <<"hour">> => #metric_config{
        resolution = timer:hours(1),
        retention = 48,
        aggregator = sum
    },
    <<"day">> => #metric_config{
        resolution = timer:hours(24),
        retention = 60,
        aggregator = sum
    },
    <<"month">> => #metric_config{
        resolution = timer:hours(24 * 30),
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

%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about archive recalls progress.
%%% It uses `time_series_collection` structure for storing statistics.
%%% For each recall there are 3 time series:
%%%     * currentBytes - stores sum of copied bytes
%%%     * currentFiles - stores number of copied files 
%%%     * failedFiles - stores number of unsuccessfully recalled files
%%% Recall statistics are only kept locally on provider that is 
%%% performing recall. 
%%% `json_infinite_log_model` structure is used for storing information about encountered errors.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall_progress).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").
-include_lib("cluster_worker/include/modules/datastore/ts_metric_config.hrl").


%% API
-export([create/1, delete/1]).
-export([get/1]).
-export([report_bytes_copied/2, report_file_finished/1, report_file_failed/3]).
%% Test API
-export([get_stats/1]).
%% Datastore callbacks
-export([get_ctx/0]).

-type id() :: archive_recall:id().

% Map in following format: 
%  #{
%       <<"filesCopied" := non_neg_integer(),
%       <<"bytesCopied" := non_neg_integer(),
%       <<"filesFailed" := non_neg_integer(),
%       <<"lastError" := undefined | #{
%           <<"fileId">> := file_id:objectid(), 
%           <<"reason">> := errors:as_json()
%       }
%   }
-type recall_progress_map() :: #{binary() => non_neg_integer() | map() | undefined}.
-export_type([recall_progress_map/0]).


-define(CTX, #{
    model => ?MODULE
}).

-define(BYTES_TS, <<"bytesCopied">>).
-define(FILES_TS, <<"filesCopied">>).
-define(FAILED_FILES_TS, <<"filesFailed">>).
-define(TSC_ID(Id), <<Id/binary, "tsc">>).

-define(ERROR_LOG_ID(Id), <<Id/binary, "el">>).

-define(TOTAL_METRIC, <<"total">>).
-define(MINUTE_METRIC, <<"minute">>).
-define(HOUR_METRIC, <<"hour">>).
-define(DAY_METRIC, <<"day">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(id()) -> ok | {error, term()}.
create(Id) ->
    try
        ok = json_infinite_log_model:create(?ERROR_LOG_ID(Id), #{}),
        ok = create_tsc(Id)
    catch _:{badmatch, Error} ->
        delete(Id),
        Error
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    datastore_time_series_collection:delete(?CTX, ?TSC_ID(Id)),
    json_infinite_log_model:destroy(?ERROR_LOG_ID(Id)).


-spec get(id()) -> {ok, recall_progress_map()}.
get(Id) ->
    {ok, CountersCurrentValue} = get_counters_current_value(Id),
    case CountersCurrentValue of
        #{?FAILED_FILES_TS := 0} -> 
            {ok, CountersCurrentValue#{<<"lastError">> => undefined}};
        _ ->
            %% @TODO VFS-8839 - browse error log when gui supports it
            {ok, #{
                <<"logEntries">> := [#{
                    <<"content">> := LastEntryContent
                }]
            }} = json_infinite_log_model:browse_content(?ERROR_LOG_ID(Id), 
                #{limit => 1, direction => ?BACKWARD}),
            {ok, CountersCurrentValue#{<<"lastError">> => LastEntryContent}}
    end.


-spec report_file_finished(id()) -> ok | {error, term()}.
report_file_finished(Id) ->
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?FILES_TS, 1}
    ]).


-spec report_file_failed(id(), file_id:file_guid(), {error, term()}) -> ok | {error, term()}.
report_file_failed(Id, FileGuid, Error) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    json_infinite_log_model:append(?ERROR_LOG_ID(Id), #{
        <<"fileId">> => ObjectId,
        <<"reason">> => errors:to_json(Error)
    }),
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?FAILED_FILES_TS, 1}
    ]).


-spec report_bytes_copied(id(), non_neg_integer()) -> ok | {error, term()}.
report_bytes_copied(Id, Bytes) ->
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?BYTES_TS, Bytes}
    ]).

%%%===================================================================
%%% Test API 
%%%===================================================================

-spec get_stats(id()) -> time_series_collection:windows_map() | {error, term()}.
get_stats(Id) ->
    datastore_time_series_collection:list_windows(?CTX, ?TSC_ID(Id), #{}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create_tsc(id()) -> ok | {error, term()}.
create_tsc(Id) ->
    TotalMetric = #{
        ?TOTAL_METRIC => #metric_config{
            resolution = 0,
            retention = 1,
            aggregator = sum
        }
    },
    Metrics = maps:merge(TotalMetric, supported_metrics()),
    Config = #{
        ?BYTES_TS => Metrics,
        ?FILES_TS => Metrics,
        ?FAILED_FILES_TS => TotalMetric
    },
    case datastore_time_series_collection:create(?CTX, ?TSC_ID(Id), Config) of
        ok -> ok;
        {error, collection_already_exists} -> ok;
        Error -> Error
    end.


%% @private
-spec supported_metrics() -> #{ts_metric:id() => ts_metric:config()}.
supported_metrics() -> #{
    ?MINUTE_METRIC => #metric_config{
        resolution = timer:minutes(1),
        retention = 120,
        aggregator = sum
    },
    ?HOUR_METRIC => #metric_config{
        resolution = timer:hours(1),
        retention = 48,
        aggregator = sum
    },
    ?DAY_METRIC => #metric_config{
        resolution = timer:hours(24),
        retention = 60,
        aggregator = sum
    }
}.


%% @private
-spec get_counters_current_value(id()) -> {ok, map()}.
get_counters_current_value(Id) ->
    RequestRange = lists:map(fun(Stat) -> {Stat, ?TOTAL_METRIC} end, 
        [?BYTES_TS, ?FILES_TS, ?FAILED_FILES_TS]),
    
    case datastore_time_series_collection:list_windows(?CTX, ?TSC_ID(Id), RequestRange, #{limit => 1}) of
        {ok, WindowsMap} ->
            WindowToValue = fun
                ({{Stat, ?TOTAL_METRIC}, [{_Timestamp, {_Measurements, Value}}]}) -> 
                    {Stat, Value};
                ({{Stat, ?TOTAL_METRIC}, []}) -> 
                    {Stat, 0}
            end,
            {ok, maps:from_list(lists:map(WindowToValue, maps:to_list(WindowsMap)))};
        Error ->
            Error
    end.


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

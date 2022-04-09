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
-include_lib("ctool/include/time_series/common.hrl").


%% API
-export([create/1, delete/1]).
-export([get/1]).
-export([report_bytes_copied/2, report_file_finished/1, report_file_failed/3]).
%% Test API
-export([get_stats/3]).
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

-define(TSC_ID(Id), <<Id/binary, "tsc">>).

-define(BYTES_TS, <<"bytesCopied">>).
-define(FILES_TS, <<"filesCopied">>).
-define(FAILED_FILES_TS, <<"filesFailed">>).

-define(ERROR_LOG_ID(Id), <<Id/binary, "el">>).

-define(TOTAL_METRIC, <<"total">>).
-define(MINUTE_METRIC, <<"minute">>).
-define(HOUR_METRIC, <<"hour">>).
-define(DAY_METRIC, <<"day">>).

-define(NOW(), global_clock:timestamp_seconds()).

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
    datastore_time_series_collection:consume_measurements(?CTX, ?TSC_ID(Id), #{
        ?FILES_TS => #{?ALL_METRICS => [{?NOW(), 1}]}}
    ).


-spec report_file_failed(id(), file_id:file_guid(), {error, term()}) -> ok | {error, term()}.
report_file_failed(Id, FileGuid, Error) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    json_infinite_log_model:append(?ERROR_LOG_ID(Id), #{
        <<"fileId">> => ObjectId,
        <<"reason">> => errors:to_json(Error)
    }),
    datastore_time_series_collection:consume_measurements(?CTX, ?TSC_ID(Id), #{
        ?FAILED_FILES_TS => #{?ALL_METRICS => [{?NOW(), 1}]}}
    ).


-spec report_bytes_copied(id(), non_neg_integer()) -> ok | {error, term()}.
report_bytes_copied(Id, Bytes) ->
    datastore_time_series_collection:consume_measurements(?CTX, ?TSC_ID(Id), #{
        ?BYTES_TS => #{?ALL_METRICS => [{?NOW(), Bytes}]}}
    ).

%%%===================================================================
%%% Test API 
%%%===================================================================

-spec get_stats(id(), time_series_collection:layout(), ts_windows:list_options()) ->
    {ok, time_series_collection:slice()} | {error, term()}.
get_stats(Id, SliceLayout, ListWindowsOptions) ->
    datastore_time_series_collection:get_slice(?CTX, ?TSC_ID(Id), SliceLayout, ListWindowsOptions).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create_tsc(id()) -> ok | {error, term()}.
create_tsc(Id) ->
    TotalMetric = #{
        ?TOTAL_METRIC => #metric_config{
            resolution = ?INFINITY_RESOLUTION,
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
        {error, already_exists} -> ok;
        Error -> Error
    end.


%% @private
-spec supported_metrics() -> time_series:metric_composition().
supported_metrics() -> #{
    ?MINUTE_METRIC => #metric_config{
        resolution = ?MINUTE_RESOLUTION,
        retention = 120,
        aggregator = sum
    },
    ?HOUR_METRIC => #metric_config{
        resolution = ?HOUR_RESOLUTION,
        retention = 48,
        aggregator = sum
    },
    ?DAY_METRIC => #metric_config{
        resolution = ?DAY_RESOLUTION,
        retention = 60,
        aggregator = sum
    }
}.


%% @private
-spec get_counters_current_value(id()) ->
    {ok, #{time_series_collection:time_series_name() => non_neg_integer()}} | {error, term()}.
get_counters_current_value(Id) ->
    case datastore_time_series_collection:get_slice(
        ?CTX, ?TSC_ID(Id), #{?ALL_TIME_SERIES => [?TOTAL_METRIC]}, #{window_limit => 1}
    ) of
        {ok, Slice} ->
            {ok, maps:map(fun(_TimeSeriesName, #{?TOTAL_METRIC := Windows}) ->
                case Windows of
                    [{_Timestamp, {_Count, Value}}] ->
                        Value;
                    [] ->
                        0
                end
            end, Slice)};
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

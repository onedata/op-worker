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
-export([get/1, browse_error_log/2]).
-export([report_bytes_copied/2, report_file_finished/1, report_error/2]).
%% Test API
-export([get_stats/3]).
%% Datastore callbacks
-export([get_ctx/0]).

-type id() :: archive_recall:id().

% Map in following format: 
%  #{
%       <<"filesCopied" := non_neg_integer(),
%       <<"bytesCopied" := non_neg_integer(),
%       <<"filesFailed" := non_neg_integer()
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

-define(LOG_MAX_SIZE, op_worker:get_env(archive_recall_error_log_max_size, 10000)).
-define(LOG_EXPIRATION, op_worker:get_env(archive_recall_error_log_expiration_seconds, 1209600)). % 14 days

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(id()) -> ok | {error, term()}.
create(Id) ->
    try
        ok = json_infinite_log_model:create(?ERROR_LOG_ID(Id), #{
            
        }),
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
    get_counters_current_value(Id).


-spec browse_error_log(id(), json_infinite_log_model:listing_opts()) ->
    {ok, json_infinite_log_model:browse_result()} | {error, term()}.
browse_error_log(Id, Options) ->
    json_infinite_log_model:browse_content(?ERROR_LOG_ID(Id), Options).


-spec report_file_finished(id()) -> ok | {error, term()}.
report_file_finished(Id) ->
    datastore_time_series_collection:consume_measurements(?CTX, ?TSC_ID(Id), #{
        ?FILES_TS => #{?ALL_METRICS => [{?NOW(), 1}]}}
    ).


-spec report_error(id(), json_utils:json_term()) -> ok | {error, term()}.
report_error(Id, ErrorJson) ->
    json_infinite_log_model:append(?ERROR_LOG_ID(Id), ErrorJson),
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

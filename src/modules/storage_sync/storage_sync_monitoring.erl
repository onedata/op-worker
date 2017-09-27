%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for reporting metrics
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_monitoring).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").


%% reporters API
-export([start_lager_reporter/0, delete_lager_reporter/0, start_ets_reporter/0,
    delete_ets_reporter/0, ensure_reporters_started/1]).

%% counters API
-export([start_imported_files_counter/1, increase_imported_files_counter/1,
    get_imported_files_value/1, stop_imported_files_counter/1,
    start_files_to_import_counter/1, update_files_to_import_counter/2,
    get_files_to_import_value/1, stop_files_to_import_counter/1,
    start_files_to_update_counter/1, get_files_to_update_value/1,
    update_files_to_update_counter/2, stop_files_to_update_counter/1,
    update_to_do_counter/3
]).

-export([update_in_progress/1, get_metric/3, ensure_all_metrics_stopped/1,
    import_state/1]).

-export([start_imported_files_spirals/1, increase_imported_files_spirals/1,
    stop_imported_files_spirals/1, start_updated_files_spirals/1,
    increase_updated_files_spirals/1, stop_updated_files_spirals/1,
    start_deleted_files_spirals/1, increase_deleted_files_spirals/1,
    stop_deleted_files_spirals/1, start_queue_length_spirals/1,
    update_queue_length_spirals/2, stop_queue_length_spirals/1
]).


-type window() :: day | hours | minute.
-type counter_type() :: imported_files | files_to_import | files_to_update.
-type spiral_type() :: imported_files | updated_files | deleted_files | queue_length.
-type error() :: {error, term()}.

-define(STORAGE_SYNC_METRIC_PREFIX, storaage_sync).

-define(COUNTER_NAME(SpaceId, Type), [
    ?STORAGE_SYNC_METRIC_PREFIX, counter, SpaceId, Type
]).

-define(SPIRAL_NAME(SpaceId, Type, Window), [
    ?STORAGE_SYNC_METRIC_PREFIX, spiral, SpaceId, Type, Window
]).

-define(COUNTER_LOGGING_INTERVAL, timer:seconds(30)).
-define(SPIRAL_RESOLUTION, application:get_env(?APP_NAME, storage_sync_histogram_length, 12)).


-define(LAGER_REPORTER_NAME, exometer_report_lager).
-define(ETS_REPORTER_NAME, exometer_report_rrd_ets).

%%metric types
-define(IMPORTED_FILES, imported_files).
-define(DELETED_FILES, deleted_files).
-define(UPDATED_FILES, updated_files).

-define(FILES_TO_IMPORT, files_to_import).
-define(FILES_TO_UPDATE, files_to_update).
-define(QUEUE_LENGTH, queue_length).

-define(COUNTER_TYPES, [
    ?IMPORTED_FILES, ?FILES_TO_IMPORT, ?FILES_TO_UPDATE
]).

-define(SPIRAL_TYPES, [
    ?IMPORTED_FILES, ?DELETED_FILES, ?UPDATED_FILES, ?QUEUE_LENGTH
]).


-define(WINDOWS, [
   minute, hour, day
]).

-define(LOG_LEVEL, critical).

%%-------------------------------------------------------------------
%% @doc
%% Starts space_sync_monitoring lager reporter
%% @end
%%-------------------------------------------------------------------
-spec start_lager_reporter() -> ok | error().
start_lager_reporter() ->
    ok = exometer_report:add_reporter(?LAGER_REPORTER_NAME, [
        {type_map,[{'_',integer}]},
        {level, ?LOG_LEVEL}
    ]).

%%-------------------------------------------------------------------
%% @doc
%% Starts space_sync_monitoring ets reporter.
%% @end
%%-------------------------------------------------------------------
-spec start_ets_reporter() -> ok | error().
start_ets_reporter() ->
    ok = exometer_report:add_reporter(?ETS_REPORTER_NAME, []).

%%-------------------------------------------------------------------
%% @doc
%% Deletes space_sync_monitoring_reporter
%% @end
%%-------------------------------------------------------------------
-spec delete_lager_reporter() -> ok | error().
delete_lager_reporter() ->
    exometer_report:remove_reporter(?LAGER_REPORTER_NAME).

%%-------------------------------------------------------------------
%% @doc
%% Deletes space_sync_monitoring_reporter
%% @end
%%-------------------------------------------------------------------
delete_ets_reporter() ->
    exometer_report:remove_reporter(?ETS_REPORTER_NAME).

%%-------------------------------------------------------------------
%% @doc
%% Starts counter of imported files.
%% @end
%%-------------------------------------------------------------------
-spec start_imported_files_counter(od_space:id()) -> ok.
start_imported_files_counter(SpaceId) ->
    start_and_subscribe_storage_sync_counter(SpaceId, ?IMPORTED_FILES).

%%-------------------------------------------------------------------
%% @doc
%% Starts counter of files to be imported
%% @end
%%-------------------------------------------------------------------
-spec start_files_to_import_counter(od_space:id()) -> ok.
start_files_to_import_counter(SpaceId) ->
    start_and_subscribe_storage_sync_counter(SpaceId, ?FILES_TO_IMPORT).

%%-------------------------------------------------------------------
%% @doc
%% Starts counter of files to be imported
%% @end
%%-------------------------------------------------------------------
-spec start_files_to_update_counter(od_space:id()) -> ok.
start_files_to_update_counter(SpaceId) ->
    start_and_subscribe_storage_sync_counter(SpaceId, ?FILES_TO_UPDATE).

%%-------------------------------------------------------------------
%% @doc
%% Starts exometer_spiral to monitor number of imported files.
%% @end
%%-------------------------------------------------------------------
-spec start_queue_length_spirals(od_space:id()) -> ok.
start_queue_length_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        start_queue_length_spiral(SpaceId, Window, ?SPIRAL_RESOLUTION)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Starts exometer_spiral to monitor number of imported files.
%% @end
%%-------------------------------------------------------------------
-spec start_imported_files_spirals(od_space:id()) -> ok.
start_imported_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        start_imported_files_spiral(SpaceId, Window, ?SPIRAL_RESOLUTION)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Starts exometer_spiral to monitor number of imported files.
%% @end
%%-------------------------------------------------------------------
-spec start_deleted_files_spirals(od_space:id()) -> ok.
start_deleted_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        start_deleted_files_spiral(SpaceId, Window, ?SPIRAL_RESOLUTION)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Starts exometer_spiral to monitor number of imported files.
%% @end
%%-------------------------------------------------------------------
-spec start_updated_files_spirals(od_space:id()) -> ok.
start_updated_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        start_updated_files_spiral(SpaceId, Window, ?SPIRAL_RESOLUTION)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of imported files
%% @end
%%-------------------------------------------------------------------
-spec stop_imported_files_counter(od_space:id()) -> ok | {error, term()}.
stop_imported_files_counter(SpaceId) ->
    stop_and_unsubscribe_storage_sync_counter(SpaceId, ?IMPORTED_FILES).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of files to be imported
%% @end
%%-------------------------------------------------------------------
-spec stop_files_to_import_counter(od_space:id()) -> ok | {error, term()}.
stop_files_to_import_counter(SpaceId) ->
    stop_and_unsubscribe_storage_sync_counter(SpaceId, ?FILES_TO_IMPORT).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of files to be updated
%% @end
%%-------------------------------------------------------------------
-spec stop_files_to_update_counter(od_space:id()) ->
    ok | {error, term()}.
stop_files_to_update_counter(SpaceId) ->
    stop_and_unsubscribe_storage_sync_counter(SpaceId, ?FILES_TO_UPDATE).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of files to be updated
%% @end
%%-------------------------------------------------------------------
-spec stop_imported_files_spirals(od_space:id()) -> ok.
stop_imported_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        stop_imported_files_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of files to be updated
%% @end
%%-------------------------------------------------------------------
-spec stop_updated_files_spirals(od_space:id()) -> ok.
stop_updated_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        stop_updated_files_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of files to be deleted
%% @end
%%-------------------------------------------------------------------
-spec stop_deleted_files_spirals(od_space:id()) -> ok.
stop_deleted_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        stop_deleted_files_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Stops counter of files to be deleted
%% @end
%%-------------------------------------------------------------------
-spec stop_queue_length_spirals(od_space:id()) -> ok.
stop_queue_length_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        stop_queue_length_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of imported files
%% @end
%%-------------------------------------------------------------------
-spec increase_imported_files_counter(od_space:id()) ->
        ok | {error, term()}.
increase_imported_files_counter(SpaceId) ->
    update_counter(SpaceId, ?IMPORTED_FILES, 1).

%%-------------------------------------------------------------------
%% @doc
%% Updates counter of files to be imported with given Value.
%% Value can be negative.
%% @end
%%-------------------------------------------------------------------
-spec update_to_do_counter(od_space:id(),
    space_strategy:type(), integer()) -> ok | error().
update_to_do_counter(SpaceId, storage_import, Value) ->
    update_files_to_import_counter(SpaceId, Value);
update_to_do_counter(SpaceId, storage_update, Value) ->
    update_files_to_update_counter(SpaceId, Value).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of imported files
%% @end
%%-------------------------------------------------------------------
-spec increase_imported_files_spirals(od_space:id()) ->
    ok | {error, term()}.
increase_imported_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        increase_imported_files_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of deleted files
%% @end
%%-------------------------------------------------------------------
-spec increase_deleted_files_spirals(od_space:id()) ->
    ok | {error, term()}.
increase_deleted_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        increase_deleted_files_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of updated files
%% @end
%%-------------------------------------------------------------------
-spec increase_updated_files_spirals(od_space:id()) ->
    ok | {error, term()}.
increase_updated_files_spirals(SpaceId) ->
    lists:foreach(fun(Window) ->
        increase_updated_files_spiral(SpaceId, Window)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Updates jobs queue length spiral for each Window.
%% @end
%%-------------------------------------------------------------------
-spec update_queue_length_spirals(od_space:id(),
    non_neg_integer()) -> ok | {error, term()}.
update_queue_length_spirals(SpaceId, Value) ->
    lists:foreach(fun(Window) ->
        update_queue_length_spiral(SpaceId, Window, Value)
    end, ?WINDOWS).

%%-------------------------------------------------------------------
%% @doc
%% Updates counter of files to be imported with given Value.
%% Value can be negative.
%% @end
%%-------------------------------------------------------------------
-spec update_files_to_import_counter(od_space:id(), integer()) -> ok | error().
update_files_to_import_counter(SpaceId, Value) ->
    update_counter(SpaceId, ?FILES_TO_IMPORT, Value).

%%-------------------------------------------------------------------
%% @doc
%% Updates counter of files to be updated with given Value.
%% Value can be negative.
%% @end
%%-------------------------------------------------------------------
-spec update_files_to_update_counter(od_space:id(), integer()) -> ok | error().
update_files_to_update_counter(SpaceId, Value) ->
    update_counter(SpaceId, ?FILES_TO_UPDATE, Value).

%%-------------------------------------------------------------------
%% @doc
%% Returns values of files to be imported counter
%% @end
%%-------------------------------------------------------------------
-spec get_files_to_import_value(od_space:id()) -> integer().
get_files_to_import_value(SpaceId) ->
    get_value(SpaceId, ?FILES_TO_IMPORT).

%%-------------------------------------------------------------------
%% @doc
%% Returns values of imported files counter
%% @end
%%-------------------------------------------------------------------
-spec get_imported_files_value(od_space:id()) -> integer().
get_imported_files_value(SpaceId) ->
    get_value(SpaceId, ?IMPORTED_FILES).


%%-------------------------------------------------------------------
%% @doc
%% Returns values of imported files counter
%% @end
%%-------------------------------------------------------------------
-spec get_files_to_update_value(od_space:id()) -> integer().
get_files_to_update_value(SpaceId) ->
    get_value(SpaceId, ?FILES_TO_UPDATE).

%%-------------------------------------------------------------------
%% @doc
%% Returns state of import. Can be not_started, in_progress, finished.
%% @end
%%-------------------------------------------
-spec import_state(od_space:id()) -> storage_import:state().
import_state(SpaceId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = StorageStrategies
        }}} = space_strategies:get(SpaceId),
    StorageId = hd(maps:keys(StorageStrategies)),
    ImportStartTime = space_strategies:get_import_start_time(SpaceId, StorageId),
    FilesToImport = get_files_to_import_value(SpaceId),
    case {ImportStartTime, FilesToImport} of
        {undefined, 0} ->
            not_started;
        {_, FilesToImport} when FilesToImport > 0 ->
            in_progress;
        {_, 0} ->
            finished
    end.

%%-------------------------------------------------------------------
%% @doc
%% Checks if storage_update is in progress
%% @end
%%-------------------------------------------------------------------
-spec update_in_progress(od_space:id()) -> boolean().
update_in_progress(SpaceId) ->
    get_files_to_update_value(SpaceId) > 0.

%%-------------------------------------------------------------------
%% @doc
%% Returns values and last measurement timestamp for given metric.
%% @end
%%-------------------------------------------------------------------
-spec get_metric(od_space:id(), spiral_type(), window()) -> proplists:proplist() | undefined.
get_metric(SpaceId, Type, Window) ->
    SpiralName = ?SPIRAL_NAME(SpaceId, Type, Window),
    case storage_sync_histogram:get_histogram(SpiralName) of
        undefined ->
            undefined;
        {Values, Timestamp} ->
            [
                {values, Values},
                {timestamp, Timestamp}
            ]
    end.

%%-------------------------------------------------------------------
%% @doc
%% Ensures all metrics for given SpaceId are stopped.
%% @end
%%-------------------------------------------------------------------
-spec ensure_all_metrics_stopped(od_space:id()) -> ok.
ensure_all_metrics_stopped(SpaceId) ->
    storage_sync_monitoring:stop_files_to_update_counter(SpaceId),
    storage_sync_monitoring:stop_imported_files_counter(SpaceId),
    storage_sync_monitoring:stop_files_to_import_counter(SpaceId),
    storage_sync_monitoring:stop_updated_files_spirals(SpaceId),
    storage_sync_monitoring:stop_deleted_files_spirals(SpaceId),
    storage_sync_monitoring:stop_imported_files_spirals(SpaceId),
    storage_sync_monitoring:stop_queue_length_spirals(SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function restarts reporters which are supposed to be running.
%% TODO improve handling failures of exometer VFS-3173
%% @end
%%-------------------------------------------------------------------
-spec ensure_reporters_started(od_space:id()) -> ok.
ensure_reporters_started(SpaceId) ->
    ExpectedReporters = [?LAGER_REPORTER_NAME, ?ETS_REPORTER_NAME],
    AliveReporters = lists:filtermap(fun({Reporter, Pid}) ->
        case {lists:member(Reporter, ExpectedReporters), erlang:is_process_alive(Pid)} of
            {true, true} -> {true, Reporter};
            _ -> false
        end
    end, exometer_report:list_reporters()),
    case AliveReporters -- ExpectedReporters of
        [] ->
            ok;
        DeadReporters ->
            lists:foreach(fun(Reporter) ->
                start_reporter(Reporter)
            end, DeadReporters),
            resubscribe(DeadReporters, SpaceId)
    end.

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function restarts reporters which are supposed to be running.
%% TODO improve handling failures of exometer VFS-3173
%% @end
%%-------------------------------------------------------------------
-spec start_reporter(atom()) -> ok.
start_reporter(?LAGER_REPORTER_NAME) ->
    start_lager_reporter();
start_reporter(?ETS_REPORTER_NAME) ->
    start_ets_reporter().

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resubscribes suitable metrics to given reporter.
%% TODO improve handling failures of exometer VFS-3173
%% @end
%%-------------------------------------------------------------------
-spec resubscribe([atom()], od_space:id()) -> ok.
resubscribe([], _SpaceId) ->
    ok;
resubscribe(?LAGER_REPORTER_NAME, SpaceId) ->
    start_imported_files_counter(SpaceId),
    start_files_to_import_counter(SpaceId),
    start_files_to_update_counter(SpaceId);
resubscribe(?ETS_REPORTER_NAME, SpaceId) ->
    start_imported_files_spirals(SpaceId),
    start_deleted_files_spirals(SpaceId),
    start_updated_files_spirals(SpaceId),
    start_queue_length_spirals(SpaceId);
resubscribe(DeadReporters = [H | T], SpaceId) when is_list(DeadReporters) ->
    resubscribe(H, SpaceId),
    resubscribe(T, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts exometer_spiral to monitor length of queue jobs
%% @end
%%-------------------------------------------------------------------
-spec start_queue_length_spiral(od_space:id(), window(),
    non_neg_integer()) -> ok.
start_queue_length_spiral(SpaceId, Window, Resolution) ->
    start_and_subscribe_storage_sync_spiral(SpaceId, ?QUEUE_LENGTH, Window, Resolution, count).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts exometer_spiral to monitor number of imported files.
%% @end
%%-------------------------------------------------------------------
-spec start_imported_files_spiral(od_space:id(), window(),
    non_neg_integer()) -> ok.
start_imported_files_spiral(SpaceId, Window, Resolution) ->
    start_and_subscribe_storage_sync_spiral(SpaceId, ?IMPORTED_FILES, Window, Resolution).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts exometer_spiral to monitor number of deleted files.
%% @end
%%-------------------------------------------------------------------
-spec start_deleted_files_spiral(od_space:id(), window(),
    non_neg_integer()) -> ok.
start_deleted_files_spiral(SpaceId, Window, Resolution) ->
    start_and_subscribe_storage_sync_spiral(SpaceId, ?DELETED_FILES, Window, Resolution).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts exometer_spiral to monitor number of updated files.
%% @end
%%-------------------------------------------------------------------
-spec start_updated_files_spiral(od_space:id(), window(),
    non_neg_integer()) -> ok.
start_updated_files_spiral(SpaceId, Window, Resolution) ->
    start_and_subscribe_storage_sync_spiral(SpaceId, ?UPDATED_FILES, Window, Resolution).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops counter of files to be updated
%% @end
%%-------------------------------------------------------------------
-spec stop_imported_files_spiral(od_space:id(), window()) -> ok.
stop_imported_files_spiral(SpaceId, Window) ->
    stop_and_unsubscribe_storage_sync_spiral(SpaceId, ?IMPORTED_FILES, Window).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops counter of files to be updated
%% @end
%%-------------------------------------------------------------------
-spec stop_updated_files_spiral(od_space:id(), window()) -> ok.
stop_updated_files_spiral(SpaceId, Window) ->
    stop_and_unsubscribe_storage_sync_spiral(SpaceId, ?UPDATED_FILES, Window).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops counter of files to be deleted
%% @end
%%-------------------------------------------------------------------
-spec stop_deleted_files_spiral(od_space:id(), window()) -> ok.
stop_deleted_files_spiral(SpaceId, Window) ->
    stop_and_unsubscribe_storage_sync_spiral(SpaceId, ?DELETED_FILES, Window).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops counter of files to be deleted
%% @end
%%-------------------------------------------------------------------
-spec stop_queue_length_spiral(od_space:id(), window()) -> ok.
stop_queue_length_spiral(SpaceId, Window) ->
    stop_and_unsubscribe_storage_sync_spiral(SpaceId, ?QUEUE_LENGTH,Window).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Increases counter of imported files
%% @end
%%-------------------------------------------------------------------
-spec increase_imported_files_spiral(od_space:id(), window()) ->
    ok | {error, term()}.
increase_imported_files_spiral(SpaceId, Window) ->
    update_spiral(SpaceId, ?IMPORTED_FILES, Window, 1).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Increases counter of deleted files
%% @end
%%-------------------------------------------------------------------
-spec increase_deleted_files_spiral(od_space:id(), window()) ->
    ok | {error, term()}.
increase_deleted_files_spiral(SpaceId, Window) ->
    update_spiral(SpaceId, ?DELETED_FILES, Window, 1).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Increases counter of updated files
%% @end
%%-------------------------------------------------------------------
-spec increase_updated_files_spiral(od_space:id(), window()) ->
    ok | {error, term()}.
increase_updated_files_spiral(SpaceId, Window) ->
    update_spiral(SpaceId, ?UPDATED_FILES, Window, 1).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates jobs queue length spiral.
%% @end
%%-------------------------------------------------------------------
-spec update_queue_length_spiral(od_space:id(), window(),
    non_neg_integer()) -> ok | {error, term()}.
update_queue_length_spiral(SpaceId, Window, Value) ->
    update_spiral(SpaceId, ?QUEUE_LENGTH, Window, Value).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts and subscribes to given type of counter.
%% @end
%%-------------------------------------------------------------------
-spec start_and_subscribe_storage_sync_counter(od_space:id(), counter_type()) -> 
    ok | error().
start_and_subscribe_storage_sync_counter(SpaceId, CounterType) ->
    CounterName = ?COUNTER_NAME(SpaceId, CounterType),
    exometer:new(CounterName, counter),
    ok = exometer_report:subscribe(?LAGER_REPORTER_NAME, CounterName, [value],
        ?COUNTER_LOGGING_INTERVAL).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts and subscribes to given type of spiral.
%% @end
%%-------------------------------------------------------------------
-spec start_and_subscribe_storage_sync_spiral(od_space:id(), spiral_type(),
    window(), non_neg_integer()) -> ok | error().
start_and_subscribe_storage_sync_spiral(SpaceId, Type, Window, Resolution) ->
    start_and_subscribe_storage_sync_spiral(SpaceId, Type, Window, Resolution, one).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts and subscribes to given type of spiral.
%% @end
%%-------------------------------------------------------------------
%%-spec start_and_subscribe_storage_sync_spiral(od_space:id(), spiral_type(),
%%    window(), non_neg_integer()) -> ok | error().
start_and_subscribe_storage_sync_spiral(SpaceId, Type, Window, Resolution, Metric) ->
    SpiralName = ?SPIRAL_NAME(SpaceId, Type, Window),
    TimeSpan = resolution_to_time_span(Window, Resolution),
    exometer:new(SpiralName, spiral, [{time_span, TimeSpan}]),
    ok = exometer_report:subscribe(?ETS_REPORTER_NAME, SpiralName, [Metric], TimeSpan).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops and unsubscribes counter of given type.
%% @end
%%-------------------------------------------------------------------
-spec stop_and_unsubscribe_storage_sync_counter(od_space:id(),
    counter_type()) -> ok | {error, term()}.
stop_and_unsubscribe_storage_sync_counter(SpaceId, CounterType) ->
    CounterName = ?COUNTER_NAME(SpaceId, CounterType),
    exometer_report:unsubscribe_all(?LAGER_REPORTER_NAME, CounterName),
    exometer:delete(CounterName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops and unsubscribes counter of given type.
%% @end
%%-------------------------------------------------------------------
-spec stop_and_unsubscribe_storage_sync_spiral(od_space:id(),
    spiral_type(), window()) -> ok | {error, term()}.
stop_and_unsubscribe_storage_sync_spiral(SpaceId, Type, Window) ->
    SpiralName = ?SPIRAL_NAME(SpaceId, Type, Window),
    exometer_report:unsubscribe_all(?ETS_REPORTER_NAME, SpiralName),
    exometer:delete(SpiralName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates given counter with given Value.
%% @end
%%-------------------------------------------------------------------
-spec update_counter(od_space:id(), counter_type(), integer()) ->
        ok | error().
update_counter(SpaceId, CounterType, Value) ->
    CounterName = ?COUNTER_NAME(SpaceId, CounterType),
    exometer:update(CounterName, Value).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates given counter with given Value.
%% @end
%%-------------------------------------------------------------------
-spec update_spiral(od_space:id(), spiral_type(), window(), integer()) ->
    ok | error().
update_spiral(SpaceId, Type, Window, Value) ->
    SpiralName = ?SPIRAL_NAME(SpaceId, Type, Window),
    exometer:update(SpiralName, Value).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Gets value of given counter.
%% @end
%%-------------------------------------------------------------------
-spec get_value(od_space:id(), counter_type()) -> integer().
get_value(SpaceId, CounterType) ->
    case exometer:get_value(?COUNTER_NAME(SpaceId, CounterType), [value]) of
        {ok, [{value, Value}]} ->
            Value;
        {error, not_found} ->
            0
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Window and resolution to time span
%% @end
%%-------------------------------------------------------------------
-spec resolution_to_time_span(window(), non_neg_integer()) -> non_neg_integer().
resolution_to_time_span(day, Resolution) -> timer:hours(24) div Resolution;
resolution_to_time_span(hour, Resolution) -> timer:hours(1) div Resolution;
resolution_to_time_span(minute, Resolution) -> timer:minutes(1) div Resolution.
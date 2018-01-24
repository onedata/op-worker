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


%% counters API

% starting & stopping
-export([start_counters/1, stop_counters/1, reset_sync_counters/1,
    start_imported_files_spirals/1, stop_imported_files_spirals/1,
    start_updated_files_spirals/1, stop_updated_files_spirals/1,
    start_deleted_files_spirals/1, stop_deleted_files_spirals/1,
    start_queue_length_spirals/1, stop_queue_length_spirals/1,
    ensure_all_metrics_stopped/1, get_update_state/1, get_import_state/1,
    get_metric/3
]).

% getters
-export([get_unhandled_files_value/1, get_files_to_sync_value/1,
    get_imported_files_value/1, get_failed_file_imports_value/1,
    get_updated_files_value/1, get_failed_file_updates_value/1,
    get_failed_file_deletions_value/1, get_deleted_files_value/1
]).


% setters
-export([increase_imported_files_counter/1, increase_failed_file_imports_counter/1,
    increase_updated_files_counter/1, increase_failed_file_updates_counter/1,
    increase_deleted_files_counter/1, increase_failed_file_deletions_counter/1,
    update_files_to_sync_counter/2, increase_imported_files_spirals/1,
    increase_updated_files_spirals/1, increase_deleted_files_spirals/1,
    update_queue_length_spirals/2
]).

-export([init_report/0, init_reporter/1, init_counters/0]).


-type window() :: day | hours | minute.
-type counter_type() :: files_to_sync | imported_files | deleted_files | updated_files |
                        failed_file_imports | failed_file_deletions | failed_file_updates.
-type spiral_type() :: imported_files | updated_files | deleted_files | queue_length.
-type error() :: {error, term()}.

-define(STORAGE_SYNC_METRIC_PREFIX, storage_sync).

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
-define(FILES_TO_SYNC, files_to_sync).
-define(QUEUE_LENGTH, queue_length).

-define(IMPORTED_FILES, imported_files).
-define(DELETED_FILES, deleted_files).
-define(UPDATED_FILES, updated_files).

-define(FAILED_FILE_IMPORTS, failed_file_imports).
-define(FAILED_FILE_UPDATES, failed_file_updates).
-define(FAILED_FILE_DELETIONS, failed_file_deletions).

-define(SYNC_COUNTERS, [
    ?IMPORTED_FILES, ?FAILED_FILE_IMPORTS, 
    ?UPDATED_FILES, ?FAILED_FILE_UPDATES, 
    ?DELETED_FILES, ?FAILED_FILE_DELETIONS,
    ?FILES_TO_SYNC
]).

-define(SPIRAL_TYPES, [
    ?IMPORTED_FILES, ?DELETED_FILES, ?UPDATED_FILES, ?QUEUE_LENGTH
]).


-define(WINDOWS, [
   minute, hour, day
]).

%%-------------------------------------------------------------------
%% @doc
%% Starts counters required by storage_import and storage_update
%% @end
%%-------------------------------------------------------------------
-spec start_counters(od_space:id()) -> ok.
start_counters(SpaceId) ->
    lists:foreach(fun(CounterType) ->
        start_and_subscribe_storage_sync_counter(SpaceId, CounterType)
    end, ?SYNC_COUNTERS).

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
%% @private
%% @doc
%% Stops and unsubscribes counters required by storage_import and storage_update
%% @end
%%-------------------------------------------------------------------
-spec stop_counters(od_space:id()) -> ok.
stop_counters(SpaceId) ->
    lists:foreach(fun(CounterType) ->
       stop_and_unsubscribe_storage_sync_counter(SpaceId, CounterType)
    end, ?SYNC_COUNTERS).

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
%% Reset counters used by storage_import and storage_update
%% @end
%%-------------------------------------------------------------------
-spec reset_sync_counters(od_space:id()) -> ok.
reset_sync_counters(SpaceId) ->
    lists:foreach(fun(CounterType) ->
        ok = reset_counter(SpaceId, CounterType)
    end, ?SYNC_COUNTERS).

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
%% Increases counter of imported files
%% @end
%%-------------------------------------------------------------------
-spec increase_failed_file_imports_counter(od_space:id()) ->
    ok | {error, term()}.
increase_failed_file_imports_counter(SpaceId) ->
    update_counter(SpaceId, ?FAILED_FILE_IMPORTS, 1).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of updated files
%% @end
%%-------------------------------------------------------------------
-spec increase_updated_files_counter(od_space:id()) ->
    ok | {error, term()}.
increase_updated_files_counter(SpaceId) ->
    update_counter(SpaceId, ?UPDATED_FILES, 1).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of updated files
%% @end
%%-------------------------------------------------------------------
-spec increase_failed_file_updates_counter(od_space:id()) ->
    ok | {error, term()}.
increase_failed_file_updates_counter(SpaceId) ->
    update_counter(SpaceId, ?FAILED_FILE_UPDATES, 1).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of deleted files
%% @end
%%-------------------------------------------------------------------
-spec increase_deleted_files_counter(od_space:id()) ->
    ok | {error, term()}.
increase_deleted_files_counter(SpaceId) ->
    update_counter(SpaceId, ?DELETED_FILES, 1).

%%-------------------------------------------------------------------
%% @doc
%% Increases counter of deleted files
%% @end
%%-------------------------------------------------------------------
-spec increase_failed_file_deletions_counter(od_space:id()) ->
    ok | {error, term()}.
increase_failed_file_deletions_counter(SpaceId) ->
    update_counter(SpaceId, ?FAILED_FILE_DELETIONS, 1).

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
-spec update_files_to_sync_counter(od_space:id(), integer()) -> ok | error().
update_files_to_sync_counter(SpaceId, Value) ->
    update_counter(SpaceId, ?FILES_TO_SYNC, Value).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of files to be imported counter
%% @end
%%-------------------------------------------------------------------
-spec get_files_to_sync_value(od_space:id()) -> integer().
get_files_to_sync_value(SpaceId) ->
    get_value(SpaceId, ?FILES_TO_SYNC).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of imported files counter
%% @end
%%-------------------------------------------------------------------
-spec get_imported_files_value(od_space:id()) -> integer().
get_imported_files_value(SpaceId) ->
    get_value(SpaceId, ?IMPORTED_FILES).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of updated files counter
%% @end
%%-------------------------------------------------------------------
-spec get_updated_files_value(od_space:id()) -> integer().
get_updated_files_value(SpaceId) ->
    get_value(SpaceId, ?UPDATED_FILES).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of failed file imports counter
%% @end
%%-------------------------------------------------------------------
-spec get_failed_file_imports_value(od_space:id()) -> integer().
get_failed_file_imports_value(SpaceId) ->
    get_value(SpaceId, ?FAILED_FILE_IMPORTS).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of failed file updates counter
%% @end
%%-------------------------------------------------------------------
-spec get_failed_file_updates_value(od_space:id()) -> integer().
get_failed_file_updates_value(SpaceId) ->
    get_value(SpaceId, ?FAILED_FILE_UPDATES).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of failed file deletions counter
%% @end
%%-------------------------------------------------------------------
-spec get_failed_file_deletions_value(od_space:id()) -> integer().
get_failed_file_deletions_value(SpaceId) ->
    get_value(SpaceId, ?FAILED_FILE_DELETIONS).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of deleted_files
%% @end
%%-------------------------------------------------------------------
-spec get_deleted_files_value(od_space:id()) -> integer().
get_deleted_files_value(SpaceId) ->
    get_value(SpaceId, ?DELETED_FILES).

%%-------------------------------------------------------------------
%% @doc
%% Returns balance of sync counters.
%% @end
%%-------------------------------------------------------------------
-spec get_unhandled_files_value(od_space:id()) -> integer().
get_unhandled_files_value(SpaceId) ->
    get_files_to_sync_value(SpaceId) -
    get_imported_files_value(SpaceId) -
    get_failed_file_imports_value(SpaceId) -
    get_updated_files_value(SpaceId) -
    get_failed_file_updates_value(SpaceId) -
    get_deleted_files_value(SpaceId) -
    get_failed_file_deletions_value(SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Returns state of import. Possible values: not_started, in_progress, finished.
%% @end
%%-------------------------------------------
-spec get_import_state(od_space:id()) -> storage_import:state().
get_import_state(SpaceId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = StorageStrategies
        }}} = space_strategies:get(SpaceId),
    StorageId = hd(maps:keys(StorageStrategies)),
    ImportStartTime = space_strategies:get_import_start_time(StorageStrategies, StorageId),
    ImportFinishTime = space_strategies:get_import_finish_time(StorageStrategies, StorageId),
    case {ImportStartTime, ImportFinishTime} of
        {undefined, undefined} ->
            not_started;
        {_, undefined} ->
            in_progress;
        {ImportStartTime, ImportFinishTime} when ImportStartTime =/= undefined ->
            finished
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns state of update. Possible values: not_started, in_progress, finished.
%% @end
%%-------------------------------------------------------------------
-spec get_update_state(od_space:id()) -> storage_update:state().
get_update_state(SpaceId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = StorageStrategies
        }}} = space_strategies:get(SpaceId),
    {ok, #document{
        value = #space_strategies{
            storage_strategies = StorageStrategies
        }}} = space_strategies:get(SpaceId),
    StorageId = hd(maps:keys(StorageStrategies)),
    LastUpdateStartTime = space_strategies:get_last_update_start_time(StorageStrategies, StorageId),
    LastUpdateFinishTime = space_strategies:get_last_update_finish_time(StorageStrategies, StorageId),
    case {LastUpdateStartTime, LastUpdateFinishTime} of
        {undefined, undefined} ->
            not_started;
        {_, undefined} ->
            in_progress;
        {LastUpdateStartTime, LastUpdateFinishTime} when LastUpdateStartTime =/= undefined ->
            finished
    end.

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
    stop_counters(SpaceId),
    storage_sync_monitoring:stop_updated_files_spirals(SpaceId),
    storage_sync_monitoring:stop_deleted_files_spirals(SpaceId),
    storage_sync_monitoring:stop_imported_files_spirals(SpaceId),
    storage_sync_monitoring:stop_queue_length_spirals(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all spaces.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    try od_provider:get_or_fetch(oneprovider:get_provider_id()) of
        {ok, #document{value = #od_provider{spaces = SpaceIds}}} ->
            init_report(SpaceIds);
        {error, _} -> ok
    catch
        _:TReason ->
            ?error_stacktrace("Unable to restart reporters due to: ~p", [TReason])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initialize exometer reporter used by storage_sync.
%% @end
%%--------------------------------------------------------------------
-spec init_reporter(atom()) -> ok.
init_reporter(exometer_report_rrd_ets) ->
    exometer_report:add_reporter(exometer_report_rrd_ets, []).

%%--------------------------------------------------------------------
%% @doc
%% Callback that initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    try  od_provider:get_or_fetch(oneprovider:get_provider_id()) of
        {ok, #document{value = #od_provider{spaces = SpaceIds}}} ->
            init_counters(SpaceIds);
        {error, _} -> ok
    catch
        _:TReason ->
            ?error_stacktrace("Unable to start storage_sync counters due to: ~p", [TReason])
    end.

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for resubscribing lager and ets reporter
%% for all Spaces for which storage_import is turned on.
%% @end
%%-------------------------------------------------------------------
-spec init_report([od_space:id()]) -> ok.
init_report([]) ->
    ok;
init_report([SpaceId | Rest]) ->
    case space_strategies:is_import_on(SpaceId) of
        false ->
            ok;
        true ->
            resubscribe(?LAGER_REPORTER_NAME, SpaceId),
            resubscribe(?ETS_REPORTER_NAME, SpaceId)
    end,
    init_report(Rest).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for starting counters required by
%% storage_sync.
%% @end
%%-------------------------------------------------------------------
-spec init_report([od_space:id()]) -> ok.
init_counters([]) ->
    ok;
init_counters([SpaceId | Rest]) ->
    case space_strategies:is_import_on(SpaceId) of
        false ->
            ok;
        true ->
            start_counters(SpaceId)
    end,
    init_counters(Rest).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resubscribes suitable metrics to given reporter.
%% TODO improve handling failures of exometer VFS-3173
%% @end
%%-------------------------------------------------------------------
-spec resubscribe(atom(), od_space:id()) -> ok.
resubscribe(?LAGER_REPORTER_NAME, SpaceId) ->
    start_counters(SpaceId);
resubscribe(?ETS_REPORTER_NAME, SpaceId) ->
    start_imported_files_spirals(SpaceId),
    start_deleted_files_spirals(SpaceId),
    start_updated_files_spirals(SpaceId),
    start_queue_length_spirals(SpaceId).

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
    catch exometer:new(CounterName, counter),
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
-spec start_and_subscribe_storage_sync_spiral(od_space:id(), spiral_type(),
    window(), non_neg_integer(), exometer:datapoint()) -> ok.
start_and_subscribe_storage_sync_spiral(SpaceId, Type, Window, Resolution, Metric) ->
    SpiralName = ?SPIRAL_NAME(SpaceId, Type, Window),
    TimeSpan = resolution_to_time_span(Window, Resolution),
    catch exometer:new(SpiralName, spiral, [{time_span, TimeSpan}]),
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
-spec reset_counter(od_space:id(), counter_type()) -> ok | error().
reset_counter(SpaceId, CounterType) ->
    CounterName = ?COUNTER_NAME(SpaceId, CounterType),
    exometer:reset(CounterName).

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
            0;
        Error ->
            ?error("Cannot get exometer counter ~p for space ~p, error: ~p:p",
                [CounterType, SpaceId, Error])
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
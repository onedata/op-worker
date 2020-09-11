%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model responsible for storing monitoring data from storage import
%%% auto scans.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_monitoring).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/storage/import/storage_import.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([prepare_new_scan/1]).
-export([
    increase_to_process_counter/2,
    mark_imported_file/1,
    mark_updated_file/1,
    mark_deleted_file/1,
    mark_processed_file/1,
    mark_failed_file/1,
    mark_finished_scan/2,
    set_aborting_status/1
]).
-export([
    get_info/1,
    get_stats/3,
    is_scan_in_progress/1,
    is_initial_scan_finished/1,
    is_scan_finished/2, 
    is_initial_scan_not_started_yet/1, 
    is_scan_not_started_yet/2,
    get_finished_scans_num/1,
    get_scan_stop_time/1]).


% export for use/mocking in CT tests
-export([describe/1, update/2]).

%% datastore API
-export([get/1, get_or_create/1, create/2, delete/1]).

% export for migration
-export([migrate_to_v1/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-define(CTX, #{model => ?MODULE}).

% metric types
-define(QUEUE_LENGTH, <<"queueLength">>).
-define(IMPORT_COUNT, <<"importCount">>).
-define(UPDATE_COUNT, <<"updateCount">>).
-define(DELETE_COUNT, <<"deleteCount">>).

-type key() :: od_space:id().
-type record() :: #storage_import_monitoring{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type timestamp() :: non_neg_integer().
-type status() :: ?ENQUEUED | ?RUNNING | ?ABORTING | ?FAILED | ?COMPLETED.

-type window() :: day | hour | minute.
-type plot_counter_type() :: binary(). % ?QUEUE_LENGTH | ?IMPORT_COUNT | ?UPDATE_COUNT | ?DELETE_COUNT.
-type error() :: {error, term()}.

%% @formatter:off
-type time_stats() :: #{
    lastValueDate := binary(),
    values := [non_neg_integer()]
}.

-type import_stats() :: #{
    plot_counter_type() => time_stats()
}.
%% @formatter:on


-export_type([record/0, doc/0, window/0, plot_counter_type/0, status/0, import_stats/0]).


-define(HISTOGRAM_LENGTH,
    application:get_env(?APP_NAME, storage_import_histogram_length, 12)).
-define(MIN_HIST_SLOT, 60 div ?HISTOGRAM_LENGTH).
-define(HOUR_HIST_SLOT, 3600 div ?HISTOGRAM_LENGTH). % 3600s = 60 * 60s
-define(DAY_HIST_SLOT, 86400 div ?HISTOGRAM_LENGTH). % 86400s = 24 * 3600s


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Prepares existing document for new scan.
%% This function assumes that document has already been created by
%% ?MODULE:ensure_created/2 function.
%% It resets control counters, and increases to_process counter and
%% queue_length histograms by 1.
%% This function also sets scan start_time.
%% @end
%%-------------------------------------------------------------------
-spec prepare_new_scan(key()) -> {ok, doc()} | {error, term()}.
prepare_new_scan(SpaceId) ->
    storage_import_monitoring:update(SpaceId, fun(SIM) ->
        case is_scan_in_progress(SIM) of
            true ->
                {error, already_started};
            false ->
                Timestamp = time_utils:cluster_time_millis(),
                TimestampSecs = Timestamp div 1000,
                SIM2 = reset_queue_length_histograms(SIM, TimestampSecs),
                SIM3 = increment_queue_length_histograms(SIM2, TimestampSecs, 1),
                SIM4 = reset_control_counters(SIM3),
                {ok, SIM4#storage_import_monitoring{
                    to_process = 1,
                    status = ?ENQUEUED,
                    scan_start_time = Timestamp
                }}
        end
    end).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file jobs were added to queue.
%% It increases suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec increase_to_process_counter(key(), non_neg_integer()) -> ok.
increase_to_process_counter(SpaceId, Value) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        to_process = FilesToProcess
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            status = ?RUNNING,
            to_process = FilesToProcess + Value
        },
        {ok, increment_queue_length_histograms(SIM2, Timestamp, Value)}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been imported.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_imported_file(key()) -> ok.
mark_imported_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        imported = ImportedFiles,
        imported_sum = ImportedFilesSum,
        imported_min_hist = MinHist,
        imported_hour_hist = HourHist,
        imported_day_hist = DayHist
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            status = ?RUNNING,
            imported = ImportedFiles + 1,
            imported_sum = ImportedFilesSum + 1,
            imported_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            imported_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            imported_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SIM3 = decrement_queue_length_histograms(SIM2, Timestamp),
        {ok, SIM3}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been updated.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_updated_file(key()) -> ok.
mark_updated_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        updated = UpdatedFiles,
        updated_sum = UpdatedFilesSum,
        updated_min_hist = MinHist,
        updated_hour_hist = HourHist,
        updated_day_hist = DayHist
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            status = ?RUNNING,
            updated = UpdatedFiles + 1,
            updated_sum = UpdatedFilesSum + 1,
            updated_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            updated_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            updated_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SIM3 = decrement_queue_length_histograms(SIM2, Timestamp),
        {ok, SIM3}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been deleted.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_deleted_file(key()) -> ok.
mark_deleted_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        deleted = DeletedFiles,
        deleted_sum = DeletedFilesSum,
        deleted_min_hist = MinHist,
        deleted_hour_hist = HourHist,
        deleted_day_hist = DayHist
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            status = ?RUNNING,
            deleted = DeletedFiles + 1,
            deleted_sum = DeletedFilesSum + 1,
            deleted_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            deleted_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            deleted_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SIM3 = decrement_queue_length_histograms(SIM2, Timestamp),
        {ok, SIM3}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been processed.
%% This function is used for marking processed files that does not match
%% to any of categories: imported, updated, deleted, failed.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_file(key()) -> ok.
mark_processed_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        other_processed = FilesProcessed
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            status = ?RUNNING,
            other_processed = FilesProcessed + 1
        },
        SIM3 = decrement_queue_length_histograms(SIM2, Timestamp),
        {ok, SIM3}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that failure occurred when
%% processing file. It increases suitable counters and histograms and
%% decreases queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed_file(key()) -> ok.
mark_failed_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        failed = FilesFailed
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            status = ?RUNNING,
            failed = FilesFailed + 1
        },
        SIM3 = decrement_queue_length_histograms(SIM2, Timestamp),
        {ok, SIM3}
    end)).


-spec mark_finished_scan(key(), boolean()) -> ok | {error, term()}.
mark_finished_scan(SpaceId, Aborted) ->
    ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM) ->
        {ok, mark_finished_scan_internal(SIM, Aborted)}
    end)).


-spec set_aborting_status(key()) -> ok.
set_aborting_status(SpaceId) ->
    ?extract_ok(update(SpaceId, fun(SIM) ->
        case is_scan_in_progress(SIM) of
            true -> {ok, SIM#storage_import_monitoring{status = ?ABORTING}};
            false -> {ok, SIM}
        end
    end)).


-spec get_info(key() | record() | doc()) -> {ok, json_utils:json_term()} | error().
get_info(#document{value = SIM}) ->
    get_info(SIM);
get_info(#storage_import_monitoring{
    finished_scans = Scans,
    status = Status,
    scan_start_time = StartTime,
    scan_stop_time = StopTime,
    imported = ImportCount,
    updated = UpdatedCount,
    deleted = DeletedCount
}) ->
    {ok, #{
        totalScans => Scans,
        status => Status,
        start => StartTime div 1000,
        stop => StopTime div 1000,
        importedFiles => ImportCount,
        updatedFiles => UpdatedCount,
        deletedFiles => DeletedCount
    }};
get_info(SpaceId) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, Doc} ->
            get_info(Doc);
        {error, _} = Error->
            Error
    end.


-spec get_stats(key() | record(), [plot_counter_type()], window()) -> {ok, import_stats()}.
get_stats(SIM = #storage_import_monitoring{}, Types, Window) ->
    {ok, lists:foldl(fun(Type, AccIn) ->
        AccIn#{Type => return_histogram_and_timestamp(SIM, Type, Window)}
    end, #{}, Types)};
get_stats(SpaceId, Types, Window) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, #document{value = SIM}} ->
            get_stats(SIM, Types, Window);
        {error, not_found} ->
            ?debug("Failed to fetch storage import metrics for space ~p SpaceId due to not_found", [SpaceId]),
            {ok, #{}};
        Error ->
            ?error("Failed to fetch storage import metrics for space ~p SpaceId due to ~p", [SpaceId, Error]),
            {ok, #{}}
    end.


-spec is_scan_in_progress(key() | doc() | record()) -> boolean().
is_scan_in_progress(#document{value = SIM = #storage_import_monitoring{}}) ->
    is_scan_in_progress(SIM);
is_scan_in_progress(#storage_import_monitoring{status = ?RUNNING}) ->
    true;
is_scan_in_progress(#storage_import_monitoring{status = ?ABORTING}) ->
    true;
is_scan_in_progress(#storage_import_monitoring{}) ->
    false;
is_scan_in_progress(SpaceId) ->
    case storage_import_monitoring:get(SpaceId) of
        {error, not_found} ->
            false;
        {ok, Doc} ->
            is_scan_in_progress(Doc)
    end.


-spec is_initial_scan_finished(key() | doc() | record()) -> boolean().
is_initial_scan_finished(IdOrDoc) ->
    is_scan_finished(IdOrDoc, 1).


-spec is_scan_finished(key() | doc() | record(), non_neg_integer()) -> boolean().
is_scan_finished(#document{value = SIM = #storage_import_monitoring{}}, ScanNo) ->
    is_scan_finished(SIM, ScanNo);
is_scan_finished(#storage_import_monitoring{finished_scans = FinishedScans}, ScanNo) ->
    FinishedScans >= ScanNo;
is_scan_finished(SpaceId, ScanNo) ->
    case storage_import_monitoring:get(SpaceId) of
        {error, not_found} ->
            false;
        {ok, Doc} ->
            is_scan_finished(Doc, ScanNo)
    end.


-spec is_initial_scan_not_started_yet(doc() | record()) -> boolean().
is_initial_scan_not_started_yet(SIM) ->
    is_scan_not_started_yet(SIM, 1).


-spec is_scan_not_started_yet(doc() | record(), non_neg_integer()) -> boolean().
is_scan_not_started_yet(#document{value = SIM = #storage_import_monitoring{}}, ScanNo) ->
    is_scan_not_started_yet(SIM, ScanNo);
is_scan_not_started_yet(SIM = #storage_import_monitoring{finished_scans = FinishedScans}, ScanNo)
    when FinishedScans =:=  ScanNo - 1
->
    not is_scan_in_progress(SIM);
is_scan_not_started_yet(#storage_import_monitoring{finished_scans = FinishedScans}, ScanNo) ->
    FinishedScans < ScanNo.


-spec get_finished_scans_num(key() | doc() | record()) -> {ok, non_neg_integer()}.
get_finished_scans_num(#storage_import_monitoring{finished_scans = Scans}) ->
    {ok, Scans};
get_finished_scans_num(#document{value = SIM}) ->
    get_finished_scans_num(SIM);
get_finished_scans_num(SpaceId) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, Doc} ->
            get_finished_scans_num(Doc);
        Error = {error, _}->
            Error
    end.

-spec get_scan_stop_time(doc() | record()) -> {ok, non_neg_integer()} | undefined.
get_scan_stop_time(#storage_import_monitoring{scan_stop_time = ScanStopTime}) ->
    {ok, ScanStopTime};
get_scan_stop_time(#document{value = SIM}) ->
    get_scan_stop_time(SIM).

%%%===================================================================
%%% CT test API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns monitoring data converted to map.
%% @end
%%-------------------------------------------------------------------
-spec describe(key()) -> json_utils:json_map().
describe(SpaceId) ->
    {ok, #document{
        value = #storage_import_monitoring{
            finished_scans = Scans,
            status = Status,
            scan_start_time = ScanStartTime,
            scan_stop_time = ScanStopTime,
            to_process = ToProcess,
            imported = Imported,
            updated = Updated,
            deleted = Deleted,
            failed = Failed,
            other_processed = OtherProcessed,
            imported_sum = ImportedSum,
            updated_sum = UpdatedSum,
            deleted_sum = DeletedSum,
            imported_min_hist = ImportedMinHist,
            imported_hour_hist = ImportedHourHist,
            imported_day_hist = ImportedDayHist,
            updated_min_hist = UpdatedMinHist,
            updated_hour_hist = UpdatedHourHist,
            updated_day_hist = UpdatedDayHist,
            deleted_min_hist = DeletedMinHist,
            deleted_hour_hist = DeletedHourHist,
            deleted_day_hist = DeletedDayHist,
            queue_length_min_hist = QueueLengthMinHist,
            queue_length_hour_hist = QueueLengthHourHist,
            queue_length_day_hist = QueueLengthDayHist
        }
    }} = storage_import_monitoring:get(SpaceId),
    ImportedHistsTimestamp = get_histogram_timestamp(ImportedMinHist),
    UpdatedHistsTimestamp = get_histogram_timestamp(UpdatedMinHist),
    DeletedHistsTimestamp = get_histogram_timestamp(DeletedMinHist),
    QueueLengthHistsTimestamp = get_histogram_timestamp(QueueLengthMinHist),
    #{
        <<"scans">> => Scans,
        <<"status">> => Status,
        <<"scanStartTime">> => ScanStartTime,
        <<"scanStopTime">> => ScanStopTime,
        <<"toProcess">> => ToProcess,
        <<"imported">> => Imported,
        <<"updated">> => Updated,
        <<"deleted">> => Deleted,
        <<"failed">> => Failed,
        <<"otherProcessed">> => OtherProcessed,
        <<"importedSum">> => ImportedSum,
        <<"updatedSum">> => UpdatedSum,
        <<"deletedSum">> => DeletedSum,
        <<"importedMinHist">> => get_histogram_values(ImportedMinHist),
        <<"importedHourHist">> => get_histogram_values(ImportedHourHist),
        <<"importedDayHist">> => get_histogram_values(ImportedDayHist),
        <<"importedHistsTimestamp">> => ImportedHistsTimestamp,
        <<"updatedMinHist">> => get_histogram_values(UpdatedMinHist),
        <<"updatedHourHist">> => get_histogram_values(UpdatedHourHist),
        <<"updatedDayHist">> => get_histogram_values(UpdatedDayHist),
        <<"updatedHistsTimestamp">> => UpdatedHistsTimestamp,
        <<"deletedMinHist">> => get_histogram_values(DeletedMinHist),
        <<"deletedHourHist">> => get_histogram_values(DeletedHourHist),
        <<"deletedDayHist">> => get_histogram_values(DeletedDayHist),
        <<"deletedHistsTimestamp">> => DeletedHistsTimestamp,
        <<"queueLengthMinHist">> => get_histogram_values(QueueLengthMinHist),
        <<"queueLengthHourHist">> => get_histogram_values(QueueLengthHourHist),
        <<"queueLengthDayHist">> => get_histogram_values(QueueLengthDayHist),
        <<"queueLengthHistsTimestamp">> => QueueLengthHistsTimestamp
    }.


-spec update(key(), diff()) -> {ok, doc()} | error().
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, Diff).

%%%===================================================================
%%% datastore API
%%%===================================================================

-spec get(key()) -> {ok, doc()}  | error().
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).


-spec get_or_create(key()) -> {ok, doc()} | {error, term()}.
get_or_create(SpaceId) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, Doc} ->
            {ok, Doc};
        {error, not_found} ->
            case create(new_doc(SpaceId)) of
                {ok, Doc} -> {ok, Doc};
                {error, already_exists} -> storage_import_monitoring:get(SpaceId)
            end;
        Error ->
            ?error("Failed to fetch storage_import_monitoring document for space ~p due to ~p",
                [SpaceId, Error])
    end.


-spec create(key(), record()) -> {ok, doc()} | error().
create(SpaceId, SIM = #storage_import_monitoring{}) ->
    create(#document{key = SpaceId, value = SIM}).


-spec delete(key()) -> ok | error().
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create(doc()) -> {ok, doc()} | error().
create(Doc) ->
    datastore_model:create(?CTX, Doc).


-spec new_doc(key()) -> doc().
new_doc(SpaceId) ->
    #document{
        key = SpaceId,
        value = new_record(),
        scope = SpaceId
    }.

-spec new_record() -> record().
new_record() ->
    Timestamp = time_utils:cluster_time_seconds(),
    EmptyMinHist = time_slot_histogram:new(Timestamp, ?MIN_HIST_SLOT, ?HISTOGRAM_LENGTH),
    EmptyHourHist = time_slot_histogram:new(Timestamp, ?HOUR_HIST_SLOT, ?HISTOGRAM_LENGTH),
    EmptyDayHist = time_slot_histogram:new(Timestamp, ?DAY_HIST_SLOT, ?HISTOGRAM_LENGTH),

    #storage_import_monitoring{
        status = ?ENQUEUED,

        imported_min_hist = EmptyMinHist,
        imported_hour_hist = EmptyHourHist,
        imported_day_hist = EmptyDayHist,

        updated_min_hist = EmptyMinHist,
        updated_hour_hist = EmptyHourHist,
        updated_day_hist = EmptyDayHist,

        deleted_min_hist = EmptyMinHist,
        deleted_hour_hist = EmptyHourHist,
        deleted_day_hist = EmptyDayHist,

        queue_length_min_hist = time_slot_histogram:new_cumulative(Timestamp, ?MIN_HIST_SLOT, ?HISTOGRAM_LENGTH),
        queue_length_hour_hist = time_slot_histogram:new_cumulative(Timestamp, ?HOUR_HIST_SLOT, ?HISTOGRAM_LENGTH),
        queue_length_day_hist = time_slot_histogram:new_cumulative(Timestamp, ?DAY_HIST_SLOT, ?HISTOGRAM_LENGTH)
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function reset all control counters in
%% given storage_import_monitoring record.
%% @end
%%-------------------------------------------------------------------
-spec reset_control_counters(record()) -> record().
reset_control_counters(SIM) ->
    SIM#storage_import_monitoring{
        to_process = 0,
        imported = 0,
        updated = 0,
        deleted = 0,
        other_processed = 0,
        failed = 0
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function increments all queue_length histograms with given value.
%% @end
%%-------------------------------------------------------------------
-spec increment_queue_length_histograms(record(), timestamp(),
    non_neg_integer()) -> record().
increment_queue_length_histograms(SIM = #storage_import_monitoring{
    queue_length_min_hist = MinHist,
    queue_length_hour_hist = HourHist,
    queue_length_day_hist = DayHist
}, Timestamp, Value) ->
    SIM#storage_import_monitoring{
        queue_length_min_hist = time_slot_histogram:increment(MinHist, Timestamp, Value),
        queue_length_hour_hist = time_slot_histogram:increment(HourHist, Timestamp, Value),
        queue_length_day_hist = time_slot_histogram:increment(DayHist, Timestamp, Value)
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function decrements all queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec decrement_queue_length_histograms(record(), timestamp()) ->
    record().
decrement_queue_length_histograms(SIM = #storage_import_monitoring{
    queue_length_min_hist = MinHist,
    queue_length_hour_hist = HourHist,
    queue_length_day_hist = DayHist
}, Timestamp) ->
    SIM#storage_import_monitoring{
        queue_length_min_hist = time_slot_histogram:decrement(MinHist, Timestamp),
        queue_length_hour_hist = time_slot_histogram:decrement(HourHist, Timestamp),
        queue_length_day_hist = time_slot_histogram:decrement(DayHist, Timestamp)
    }.

-spec mark_finished_scan_internal(record(), boolean()) -> record().
mark_finished_scan_internal(SIM = #storage_import_monitoring{
    scan_start_time = ScanStartTime,
    scan_stop_time = ScanStopTime,
    finished_scans = Scans,
    failed = Failed
}, Aborted)
    when (ScanStartTime =/= undefined andalso ScanStopTime =:= undefined)
    orelse (ScanStartTime > ScanStopTime)
->
    Timestamp = time_utils:cluster_time_millis(),
    SIM2 = SIM#storage_import_monitoring{
        finished_scans = Scans + 1,
        scan_stop_time = Timestamp,
        status = case {Aborted, Failed > 0} of
            {true, _} -> ?ABORTED;
            {false, true} -> ?FAILED;
            {false, false} -> ?COMPLETED
        end
    },
    reset_queue_length_histograms(SIM2, Timestamp div 1000);
mark_finished_scan_internal(SIM, _Aborted) ->
    SIM.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of values of given histogram.
%% @end
%%-------------------------------------------------------------------
-spec get_histogram_values(time_slot_histogram:histogram())
        -> histogram:histogram().
get_histogram_values(Histogram) ->
    time_slot_histogram:get_histogram_values(Histogram).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns last update timestamp of given histogram.
%% @end
%%-------------------------------------------------------------------
-spec get_histogram_timestamp(time_slot_histogram:histogram())
        -> timestamp().
get_histogram_timestamp(Histogram) ->
    time_slot_histogram:get_last_update(Histogram).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that queue_length histograms
%% (which are cumulative histograms) won't accumulate data from previous
%% scans.
%% @end
%%-------------------------------------------------------------------
-spec reset_queue_length_histograms(record(),
    non_neg_integer()) -> record().
reset_queue_length_histograms(SIM = #storage_import_monitoring{
    queue_length_min_hist = QueueLengthMinHist,
    queue_length_hour_hist = QueueLengthHourHist,
    queue_length_day_hist = QueueLengthDayHist
}, Timestamp) ->
    QueueLengthMinHist2 = time_slot_histogram:reset_cumulative(QueueLengthMinHist, Timestamp),
    QueueLengthHourHist2 = time_slot_histogram:reset_cumulative(QueueLengthHourHist, Timestamp),
    QueueLengthDayHist2 = time_slot_histogram:reset_cumulative(QueueLengthDayHist, Timestamp),
    SIM#storage_import_monitoring{
        queue_length_min_hist = QueueLengthMinHist2,
        queue_length_hour_hist = QueueLengthHourHist2,
        queue_length_day_hist = QueueLengthDayHist2
    }.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns histogram with given timestamp in the
%% format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec return_histogram_and_timestamp(record(),
    plot_counter_type(), window()) -> json_utils:json_term().
return_histogram_and_timestamp(SIM, ?IMPORT_COUNT, minute) ->
    prepare(SIM#storage_import_monitoring.imported_min_hist);
return_histogram_and_timestamp(SIM, ?IMPORT_COUNT, hour) ->
    prepare(SIM#storage_import_monitoring.imported_hour_hist);
return_histogram_and_timestamp(SIM, ?IMPORT_COUNT, day) ->
    prepare(SIM#storage_import_monitoring.imported_day_hist);
return_histogram_and_timestamp(SIM, ?UPDATE_COUNT, minute) ->
    prepare(SIM#storage_import_monitoring.updated_min_hist);
return_histogram_and_timestamp(SIM, ?UPDATE_COUNT, hour) ->
    prepare(SIM#storage_import_monitoring.updated_hour_hist);
return_histogram_and_timestamp(SIM, ?UPDATE_COUNT, day) ->
    prepare(SIM#storage_import_monitoring.updated_day_hist);
return_histogram_and_timestamp(SIM, ?DELETE_COUNT, minute) ->
    prepare(SIM#storage_import_monitoring.deleted_min_hist);
return_histogram_and_timestamp(SIM, ?DELETE_COUNT, hour) ->
    prepare(SIM#storage_import_monitoring.deleted_hour_hist);
return_histogram_and_timestamp(SIM, ?DELETE_COUNT, day) ->
    prepare(SIM#storage_import_monitoring.deleted_day_hist);
return_histogram_and_timestamp(SIM, ?QUEUE_LENGTH, minute) ->
    prepare(SIM#storage_import_monitoring.queue_length_min_hist);
return_histogram_and_timestamp(SIM, ?QUEUE_LENGTH, hour) ->
    prepare(SIM#storage_import_monitoring.queue_length_hour_hist);
return_histogram_and_timestamp(SIM, ?QUEUE_LENGTH, day) ->
    prepare(SIM#storage_import_monitoring.queue_length_day_hist).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of Values and Timestamp from given TimeSlotHistogram
%% in the format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec prepare(time_slot_histogram:histogram()) -> time_stats().
    prepare(TimeSlotHistogram) ->
    Timestamp = time_utils:cluster_time_seconds(),
    TimeSlotHistogram2 = time_slot_histogram:increment(TimeSlotHistogram, Timestamp, 0),
    Values = time_slot_histogram:get_histogram_values(TimeSlotHistogram2),
    prepare(Timestamp, Values).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns given list of Values and Timestamp in the format acceptable
%% by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec prepare(timestamp(), [integer()]) -> time_stats().
prepare(Timestamp, Values) ->
    #{
        lastValueDate => time_utils:epoch_to_iso8601(Timestamp),
        values => lists:reverse(Values)
    }.

%%%===================================================================
%%% Migration functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function migrates old `storage_sync_monitoring` record to
%% `storage_import_monitoring` in version 1.
%% @end
%%--------------------------------------------------------------------
-spec migrate_to_v1(storage_sync_monitoring:record()) -> record().
migrate_to_v1({storage_sync_monitoring,
    FinishedScans,

    ImportStartTime,
    ImportFinishTime,
    LastUpdateStartTime,
    LastUpdateFinishTime,

    ToProcess,
    Imported,
    Updated,
    Deleted,
    Failed,
    OtherProcessed,

    ImportedSum,
    UpdatedSum,
    DeletedSum,

    ImportedMinHist,
    ImportedHourHist,
    ImportedDayHist,

    UpdatedMinHist,
    UpdatedHourHist,
    UpdatedDayHist,

    DeletedMinHist,
    DeletedHourHist,
    DeletedDayHist,

    QueueLengthMinHist,
    QueueLengthHourHist,
    QueueLengthDayHist
}) ->
    InProgressStatus = case ToProcess > 1 of
        true ->
            ?RUNNING;
        false when ToProcess =:= 1 ->
            case (Imported + Updated + Deleted + OtherProcessed + Failed) > 0 of
                true ->
                    ?RUNNING;
                false ->
                    ?ENQUEUED
            end;
        false ->
            ?ENQUEUED
    end,

    FinishedStatus = case ToProcess =:= (Imported + Updated + Deleted + OtherProcessed) of
        true -> ?COMPLETED;
        false -> ?FAILED
    end,

    {StartTime, StopTime, Status} = case
        {ImportStartTime, ImportFinishTime, LastUpdateStartTime, LastUpdateFinishTime}
    of
        {undefined, _, _, _} ->
            {undefined, undefined, ?ENQUEUED};
        {_, undefined, _, _} ->
            {ImportStartTime, undefined, InProgressStatus};
        {_, _, undefined, _} ->
            {ImportStartTime, ImportFinishTime, FinishedStatus};
        {_, _, _, undefined} ->
            {LastUpdateStartTime, ImportFinishTime, InProgressStatus};
        {_, _, _, _} when LastUpdateStartTime > LastUpdateFinishTime ->
            {LastUpdateStartTime, LastUpdateFinishTime, InProgressStatus};
        _ ->
            {LastUpdateStartTime, LastUpdateFinishTime, FinishedStatus}
    end,

    #storage_import_monitoring{
        finished_scans = FinishedScans,
        status = Status,

        scan_start_time = StartTime,
        scan_stop_time = StopTime,

        to_process = ToProcess,
        imported = Imported,
        updated = Updated,
        deleted = Deleted,
        failed = Failed,
        other_processed = OtherProcessed,

        imported_sum = ImportedSum,
        updated_sum = UpdatedSum,
        deleted_sum = DeletedSum,

        imported_min_hist = ImportedMinHist,
        imported_hour_hist = ImportedHourHist,
        imported_day_hist = ImportedDayHist,

        updated_min_hist = UpdatedMinHist,
        updated_hour_hist = UpdatedHourHist,
        updated_day_hist = UpdatedDayHist,

        deleted_min_hist = DeletedMinHist,
        deleted_hour_hist = DeletedHourHist,
        deleted_day_hist = DeletedDayHist,

        queue_length_min_hist = QueueLengthMinHist,
        queue_length_hour_hist = QueueLengthHourHist,
        queue_length_day_hist = QueueLengthDayHist
    }.
    

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
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
    % This model was renamed from storage_sync_monitoring.
    % Changes in comparison to the latest version of storage_sync_monitoring
    % are described in this function_clause.
    {record, [
        {finished_scans, integer},

        % field status was added in this version
        {status, atom},

        % fields:
        %  - import_start_time,
        %  - import_finish_time,
        %  - last_update_start_time,
        %  - last_update_finish_time
        % were removed in this version.

        % field scan_start_time was added in this version
        {scan_start_time, integer},
        % field scan_stop_time was added in this version
        {scan_stop_time, integer},

        {to_process, integer},
        {imported, integer},
        {updated, integer},
        {deleted, integer},
        {failed, integer},
        {other_processed, integer},

        {imported_sum, integer},
        {updated_sum, integer},
        {deleted_sum, integer},

        {imported_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {imported_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {imported_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {updated_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {updated_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {updated_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {deleted_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {deleted_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {deleted_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {queue_length_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {queue_length_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {queue_length_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}}
    ]}.
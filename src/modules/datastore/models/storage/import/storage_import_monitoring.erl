%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model responsible for storing monitoring data from auto storage
%%% import scans.
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
-export([prepare_new_scan/1, ensure_created/1]).
-export([
    increment_queue_length_histograms/2,
    mark_unmodified_files_and_increment_queue_length_histograms/3,
    mark_started_scan/1,
    mark_created_file/1,
    mark_modified_file/1,
    mark_deleted_file/1,
    mark_unmodified_file/1,
    mark_unmodified_files/2,
    mark_failed_file/1,
    mark_processed_job/1,
    mark_finished_scan/2,
    set_aborting_status/1
]).
-export([
    get_info/1,
    get_stats/3,
    is_scan_in_progress/1,
    is_initial_scan_finished/1,
    is_scan_finished/2,
    get_finished_scans_num/1,
    get_scan_stop_time/1
]).


% export for use/mocking in CT tests
-export([describe/1, update/2]).

%% datastore API
-export([get/1, get_or_create/1, create/2, delete/1]).

% export for migration
-export([migrate_to_v1/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-define(CTX, #{model => ?MODULE}).

% metric types
-define(QUEUE_LENGTH, <<"queueLength">>).
-define(CREATED_FILES, <<"createdFiles">>).
-define(MODIFIED_FILES, <<"modifiedFiles">>).
-define(DELETED_FILES, <<"deletedFiles">>).

-type key() :: od_space:id().
-type record() :: #storage_import_monitoring{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type timestamp() :: clock:seconds().
-type status() :: undefined | ?ENQUEUED | ?RUNNING | ?ABORTING | ?FAILED | ?COMPLETED | ?ABORTED.

-type window() :: day | hour | minute.
-type plot_counter_type() :: binary(). % ?QUEUE_LENGTH | ?CREATED_FILES | ?MODIFIED_FILES | ?DELETED_FILES.
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
-define(HOUR_HIST_SLOT, 3600 div ?HISTOGRAM_LENGTH).
-define(DAY_HIST_SLOT, 86400 div ?HISTOGRAM_LENGTH).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Prepares existing document for a new scan.
%% It resets control counters, sets ?ENQUEUED status and
%% increments queue_length histograms by 1.
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
                Timestamp = clock:timestamp_millis(),
                TimestampSecs = Timestamp div 1000,
                SIM2 = reset_queue_length_histograms(SIM, TimestampSecs),
                SIM3 = increment_queue_length_histograms(SIM2, TimestampSecs, 1),
                SIM4 = reset_counters(SIM3),
                {ok, SIM4#storage_import_monitoring{
                    status = ?ENQUEUED,
                    scan_start_time = Timestamp
                }}
        end
    end).


%%-------------------------------------------------------------------
%% @doc
%% Ensures that document for given SpaceId exists.
%% @end
%%-------------------------------------------------------------------
-spec ensure_created(od_space:id()) -> ok | {error, term()}.
ensure_created(SpaceId) ->
    case datastore_model:exists(?CTX, SpaceId) of
        {ok, true} ->
            ok;
        {ok, false} ->
            case create(new_doc(SpaceId)) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end;
        Error ->
            ?error("Failed to check whether storage_import_monitoring document for space ~s exists due to ~p.",
                [SpaceId, Error])
    end.


-spec mark_started_scan(key()) -> ok | {error, term()}.
mark_started_scan(SpaceId) ->
    ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM) ->
        {ok, SIM#storage_import_monitoring{
            status = ?RUNNING
        }}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file jobs were added to queue.
%% It increases suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec increment_queue_length_histograms(key(), non_neg_integer()) -> ok.
increment_queue_length_histograms(SpaceId, Value) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{}) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = maybe_proceed_to_running_status(SIM),
        {ok, increment_queue_length_histograms(SIM2, Timestamp, Value)}
    end)).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that there were unmodified files found
%% and new file jobs were added to queue.
%% It increases suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_unmodified_files_and_increment_queue_length_histograms(key(), non_neg_integer(), non_neg_integer()) -> ok.
mark_unmodified_files_and_increment_queue_length_histograms(SpaceId, NewUnmodifiedFilesNum, NewJobsToProcessNum) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        unmodified = CurrentUnmodifiedFilesNum
    }) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = maybe_proceed_to_running_status(SIM),
        SIM3 = SIM2#storage_import_monitoring{unmodified = CurrentUnmodifiedFilesNum + NewUnmodifiedFilesNum},
        {ok, increment_queue_length_histograms(SIM3, Timestamp, NewJobsToProcessNum)}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that creation of file has been
%% detected.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_created_file(key()) -> ok.
mark_created_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        created = CreatedFiles,
        created_min_hist = MinHist,
        created_hour_hist = HourHist,
        created_day_hist = DayHist
    }) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            created = CreatedFiles + 1,
            created_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            created_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            created_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SIM3 = maybe_proceed_to_running_status(SIM2),
        SIM4 = decrement_queue_length_histograms(SIM3, Timestamp),
        {ok, SIM4}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that modification of file has been
%% detected.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_modified_file(key()) -> ok.
mark_modified_file(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        modified = ModifiedFiles,
        modified_min_hist = MinHist,
        modified_hour_hist = HourHist,
        modified_day_hist = DayHist
    }) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            modified = ModifiedFiles + 1,
            modified_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            modified_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            modified_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SIM3 = maybe_proceed_to_running_status(SIM2),
        SIM4 = decrement_queue_length_histograms(SIM3, Timestamp),
        {ok, SIM4}
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
        deleted_min_hist = MinHist,
        deleted_hour_hist = HourHist,
        deleted_day_hist = DayHist
    }) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = SIM#storage_import_monitoring{
            deleted = DeletedFiles + 1,
            deleted_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            deleted_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            deleted_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SIM3 = maybe_proceed_to_running_status(SIM2),
        SIM4 = decrement_queue_length_histograms(SIM3, Timestamp),
        {ok, SIM4}
    end)).


-spec mark_unmodified_file(key()) -> ok.
mark_unmodified_file(SpaceId) ->
    mark_unmodified_files(SpaceId, 1).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been processed.
%% This function is used for marking processed files that does not match
%% to any of categories: created, modified, deleted, failed.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_unmodified_files(key(), FilesNum :: non_neg_integer()) -> ok.
mark_unmodified_files(SpaceId, NewUnmodifiedFilesNum) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{
        unmodified = UnmodifiedFilesNum
    }) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = maybe_proceed_to_running_status(SIM),
        SIM3 = SIM2#storage_import_monitoring{unmodified = UnmodifiedFilesNum + NewUnmodifiedFilesNum},
        SIM4 = decrement_queue_length_histograms(SIM3, Timestamp, NewUnmodifiedFilesNum),
        {ok, SIM4}
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
        Timestamp = clock:timestamp_seconds(),
        SIM2 = SIM#storage_import_monitoring{failed = FilesFailed + 1},
        SIM3 = maybe_proceed_to_running_status(SIM2),
        SIM4 = decrement_queue_length_histograms(SIM3, Timestamp),
        {ok, SIM4}
    end)).


%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that single import job has been
%% processed. It decreases queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_job(key()) -> ok.
mark_processed_job(SpaceId) ->
    ok = ?extract_ok(storage_import_monitoring:update(SpaceId, fun(SIM = #storage_import_monitoring{}) ->
        Timestamp = clock:timestamp_seconds(),
        SIM2 = maybe_proceed_to_running_status(SIM),
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
get_info(SIM = #storage_import_monitoring{
    finished_scans = Scans,
    status = Status,
    scan_start_time = StartTime,
    scan_stop_time = StopTime,
    created = CreatedFiles,
    modified = ModifiedFiles,
    deleted = DeletedFiles,
    unmodified = UnmodifiedFiles,
    failed = FailedFiles
}) ->
    Info = #{
        totalScans => Scans,
        status => utils:undefined_to_null(Status),
        createdFiles => CreatedFiles,
        modifiedFiles => ModifiedFiles,
        deletedFiles => DeletedFiles,
        unmodifiedFiles => UnmodifiedFiles,
        failedFiles => FailedFiles
    },
    Info2 = case StartTime =:= undefined of
        true -> Info;
        false -> Info#{start => StartTime div 1000}
    end,
    Info3 = case StopTime =:= undefined orelse is_scan_in_progress(SIM) of
        true -> Info2;
        false -> Info2#{stop => StopTime div 1000}
    end,
    {ok, Info3};
get_info(SpaceId) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, Doc} ->
            get_info(Doc);
        {error, _} = Error->
            Error
    end.


-spec get_stats(key() | record(), [plot_counter_type()], window()) -> {ok, import_stats()}.
get_stats(SIM = #storage_import_monitoring{}, Types, Window) ->
    {ok, return_histograms_and_timestamps(SIM, Types, Window)};
get_stats(SpaceId, Types, Window) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, #document{value = SIM}} ->
            get_stats(SIM, Types, Window);
        {error, not_found} ->
            ?debug("Failed to fetch storage import metrics for space ~p SpaceId due to not_found", [SpaceId]),
            {ok, return_empty_histograms_and_timestamps(Types)};
        Error ->
            ?error("Failed to fetch storage import metrics for space ~p SpaceId due to ~p", [SpaceId, Error]),
            {ok, return_empty_histograms_and_timestamps(Types)}
    end.


-spec is_scan_in_progress(key() | doc() | record()) -> boolean().
is_scan_in_progress(#document{value = SIM = #storage_import_monitoring{}}) ->
    is_scan_in_progress(SIM);
is_scan_in_progress(#storage_import_monitoring{status = ?ENQUEUED}) ->
    true;
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

-spec get_scan_stop_time(doc() | record()) -> {ok, clock:millis()} | undefined.
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
            created = Created,
            modified = Modified,
            deleted = Deleted,
            failed = Failed,
            unmodified = Unmodified,
            created_min_hist = CreatedMinHist,
            created_hour_hist = CreatedHourHist,
            created_day_hist = CreatedDayHist,
            modified_min_hist = ModifiedMinHist,
            modified_hour_hist = ModifiedHourHist,
            modified_day_hist = ModifiedDayHist,
            deleted_min_hist = DeletedMinHist,
            deleted_hour_hist = DeletedHourHist,
            deleted_day_hist = DeletedDayHist,
            queue_length_min_hist = QueueLengthMinHist,
            queue_length_hour_hist = QueueLengthHourHist,
            queue_length_day_hist = QueueLengthDayHist
        }
    }} = storage_import_monitoring:get(SpaceId),
    CreatedHistsTimestamp = get_histogram_timestamp(CreatedMinHist),
    ModifiedHistsTimestamp = get_histogram_timestamp(ModifiedMinHist),
    DeletedHistsTimestamp = get_histogram_timestamp(DeletedMinHist),
    QueueLengthHistsTimestamp = get_histogram_timestamp(QueueLengthMinHist),
    #{
        <<"scans">> => Scans,
        <<"status">> => Status,
        <<"scanStartTime">> => ScanStartTime,
        <<"scanStopTime">> => ScanStopTime,
        <<"created">> => Created,
        <<"modified">> => Modified,
        <<"deleted">> => Deleted,
        <<"failed">> => Failed,
        <<"unmodified">> => Unmodified,
        <<"createdMinHist">> => get_histogram_values(CreatedMinHist),
        <<"createdHourHist">> => get_histogram_values(CreatedHourHist),
        <<"createdDayHist">> => get_histogram_values(CreatedDayHist),
        <<"createdHistsTimestamp">> => CreatedHistsTimestamp,
        <<"modifiedMinHist">> => get_histogram_values(ModifiedMinHist),
        <<"modifiedHourHist">> => get_histogram_values(ModifiedHourHist),
        <<"modifiedDayHist">> => get_histogram_values(ModifiedDayHist),
        <<"modifiedHistsTimestamp">> => ModifiedHistsTimestamp,
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


-spec create(doc()) -> {ok, doc()} | error().
create(Doc) ->
    datastore_model:create(?CTX, Doc).


-spec create(key(), record()) -> {ok, doc()} | error().
create(SpaceId, SIM = #storage_import_monitoring{}) ->
    create(#document{key = SpaceId, value = SIM}).


-spec delete(key()) -> ok | error().
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec new_doc(key()) -> doc().
new_doc(SpaceId) ->
    #document{
        key = SpaceId,
        value = new_record(),
        scope = SpaceId
    }.


-spec new_record() -> record().
new_record() ->
    Timestamp = clock:timestamp_seconds(),
    EmptyMinHist = time_slot_histogram:new(Timestamp, ?MIN_HIST_SLOT, ?HISTOGRAM_LENGTH),
    EmptyHourHist = time_slot_histogram:new(Timestamp, ?HOUR_HIST_SLOT, ?HISTOGRAM_LENGTH),
    EmptyDayHist = time_slot_histogram:new(Timestamp, ?DAY_HIST_SLOT, ?HISTOGRAM_LENGTH),

    #storage_import_monitoring{
        status = undefined,

        created_min_hist = EmptyMinHist,
        created_hour_hist = EmptyHourHist,
        created_day_hist = EmptyDayHist,

        modified_min_hist = EmptyMinHist,
        modified_hour_hist = EmptyHourHist,
        modified_day_hist = EmptyDayHist,

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
%% This function reset all counters in
%% given storage_import_monitoring record.
%% @end
%%-------------------------------------------------------------------
-spec reset_counters(record()) -> record().
reset_counters(SIM) ->
    SIM#storage_import_monitoring{
        created = 0,
        modified = 0,
        deleted = 0,
        failed = 0,
        unmodified = 0
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


-spec decrement_queue_length_histograms(record(), timestamp()) ->
    record().
decrement_queue_length_histograms(SIM, Timestamp) ->
    decrement_queue_length_histograms(SIM, Timestamp, 1).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function decrements all queue_length histograms by given value.
%% @end
%%-------------------------------------------------------------------
-spec decrement_queue_length_histograms(record(), timestamp(), non_neg_integer()) ->
    record().
decrement_queue_length_histograms(SIM = #storage_import_monitoring{
    queue_length_min_hist = MinHist,
    queue_length_hour_hist = HourHist,
    queue_length_day_hist = DayHist
}, Timestamp, Value) ->
    SIM#storage_import_monitoring{
        queue_length_min_hist = time_slot_histogram:decrement(MinHist, Timestamp, Value),
        queue_length_hour_hist = time_slot_histogram:decrement(HourHist, Timestamp, Value),
        queue_length_day_hist = time_slot_histogram:decrement(DayHist, Timestamp, Value)
    }.


-spec mark_finished_scan_internal(record(), boolean()) -> record().
mark_finished_scan_internal(SIM = #storage_import_monitoring{
    finished_scans = Scans,
    failed = Failed
}, Aborted) ->
    case is_scan_in_progress(SIM) of
        true ->
            Timestamp = clock:timestamp_millis(),
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
        false ->
            % this should never happen
            ?error("Unexpected attempt to mark scan as finished while it is not in progress."),
            SIM
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of values of given histogram.
%% @end
%%-------------------------------------------------------------------
-spec get_histogram_values(time_slot_histogram:histogram()) -> histogram:histogram().
get_histogram_values(Histogram) ->
    time_slot_histogram:get_histogram_values(Histogram).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns last update timestamp of given histogram.
%% @end
%%-------------------------------------------------------------------
-spec get_histogram_timestamp(time_slot_histogram:histogram()) -> timestamp().
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


-spec return_empty_histograms_and_timestamps([plot_counter_type()]) -> import_stats().
return_empty_histograms_and_timestamps(Types) ->
    lists:foldl(fun(Type, AccIn) ->
        AccIn#{Type => return_empty_histogram_and_timestamp()}
    end, #{}, Types).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns empty histogram with given timestamp in the
%% format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec return_empty_histogram_and_timestamp() -> time_stats().
return_empty_histogram_and_timestamp() ->
    prepare(clock:timestamp_seconds(), histogram:new(?HISTOGRAM_LENGTH)).


-spec return_histograms_and_timestamps(record(), [plot_counter_type()], window()) ->
    import_stats().
return_histograms_and_timestamps(SIM, Types, Window) ->
    lists:foldl(fun(Type, AccIn) ->
        AccIn#{Type => return_histogram_and_timestamp(SIM, Type, Window)}
    end, #{}, Types).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns histogram with given timestamp in the
%% format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec return_histogram_and_timestamp(record(),
    plot_counter_type(), window()) -> json_utils:json_term().
return_histogram_and_timestamp(SIM, ?CREATED_FILES, minute) ->
    prepare(SIM#storage_import_monitoring.created_min_hist);
return_histogram_and_timestamp(SIM, ?CREATED_FILES, hour) ->
    prepare(SIM#storage_import_monitoring.created_hour_hist);
return_histogram_and_timestamp(SIM, ?CREATED_FILES, day) ->
    prepare(SIM#storage_import_monitoring.created_day_hist);
return_histogram_and_timestamp(SIM, ?MODIFIED_FILES, minute) ->
    prepare(SIM#storage_import_monitoring.modified_min_hist);
return_histogram_and_timestamp(SIM, ?MODIFIED_FILES, hour) ->
    prepare(SIM#storage_import_monitoring.modified_hour_hist);
return_histogram_and_timestamp(SIM, ?MODIFIED_FILES, day) ->
    prepare(SIM#storage_import_monitoring.modified_day_hist);
return_histogram_and_timestamp(SIM, ?DELETED_FILES, minute) ->
    prepare(SIM#storage_import_monitoring.deleted_min_hist);
return_histogram_and_timestamp(SIM, ?DELETED_FILES, hour) ->
    prepare(SIM#storage_import_monitoring.deleted_hour_hist);
return_histogram_and_timestamp(SIM, ?DELETED_FILES, day) ->
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
    Timestamp = clock:timestamp_seconds(),
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
        lastValueDate => time_format:seconds_to_iso8601(Timestamp),
        values => lists:reverse(Values)
    }.


-spec maybe_proceed_to_running_status(record()) -> record().
maybe_proceed_to_running_status(SIM = #storage_import_monitoring{status = ?ENQUEUED}) ->
    SIM#storage_import_monitoring{status = ?RUNNING};
maybe_proceed_to_running_status(SIM) ->
    SIM.

%%%===================================================================
%%% Migration functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function migrates old `storage_sync_monitoring` record to
%% `storage_import_monitoring` in version 1.
%% @end
%%--------------------------------------------------------------------
-spec migrate_to_v1(storage_sync_monitoring:record()) -> tuple().
migrate_to_v1(SSM) ->
    storage_import_monitoring_model:migrate_to_v1(SSM).

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
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(Version) ->
    storage_import_monitoring_model:get_record_struct(Version).

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(Version, Record) ->
    storage_import_monitoring_model:upgrade_record(Version, Record).
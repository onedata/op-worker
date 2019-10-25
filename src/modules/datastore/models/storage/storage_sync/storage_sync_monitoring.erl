%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model responsible for saving information about storage_sync scans
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_monitoring).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([ensure_created/2,
    prepare_new_import_scan/3,
    prepare_new_update_scan/3,
    delete/2, reset/2]).

-export([
    increase_to_process_counter/3,
    mark_imported_file/2,
    mark_updated_file/2,
    mark_deleted_file/2,
    mark_processed_file/2,
    mark_failed_file/2, mark_finished_scan/2]).

-export([get/2,
    get_import_status/1, get_import_status/2,
    get_update_status/1, get_update_status/2,
    get_info/2, get_metric/3,
    is_scan_in_progress/1, is_scan_in_progress/2,
    get_scans_num/1, get_scans_num/2,
    get_import_finish_time/1, get_last_update_finish_time/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{model => ?MODULE}).

-type id() :: datastore:key().
-type record() :: #storage_sync_monitoring{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type timestamp() :: non_neg_integer().

-type window() :: day | hour | minute.
-type plot_counter_type() :: imported_files | updated_files | deleted_files | queue_length.
-type error() :: {error, term()}.


-export_type([record/0, doc/0, window/0, plot_counter_type/0]).


-define(HISTOGRAM_LENGTH,
    application:get_env(?APP_NAME, storage_sync_histogram_length, 12)).
-define(MIN_HIST_SLOT, 60 div ?HISTOGRAM_LENGTH).
-define(HOUR_HIST_SLOT, 3600 div ?HISTOGRAM_LENGTH). % 3600s = 60 * 60s
-define(DAY_HIST_SLOT, 86400 div ?HISTOGRAM_LENGTH). % 86400s = 24 * 3600s


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Ensures that document for given SpaceId exists.
%% @end
%%-------------------------------------------------------------------
-spec ensure_created(od_space:id(), storage:id()) -> {ok, doc()} | {error, term()}.
ensure_created(SpaceId, StorageId) ->
    case datastore_model:exists(?CTX, id(SpaceId, StorageId)) of
        {ok, true} ->
            ok;
        {ok, false} ->
            case datastore_model:create(?CTX, new_doc(SpaceId, StorageId)) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end;
        Error ->
            ?error("Failed to check if storage_sync_monitoring document for space ~p and storage ~p exists due to ~p",
                [SpaceId, StorageId, Error])
    end.

%%-------------------------------------------------------------------
%% @doc
%% Prepares existing document for new import scan.
%% This function assumes that document has already been created by
%% ?MODULE:ensure_created/2 function.
%% It resets control counters, and increases to_process counter and
%% queue_length histograms by 1.
%% This function also sets scan start_time.
%% @end
%%-------------------------------------------------------------------
-spec prepare_new_import_scan(od_space:id(), storage:id(), non_neg_integer()) -> {ok, doc()}.
prepare_new_import_scan(SpaceId, StorageId, Timestamp) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM) ->
        SSM2 = reset_queue_length_histograms(SSM, Timestamp),
        SSM3 = increment_queue_length_histograms(SSM2, Timestamp, 1),
        SSM4 = reset_control_counters(SSM3),
        {ok, SSM4#storage_sync_monitoring{
            to_process = 1,
            import_start_time = Timestamp
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Prepares document for new update scan.
%% This function assumes that document has already been created by
%% ?MODULE:ensure_created/2 function.
%% It resets control counters, and increases to_process counter and
%% queue_length histograms by 1.
%% This function also sets scan start_time.
%% @end
%%-------------------------------------------------------------------
-spec prepare_new_update_scan(od_space:id(), storage:id(), non_neg_integer()) -> {ok, doc()}.
prepare_new_update_scan(SpaceId, StorageId, Timestamp) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM) ->
        SSM2 = reset_queue_length_histograms(SSM, Timestamp),
        SSM3 = increment_queue_length_histograms(SSM2, Timestamp, 1),
        SSM4 = reset_control_counters(SSM3),
        {ok, SSM4#storage_sync_monitoring{
            to_process = 1,
            last_update_start_time = Timestamp
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file jobs were added to queue.
%% It increases suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec increase_to_process_counter(od_space:id(), storage:id(),
    non_neg_integer()) -> ok.
increase_to_process_counter(SpaceId, StorageId, Value) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        to_process = FilesToProcess
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{to_process = FilesToProcess + Value},
        {ok, increment_queue_length_histograms(SSM2, Timestamp, Value)}
    end)).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been imported.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_imported_file(od_space:id(), storage:id()) -> ok.
mark_imported_file(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        imported = ImportedFiles,
        imported_sum = ImportedFilesSum,
        imported_min_hist = MinHist,
        imported_hour_hist = HourHist,
        imported_day_hist = DayHist
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{
            imported = ImportedFiles + 1,
            imported_sum = ImportedFilesSum + 1,
            imported_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            imported_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            imported_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, SSM3}
    end)).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been updated.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_updated_file(od_space:id(), storage:id()) -> ok.
mark_updated_file(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        updated = UpdatedFiles,
        updated_sum = UpdatedFilesSum,
        updated_min_hist = MinHist,
        updated_hour_hist = HourHist,
        updated_day_hist = DayHist
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{
            updated = UpdatedFiles + 1,
            updated_sum = UpdatedFilesSum + 1,
            updated_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            updated_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            updated_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, SSM3}
    end)).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been deleted.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_deleted_file(od_space:id(), storage:id()) -> ok.
mark_deleted_file(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        deleted = DeletedFiles,
        deleted_sum = DeletedFilesSum,
        deleted_min_hist = MinHist,
        deleted_hour_hist = HourHist,
        deleted_day_hist = DayHist
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{
            deleted = DeletedFiles + 1,
            deleted_sum = DeletedFilesSum + 1,
            deleted_min_hist = time_slot_histogram:increment(MinHist, Timestamp),
            deleted_hour_hist = time_slot_histogram:increment(HourHist, Timestamp),
            deleted_day_hist = time_slot_histogram:increment(DayHist, Timestamp)
        },
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, SSM3}
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
-spec mark_processed_file(od_space:id(), storage:id()) -> ok.
mark_processed_file(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        other_processed = FilesProcessed
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{other_processed = FilesProcessed + 1},
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, SSM3}
    end)).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that failure occurred when
%% processing file. It increases suitable counters and histograms and
%% decreases queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed_file(od_space:id(), storage:id()) -> ok.
mark_failed_file(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        failed = FilesFailed
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{failed = FilesFailed + 1},
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, SSM3}
    end)).

%%-------------------------------------------------------------------
%% @doc
%% Deletes document associated with given SpaceId and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec delete(od_space:id(), storage:id()) -> ok | error().
delete(SpaceId, StorageId) ->
    datastore_model:delete(?CTX, id(SpaceId, StorageId)).

%%-------------------------------------------------------------------
%% @doc
%% Returns
%% @end
%%-------------------------------------------------------------------
-spec get(od_space:id(), storage:id()) -> {ok, doc()}  | error().
get(SpaceId, StorageId) ->
    datastore_model:get(?CTX, id(SpaceId, StorageId)).

%%-------------------------------------------------------------------
%% @doc
%% Returns monitoring data converted to map.
%% @end
%%-------------------------------------------------------------------
get_info(SpaceId, StorageId) ->
    {ok, #document{
        value = #storage_sync_monitoring{
            scans = Scans,
            import_start_time = ImportStartTime,
            import_finish_time = ImportFinishTime,
            last_update_start_time = LastUpdateStartTime,
            last_update_finish_time = LastUpdateFinishTime,
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
    }} = ?MODULE:get(SpaceId, StorageId),
    ImportedHistsTimestamp = get_histogram_timestamp(ImportedMinHist),
    UpdatedHistsTimestamp = get_histogram_timestamp(UpdatedMinHist),
    DeletedHistsTimestamp = get_histogram_timestamp(DeletedMinHist),
    QueueLengthHistsTimestamp = get_histogram_timestamp(QueueLengthMinHist),
    #{
        <<"scans">> => Scans,
        <<"importStartTime">> => ImportStartTime,
        <<"importFinishTime">> => ImportFinishTime,
        <<"lastUpdateStartTime">> => LastUpdateStartTime,
        <<"lastUpdateFinishTime">> => LastUpdateFinishTime,
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

%%-------------------------------------------------------------------
%% @doc
%% Returns state of import. Possible values: not_started, in_progress, finished.
%% @end
%%-------------------------------------------
-spec get_import_status(od_space:id()) -> storage_sync_traverse:scan_status().
get_import_status(SpaceId) ->
    StorageId = space_storage:get_storage_id(SpaceId),
    get_import_status(SpaceId, StorageId).

-spec get_import_status(od_space:id(), storage:id()) -> storage_sync_traverse:scan_status().
get_import_status(SpaceId, StorageId) ->
    case get(SpaceId, StorageId) of
        {ok, #document{
            value = #storage_sync_monitoring{
                import_start_time = ImportStartTime,
                import_finish_time = ImportFinishTime
            }
        }} ->
            case {ImportStartTime, ImportFinishTime} of
                {undefined, undefined} ->
                    not_started;
                {_, undefined} ->
                    in_progress;
                {ImportStartTime, ImportFinishTime} when ImportStartTime =/= undefined ->
                    finished
            end;
        {error, not_found} ->
            not_started
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns state of update. Possible values: not_started, in_progress, finished.
%% @end
%%-------------------------------------------------------------------
-spec get_update_status(od_space:id() | record() | doc()) -> storage_sync_traverse:scan_status().
get_update_status(#document{value = SSM = #storage_sync_monitoring{}}) ->
    get_update_status(SSM);
get_update_status(#storage_sync_monitoring{
    last_update_start_time = undefined,
    last_update_finish_time = undefined
}) ->
    not_started;
get_update_status(#storage_sync_monitoring{
    last_update_finish_time = undefined
}) ->
    in_progress;
get_update_status(#storage_sync_monitoring{
    last_update_start_time = LastUpdateStartTime,
    last_update_finish_time = LastUpdateFinishTime
}) when (LastUpdateStartTime =/= undefined) andalso (LastUpdateStartTime > LastUpdateFinishTime) ->
    in_progress;
get_update_status(#storage_sync_monitoring{
    last_update_start_time = LastUpdateStartTime
}) when (LastUpdateStartTime =/= undefined) ->
    finished;
get_update_status(SpaceId) when is_binary(SpaceId) ->
    StorageId = space_storage:get_storage_id(SpaceId),
    get_update_status(SpaceId, StorageId).

-spec get_update_status(od_space:id(), storage:id()) -> storage_sync_traverse:scan_status().
get_update_status(SpaceId, StorageId) ->
    case get(SpaceId, StorageId) of
        {ok, Doc} -> get_update_status(Doc);
        {error, not_found} -> not_started
    end.

-spec get_scans_num(doc() | record()) -> {ok, non_neg_integer()}.
get_scans_num(#storage_sync_monitoring{scans = Scans}) ->
    {ok, Scans};
get_scans_num(#document{value = SSM}) ->
    get_scans_num(SSM).

-spec get_scans_num(od_space:id(), storage:id()) -> {ok, non_neg_integer()} | error().
get_scans_num(SpaceId, StorageId) ->
    case get(SpaceId, StorageId) of
        {ok, Doc} ->
            get_scans_num(Doc);
        Error = {error, _}->
            Error
    end.

-spec get_import_finish_time(doc() | record()) -> non_neg_integer() | undefined.
get_import_finish_time(#storage_sync_monitoring{import_finish_time = ImportFinishTime}) ->
    ImportFinishTime;
get_import_finish_time(#document{value = SSM}) ->
    get_import_finish_time(SSM).

-spec get_last_update_finish_time(doc() | record()) -> non_neg_integer() | undefined.
get_last_update_finish_time(#storage_sync_monitoring{last_update_finish_time = ImportFinishTime}) ->
    ImportFinishTime;
get_last_update_finish_time(#document{value = SSM}) ->
    get_last_update_finish_time(SSM).

%%-------------------------------------------------------------------
%% @doc
%% Returns values and last measurement timestamp for given metric.
%% @end
%%-------------------------------------------------------------------
-spec get_metric(od_space:id(), plot_counter_type(), window()) -> proplists:proplist().
get_metric(SpaceId, Type, Window) ->
    StorageId = space_storage:get_storage_id(SpaceId),
    case ?MODULE:get(SpaceId, StorageId) of
        {ok, #document{value = SSM}} ->
            return_histogram_and_timestamp(SSM, Type, Window);
        {error, not_found} ->
            ?debug("Failed to fetch storage sync metrics for space ~p SpaceId due to not_found", [SpaceId]),
            return_empty_histogram_and_timestamp();
        Error ->
            ?error("Failed to fetch storage sync metrics for space ~p SpaceId due to ~p", [SpaceId, Error]),
            return_empty_histogram_and_timestamp()
    end.

-spec reset(od_space:id(), storage:id()) -> ok.
reset(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM) ->
        case is_scan_in_progress(SSM) of
            true ->
                SSM2 = case SSM of
                    #storage_sync_monitoring{import_finish_time = undefined} ->
                        SSM#storage_sync_monitoring{import_finish_time = time_utils:cluster_time_seconds()};
                    _ ->
                        SSM#storage_sync_monitoring{last_update_finish_time = time_utils:cluster_time_seconds()}
                end,
                {ok, reset_control_counters(SSM2)};
            false ->
                {ok, SSM}
        end
    end)).


-spec mark_finished_scan(od_space:id(), storage:id()) -> ok.
mark_finished_scan(SpaceId, StorageId) ->
    ok = ?extract_ok(update(SpaceId, StorageId, fun(SSM) -> {ok, mark_finished_scan(SSM)} end)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns id of storage_sync_monitoring document for given SpaceId
%% and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec id(od_space:id(), storage:id()) -> id().
id(SpaceId, StorageId) ->
    datastore_utils:gen_key(SpaceId, StorageId).

-spec update(od_space:id(), storage:id(), diff()) -> {ok, doc()} | error().
update(SpaceId, StorageId, Diff) ->
    datastore_model:update(?CTX, id(SpaceId, StorageId), Diff).


-spec new_doc(od_space:id(), storage:id()) -> doc().
new_doc(SpaceId, StorageId) ->
    new_doc(SpaceId, StorageId, time_utils:cluster_time_seconds()).

-spec new_doc(od_space:id(), storage:id(), non_neg_integer()) -> doc().
new_doc(SpaceId, StorageId, Timestamp) ->
    EmptyMinHist = time_slot_histogram:new(Timestamp, ?MIN_HIST_SLOT, ?HISTOGRAM_LENGTH),
    EmptyHourHist = time_slot_histogram:new(Timestamp, ?HOUR_HIST_SLOT, ?HISTOGRAM_LENGTH),
    EmptyDayHist = time_slot_histogram:new(Timestamp, ?DAY_HIST_SLOT, ?HISTOGRAM_LENGTH),

    #document{
        key = id(SpaceId, StorageId),
        value = #storage_sync_monitoring{
            imported_min_hist = EmptyMinHist,
            imported_hour_hist = EmptyHourHist,
            imported_day_hist = EmptyDayHist,

            updated_min_hist = EmptyMinHist,
            updated_hour_hist = EmptyHourHist,
            updated_day_hist = EmptyDayHist,

            deleted_min_hist = EmptyMinHist,
            deleted_hour_hist = EmptyHourHist,
            deleted_day_hist = EmptyDayHist,

            queue_length_min_hist =
            time_slot_histogram:new_cumulative(Timestamp, ?MIN_HIST_SLOT, ?HISTOGRAM_LENGTH),
            queue_length_hour_hist =
            time_slot_histogram:new_cumulative(Timestamp, ?HOUR_HIST_SLOT, ?HISTOGRAM_LENGTH),
            queue_length_day_hist =
            time_slot_histogram:new_cumulative(Timestamp, ?DAY_HIST_SLOT, ?HISTOGRAM_LENGTH)
        },
        scope = SpaceId
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function reset all control counters in
%% given storage_sync_monitoring record.
%% @end
%%-------------------------------------------------------------------
-spec reset_control_counters(record()) -> record().
reset_control_counters(SSM) ->
    SSM#storage_sync_monitoring{
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
increment_queue_length_histograms(SSM = #storage_sync_monitoring{
    queue_length_min_hist = MinHist,
    queue_length_hour_hist = HourHist,
    queue_length_day_hist = DayHist
}, Timestamp, Value) ->
    SSM#storage_sync_monitoring{
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
decrement_queue_length_histograms(SSM = #storage_sync_monitoring{
    queue_length_min_hist = MinHist,
    queue_length_hour_hist = HourHist,
    queue_length_day_hist = DayHist
}, Timestamp) ->
    SSM#storage_sync_monitoring{
        queue_length_min_hist = time_slot_histogram:decrement(MinHist, Timestamp),
        queue_length_hour_hist = time_slot_histogram:decrement(HourHist, Timestamp),
        queue_length_day_hist = time_slot_histogram:decrement(DayHist, Timestamp)
    }.

-spec mark_finished_scan(record()) -> record().
mark_finished_scan(SSM = #storage_sync_monitoring{
    import_finish_time = undefined
}) ->
    mark_finished_import_scan(SSM);
mark_finished_scan(SSM = #storage_sync_monitoring{
    last_update_start_time = LastUpdateStartTime,
    last_update_finish_time = LastUpdateFinishTime
}) when LastUpdateStartTime > LastUpdateFinishTime ->
    mark_finished_update_scan(SSM);
mark_finished_scan(SSM = #storage_sync_monitoring{
    last_update_finish_time = undefined
}) ->
    mark_finished_update_scan(SSM);
mark_finished_scan(SSM) ->
    SSM.

-spec mark_finished_import_scan(record()) -> record().
mark_finished_import_scan(SSM = #storage_sync_monitoring{
    scans = Scans,
    import_finish_time = undefined
}) ->
    Timestamp = time_utils:cluster_time_seconds(),
    SSM2 = SSM#storage_sync_monitoring{
        scans = Scans + 1,
        import_finish_time = Timestamp
    },
    reset_queue_length_histograms(SSM2, Timestamp).


-spec mark_finished_update_scan(record()) -> record().
mark_finished_update_scan(SSM = #storage_sync_monitoring{
    scans = Scans
}) ->
    Timestamp = time_utils:cluster_time_seconds(),
    SSM2 = SSM#storage_sync_monitoring{
        scans = Scans + 1,
        last_update_finish_time = Timestamp
    },
    reset_queue_length_histograms(SSM2, Timestamp).

-spec is_scan_in_progress(doc() | record()) -> boolean().
is_scan_in_progress(#document{value = SSM = #storage_sync_monitoring{}}) ->
    is_scan_in_progress(SSM);
is_scan_in_progress(#storage_sync_monitoring{import_start_time = undefined}) ->
    false;
is_scan_in_progress(#storage_sync_monitoring{import_finish_time = undefined}) ->
    true;
is_scan_in_progress(#storage_sync_monitoring{last_update_start_time = undefined}) ->
    false;
is_scan_in_progress(#storage_sync_monitoring{last_update_finish_time = undefined}) ->
    true;
is_scan_in_progress(SSM = #storage_sync_monitoring{
    last_update_start_time = LastUpdateStartTime,
    last_update_finish_time = LastUpdateFinishTime
}) ->
    LastUpdateStartTime > LastUpdateFinishTime orelse get_unhandled_jobs_value(SSM) > 0.

-spec is_scan_in_progress(od_space:id(), storage:id()) -> boolean().
is_scan_in_progress(SpaceId, StorageId) ->
    case get(SpaceId, StorageId) of
        {error, not_found} ->
            false;
        {ok, Doc} ->
            is_scan_in_progress(Doc)
    end.

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
%% This function returns number of unhandled jobs.
%% @end
%%-------------------------------------------------------------------
-spec get_unhandled_jobs_value(record()) -> integer().
get_unhandled_jobs_value(#storage_sync_monitoring{
    to_process = ToProcess,
    imported = Imported,
    updated = Updated,
    deleted = Deleted,
    other_processed = OtherProcessed,
    failed = Failed
}) ->
    ToProcess - Imported - Updated - Deleted - OtherProcessed - Failed.

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
reset_queue_length_histograms(SSM = #storage_sync_monitoring{
    queue_length_min_hist = QueueLengthMinHist,
    queue_length_hour_hist = QueueLengthHourHist,
    queue_length_day_hist = QueueLengthDayHist
}, Timestamp) ->
    QueueLengthMinHist2 = time_slot_histogram:reset_cumulative(QueueLengthMinHist, Timestamp),
    QueueLengthHourHist2 = time_slot_histogram:reset_cumulative(QueueLengthHourHist, Timestamp),
    QueueLengthDayHist2 = time_slot_histogram:reset_cumulative(QueueLengthDayHist, Timestamp),
    SSM#storage_sync_monitoring{
        queue_length_min_hist = QueueLengthMinHist2,
        queue_length_hour_hist = QueueLengthHourHist2,
        queue_length_day_hist = QueueLengthDayHist2
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns empty histogram with given timestamp in the
%% format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec return_empty_histogram_and_timestamp() -> proplists:proplist().
return_empty_histogram_and_timestamp() ->
    prepare(time_utils:cluster_time_seconds(), histogram:new(?HISTOGRAM_LENGTH)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns histogram with given timestamp in the
%% format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec return_histogram_and_timestamp(record(),
    plot_counter_type(), window()) -> proplists:proplist().
return_histogram_and_timestamp(SSM, imported_files, minute) ->
    prepare(SSM#storage_sync_monitoring.imported_min_hist);
return_histogram_and_timestamp(SSM, imported_files, hour) ->
    prepare(SSM#storage_sync_monitoring.imported_hour_hist);
return_histogram_and_timestamp(SSM, imported_files, day) ->
    prepare(SSM#storage_sync_monitoring.imported_day_hist);
return_histogram_and_timestamp(SSM, updated_files, minute) ->
    prepare(SSM#storage_sync_monitoring.updated_min_hist);
return_histogram_and_timestamp(SSM, updated_files, hour) ->
    prepare(SSM#storage_sync_monitoring.updated_hour_hist);
return_histogram_and_timestamp(SSM, updated_files, day) ->
    prepare(SSM#storage_sync_monitoring.updated_day_hist);
return_histogram_and_timestamp(SSM, deleted_files, minute) ->
    prepare(SSM#storage_sync_monitoring.deleted_min_hist);
return_histogram_and_timestamp(SSM, deleted_files, hour) ->
    prepare(SSM#storage_sync_monitoring.deleted_hour_hist);
return_histogram_and_timestamp(SSM, deleted_files, day) ->
    prepare(SSM#storage_sync_monitoring.deleted_day_hist);
return_histogram_and_timestamp(SSM, queue_length, minute) ->
    prepare(SSM#storage_sync_monitoring.queue_length_min_hist);
return_histogram_and_timestamp(SSM, queue_length, hour) ->
    prepare(SSM#storage_sync_monitoring.queue_length_hour_hist);
return_histogram_and_timestamp(SSM, queue_length, day) ->
    prepare(SSM#storage_sync_monitoring.queue_length_day_hist).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of Values and Timestamp from given TimeSlotHistogram
%% in the format acceptable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec prepare(time_slot_histogram:histogram()) -> proplists:proplist().
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
-spec prepare(timestamp(), [integer()]) -> proplists:proplist().
prepare(Timestamp, Values) ->
    [
        {timestamp, Timestamp},
        {values, lists:reverse(Values)}
    ].

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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {scans, integer},
        {import_start_time, integer},
        {import_finish_time, integer},
        {last_update_start_time, integer},
        {last_update_finish_time, integer},

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
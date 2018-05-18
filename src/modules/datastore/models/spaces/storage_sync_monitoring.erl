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
    delete/2]).

-export([
    increase_to_process_counter/3,
    mark_imported_file/2,
    mark_updated_file/2,
    mark_deleted_file/2,
    mark_processed_file/2,
    mark_failed_file/2
]).

-export([get/2,
    get_import_state/1, get_update_state/1,
    get_previous_sync_timestamps/2, get_info/2, get_metric/3
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    routing => global
}).

-type id() :: binary().
-type storage_sync_monitoring() :: #storage_sync_monitoring{}.
-type doc() :: datastore:doc(storage_sync_monitoring()).
-type diff() :: datastore_doc:diff(storage_sync_monitoring()).
-type timestamp() :: non_neg_integer().

-type window() :: day | hours | minute.
-type plot_counter_type() :: imported_files | updated_files | deleted_files | queue_length.
-type error() :: {error, term()}.


-export_type([storage_sync_monitoring/0, doc/0]).


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
        Error  ->
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
-spec prepare_new_import_scan(od_space:id(), storage:id(), non_neg_integer()) ->
    {ok, doc()}.
prepare_new_import_scan(SpaceId, StorageId, Timestamp) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM) ->
        SSM2 = increment_queue_length_histograms(SSM, Timestamp, 1),
        SSM3 = reset_control_counters(SSM2),
        {ok, SSM3#storage_sync_monitoring{
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
-spec prepare_new_update_scan(od_space:id(), storage:id(), non_neg_integer()) ->
    {ok, doc()}.
prepare_new_update_scan(SpaceId, StorageId, Timestamp) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM) ->
        SSM2 = increment_queue_length_histograms(SSM, Timestamp, 1),
        SSM3 = reset_control_counters(SSM2),
        {ok, SSM3#storage_sync_monitoring{
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
    non_neg_integer()) -> {ok, doc()}.
increase_to_process_counter(SpaceId, StorageId, Value) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        to_process = FilesToProcess
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{to_process = FilesToProcess + Value},
        {ok, increment_queue_length_histograms(SSM2, Timestamp, Value)}
    end).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been imported.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_imported_file(od_space:id(), storage:id()) -> {ok, doc()}.
mark_imported_file(SpaceId, StorageId) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
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
        {ok, maybe_mark_finished_scan(SSM3)}
    end).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been updated.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_updated_file(od_space:id(), storage:id()) -> {ok, doc()}.
mark_updated_file(SpaceId, StorageId) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
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
        {ok, maybe_mark_finished_scan(SSM3)}
    end).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been deleted.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_deleted_file(od_space:id(), storage:id()) -> {ok, doc()}.
mark_deleted_file(SpaceId, StorageId) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
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
        {ok, maybe_mark_finished_scan(SSM3)}
    end).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that file has been processed.
%% This function is used for marking processed files that does not match
%% to any of categories: imported, updated, deleted, failed.
%% It increases suitable counters and histograms and decreases
%% queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_file(od_space:id(), storage:id()) -> {ok, doc()}.
mark_processed_file(SpaceId, StorageId) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        other_processed = FilesProcessed
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{other_processed = FilesProcessed + 1},
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, maybe_mark_finished_scan(SSM3)}
    end).

%%-------------------------------------------------------------------
%% @doc
%% This function marks in document that failure occurred when
%% processing file. It increases suitable counters and histograms and
%% decreases queue_length histograms.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed_file(od_space:id(), storage:id()) -> {ok, doc()}.
mark_failed_file(SpaceId, StorageId) ->
    {ok, _} = update(SpaceId, StorageId, fun(SSM = #storage_sync_monitoring{
        failed = FilesFailed
    }) ->
        Timestamp = time_utils:cluster_time_seconds(),
        SSM2 = SSM#storage_sync_monitoring{failed = FilesFailed + 1},
        SSM3 = decrement_queue_length_histograms(SSM2, Timestamp),
        {ok, maybe_mark_finished_scan(SSM3)}
    end).

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
%% This function returns timestamps of previous import and update scans.
%% It takes into consideration whether it is a FirstRun (first after
%% start/restart of provider).
%% @end
%%-------------------------------------------------------------------
-spec get_previous_sync_timestamps(storage_sync_monitoring(),
    FirstRun :: boolean()) -> tuple().
get_previous_sync_timestamps(#storage_sync_monitoring{
    import_start_time = _ImportStartTime,
    import_finish_time = undefined
}, true) ->
    % no previous import or import was in progress when provider was restarted
    {undefined, undefined, undefined, undefined};
get_previous_sync_timestamps(#storage_sync_monitoring{
    import_start_time = ImportStartTime,
    import_finish_time = ImportFinishTime,
    last_update_start_time = _LastUpdateStartTime,
    last_update_finish_time = undefined
}, true) ->
    % there was no update or update was in progress when provider was stopped
    {ImportStartTime, ImportFinishTime, undefined, undefined};
get_previous_sync_timestamps(#storage_sync_monitoring{
    import_start_time = ImportStartTime,
    import_finish_time = ImportFinishTime,
    last_update_start_time = LastUpdateStartTime,
    last_update_finish_time = LastUpdateFinishTime
}, true) when LastUpdateFinishTime < LastUpdateStartTime ->
    % there was update in progress when provider was stopped
    {ImportStartTime, ImportFinishTime, undefined, undefined};
get_previous_sync_timestamps(SSM = #storage_sync_monitoring{}, true) ->
    get_previous_sync_timestamps(SSM, false);
get_previous_sync_timestamps(#storage_sync_monitoring{
    import_start_time = ImportStartTime,
    import_finish_time = ImportFinishTime,
    last_update_start_time = LastUpdateStartTime,
    last_update_finish_time = LastUpdateFinishTime
}, false) ->
    {ImportStartTime, ImportFinishTime, LastUpdateStartTime, LastUpdateFinishTime}.

%%-------------------------------------------------------------------
%% @doc
%% Returns state of import. Possible values: not_started, in_progress, finished.
%% @end
%%-------------------------------------------
-spec get_import_state(od_space:id()) -> storage_import:state().
get_import_state(SpaceId) ->
    StorageId = get_storage_id(SpaceId),
    {ok, #document{
        value = #storage_sync_monitoring{
            import_start_time = ImportStartTime,
            import_finish_time = ImportFinishTime
        }
    }} = ?MODULE:get(SpaceId, StorageId),
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
    StorageId = get_storage_id(SpaceId),
    {ok, #document{
        value = #storage_sync_monitoring{
            last_update_start_time = LastUpdateStartTime,
            last_update_finish_time = LastUpdateFinishTime
        }
    }} = ?MODULE:get(SpaceId, StorageId),
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
-spec get_metric(od_space:id(), plot_counter_type(), window()) -> proplists:proplist().
get_metric(SpaceId, Type, Window) ->
    StorageId = get_storage_id(SpaceId),
    case ?MODULE:get(SpaceId, StorageId) of
        {ok, #document{value = SSM}} ->
            return_histogram_and_timestamp(SSM, Type, Window);
        Error ->
            ?error_stacktrace("Failed to fetch storage sync metrics for space ~p SpaceId due to ~p",
                [SpaceId, Error]),
            return_empty_histogram_and_timestamp()
    end.

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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv datastore_model:update(?CTX, id(SpaceId, StorageId), Diff).
%% @end
%%-------------------------------------------------------------------
-spec update(od_space:id(), storage:id(), diff()) -> {ok, doc()} | error().
update(SpaceId, StorageId, Diff) ->
    datastore_model:update(?CTX, id(SpaceId, StorageId), Diff).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns initialized storage_sync_monitoring doc.
%% @end
%%-------------------------------------------------------------------
-spec new_doc(od_space:id(), storage:id()) -> doc().
new_doc(SpaceId, StorageId) ->
    Timestamp = time_utils:cluster_time_seconds(),
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
-spec reset_control_counters(storage_sync_monitoring()) -> storage_sync_monitoring().
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
-spec increment_queue_length_histograms(storage_sync_monitoring(), timestamp(),
    non_neg_integer()) -> storage_sync_monitoring().
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
-spec decrement_queue_length_histograms(storage_sync_monitoring(), timestamp()) ->
    storage_sync_monitoring().
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

%%-------------------------------------------------------------------
%% @doc
%% This function returns StorageId for given SpaceId.
%% WARNING!!! After allowing to support one space with many storages
%% on one provider, this function will be useless !!!
%% @end
%%-------------------------------------------------------------------
-spec get_storage_id(od_space:id()) -> storage:id().
get_storage_id(SpaceId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = StorageStrategies
        }}} = space_strategies:get(SpaceId),
    hd(maps:keys(StorageStrategies)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions checks whether scan has been finished. If true
%% it updates suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec maybe_mark_finished_scan(storage_sync_monitoring()) -> storage_sync_monitoring().
maybe_mark_finished_scan(SSM = #storage_sync_monitoring{
    import_finish_time = undefined
}) ->
    maybe_mark_finished_import_scan(SSM);
maybe_mark_finished_scan(SSM = #storage_sync_monitoring{
    last_update_start_time = LastUpdateStartTime,
    last_update_finish_time = LastUpdateFinishTime
}) when  LastUpdateStartTime > LastUpdateFinishTime ->
    maybe_mark_finished_update_scan(SSM);
maybe_mark_finished_scan(SSM = #storage_sync_monitoring{
    last_update_finish_time = undefined
}) ->
    maybe_mark_finished_update_scan(SSM);
maybe_mark_finished_scan(SSM) ->
    SSM.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions checks whether import scan has been finished. If true
%% it updates suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec maybe_mark_finished_import_scan(storage_sync_monitoring())
        -> storage_sync_monitoring().
maybe_mark_finished_import_scan(SSM = #storage_sync_monitoring{
    scans = Scans,
    import_finish_time = undefined
}) ->
    case get_unhandled_jobs_value(SSM) of
        0 ->
            SSM#storage_sync_monitoring{
                scans = Scans + 1,
                import_finish_time = time_utils:cluster_time_seconds()
            };
        _ ->
            SSM
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions checks whether update scan has been finished. If true
%% it updates suitable counters and histograms.
%% @end
%%-------------------------------------------------------------------
-spec maybe_mark_finished_update_scan(storage_sync_monitoring())
        -> storage_sync_monitoring().
maybe_mark_finished_update_scan(SSM = #storage_sync_monitoring{
    scans = Scans
}) ->
    case get_unhandled_jobs_value(SSM) of
        0 ->
            SSM#storage_sync_monitoring{
                scans = Scans + 1,
                last_update_finish_time = time_utils:cluster_time_seconds()
            };
        _ ->
            SSM
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
-spec get_unhandled_jobs_value(storage_sync_monitoring()) -> integer().
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
-spec return_histogram_and_timestamp(storage_sync_monitoring(),
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
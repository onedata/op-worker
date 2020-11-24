%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains datastore callbacks for storage_import_monitoring model.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_monitoring_model).
-author("Jakub Kudzia").

-include("modules/storage/import/storage_import.hrl").


% export for migration
-export([migrate_to_v1/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-define(STORAGE_IMPORT_MONITORING_MODEL, storage_import_monitoring).

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
migrate_to_v1({storage_sync_monitoring,
    FinishedScans,

    ImportStartTime0,
    ImportFinishTime0,
    LastUpdateStartTime0,
    LastUpdateFinishTime0,

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
    % recalculate timestamps to millis as start/stop timestamps are now stored in millis
    ImportStartTime = timestamp_to_millis(ImportStartTime0),
    ImportFinishTime = timestamp_to_millis(ImportFinishTime0),
    LastUpdateStartTime = timestamp_to_millis(LastUpdateStartTime0),
    LastUpdateFinishTime = timestamp_to_millis(LastUpdateFinishTime0),

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

    {?STORAGE_IMPORT_MONITORING_MODEL,
        FinishedScans,
        Status,

        StartTime,
        StopTime,

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
    }.


-spec timestamp_to_millis(clock:seconds() | undefined) -> clock:millis() | undefined.
timestamp_to_millis(undefined) -> undefined;
timestamp_to_millis(TimestampSecs) -> TimestampSecs * 1000.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

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

        % field scan_start_time was added in this version and is currently stored in millis
        {scan_start_time, integer},
        % field scan_stop_time was added in this version and is currently stored in millis
        {scan_stop_time, integer},

        {to_process, integer},
        % field imported was renamed to created in this version
        {created, integer},
        % field updated was renamed to modified in this version
        {modified, integer},
        {deleted, integer},
        {failed, integer},
        {other_processed, integer},

        % field imported_sum was renamed to created_sum in this version
        {created_sum, integer},
        % field updated_sum was renamed to modified_sum in this version
        {modified_sum, integer},
        {deleted_sum, integer},

        % field imported_min_hist was renamed to created_min_hist in this version
        {created_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        % field imported_hour_hist was renamed to created_hour_hist in this version
        {created_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        % field imported_day_hist was renamed to created_day_hist in this version
        {created_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        % field updated_min_hist was renamed to modified_min_hist in this version
        {modified_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        % field updated_hour_hist was renamed to modified_hour_hist in this version
        {modified_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        % field updated_day_hist was renamed to modified_day_hist in this version
        {modified_day_hist, {record, [
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
    ]};
get_record_struct(2) ->
    {record, [
        {finished_scans, integer},
        {status, atom},
        {scan_start_time, integer},
        {scan_stop_time, integer},

        % field to_process was removed in this version
        {created, integer},
        {modified, integer},
        {deleted, integer},
        {failed, integer},
        % field other_processed was removed in this version
        % field unmodified was added in this version
        {unmodified, integer},

        % field created_sum was removed in this version
        % field modified_sum was removed in this version
        % field deleted_sum was removed in this version

        {created_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {created_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {created_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {modified_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {modified_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {modified_day_hist, {record, [
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

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @endstorage_import_monitoring
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?STORAGE_IMPORT_MONITORING_MODEL, 
    FinishedScans, 
    Status, 
    
    ScanStartTIme,
    ScanStopTime,

    _ToProcess, % field to_process was removed in this version
    Created,
    Modified,
    Deleted,
    Failed,
    _OtherProcessed, % field other_processed was removed in this version
    
    _CreatedSum, % field create_sum was removed in this version
    _ModifiedSum, % field modified_sum was removed in this version
    _DeletedSum, % field deleted_sum was removed in this version
    
    CreatedMinHist,
    CreatedHourHist,
    CreatedDayHist,

    ModifiedMinHist,
    ModifiedHourHist,
    ModifiedDayHist,

    DeletedMinHist,
    DeletedHourHist,
    DeletedDayHist,

    QueueLengthMinHist,
    QueueLengthHourHist,
    QueueLengthDayHist
}) ->
    {2, {?STORAGE_IMPORT_MONITORING_MODEL,
        FinishedScans,
        Status,

        ScanStartTIme,
        ScanStopTime,

        Created,
        Modified,
        Deleted,
        Failed,
        0, % field unmodified was added in this version

        CreatedMinHist,
        CreatedHourHist,
        CreatedDayHist,

        ModifiedMinHist,
        ModifiedHourHist,
        ModifiedDayHist,

        DeletedMinHist,
        DeletedHourHist,
        DeletedDayHist,

        QueueLengthMinHist,
        QueueLengthHourHist,
        QueueLengthDayHist
    }}.
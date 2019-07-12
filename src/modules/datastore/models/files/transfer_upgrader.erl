%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains record definition of transfer model and
%%% upgrader function for this model.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_upgrader).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

-define(TRANSFER_MODEL, transfer).

%% datastore_model callbacks
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {path, string},
        {callback, string},
        {transfer_status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {start_time, integer},
        {last_update, integer},
        {min_hist, [integer]},
        {hr_hist, [integer]},
        {dy_hist, [integer]}
    ]};
get_record_struct(2) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {path, string},
        {callback, string},
        {transfer_status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {start_time, integer},
        {last_update, integer},
        {min_hist, [integer]},
        {hr_hist, [integer]},
        {dy_hist, [integer]}
    ]};
get_record_struct(3) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(4) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {failed_files, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(5) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {schedule_provider_id, string},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_transferred, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(6) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {schedule_provider_id, string},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_transferred, integer},
        {bytes_transferred, integer},
        {files_invalidated, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(7) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {status, atom},
        {invalidation_status, atom},
        {schedule_provider_id, string},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_transferred, integer},
        {bytes_transferred, integer},
        {files_invalidated, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(8) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {rerun_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {cancel, atom},
        {replication_status, atom},
        {eviction_status, atom},
        {schedule_provider_id, string},
        {replicating_provider, string},
        {evicting_provider, string},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_replicated, integer},
        {bytes_replicated, integer},
        {files_evicted, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(9) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {rerun_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {cancel, atom},
        {replication_status, atom},
        {eviction_status, atom},
        {scheduling_provider, string},
        {replicating_provider, string},
        {evicting_provider, string},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_replicated, integer},
        {bytes_replicated, integer},
        {files_evicted, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(10) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {rerun_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {cancel, atom},
        {replication_status, atom},
        {eviction_status, atom},
        {scheduling_provider, string},
        {replicating_provider, string},
        {evicting_provider, string},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_replicated, integer},
        {bytes_replicated, integer},
        {files_evicted, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}},
        {index_name, string},
        {query_view_params, [{term, term}]}
    ]};
get_record_struct(11) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {rerun_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {cancel, atom},
        {replication_status, atom},
        {eviction_status, atom},
        {scheduling_provider, string},
        {replicating_provider, string},
        {evicting_provider, string},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_replicated, integer},
        {bytes_replicated, integer},
        {files_evicted, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}},
        {index_name, string},
        {query_view_params, [{term, term}]},
        {qos_job_pid, string}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?TRANSFER_MODEL, FileUuid, SpaceId, Path, CallBack, TransferStatus,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, StartTime, LastUpdate,
    MinHist, HrHist, DyHist}
) ->
    {2, {?TRANSFER_MODEL, FileUuid, SpaceId, Path, CallBack, TransferStatus,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        0, 0, BytesToTransfer, BytesTransferred, StartTime, LastUpdate,
        MinHist, HrHist, DyHist
    }};
upgrade_record(2, {?TRANSFER_MODEL, FileUuid, SpaceId, Path, CallBack, TransferStatus,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    FilesToInvalidate, FilesInvalidated, BytesToTransfer, BytesTransferred,
    StartTime, LastUpdate, MinHist, HrHist, DyHist}
) ->
    {3, {?TRANSFER_MODEL, FileUuid, SpaceId, undefined, Path, CallBack, TransferStatus,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
        StartTime, LastUpdate,
        % There are three changes in histograms:
        %   1) They are now maps #{ProviderId => Histogram}, where ProviderId is
        %       the provider FROM which the amount of data expressed in the
        %       histogram was transferred.
        %   2) Histogram naming convention - minute histogram is now a histogram
        %       that SPANS OVER one minute, here with 5 seconds window.
        %       Other histograms are renamed analogically.
        %   3) LastUpdate must be remembered per provider to correctly keep
        %       track in histograms.
        % As there is no way to deduce source providers, older transfers will
        % only have one histogram accessible under target provider id.
        % last_update
        #{TargetProviderId => LastUpdate},
        % min_hist
        #{TargetProviderId => lists:duplicate(60 div ?FIVE_SEC_TIME_WINDOW, 0)},
        %hr_hist
        #{TargetProviderId => MinHist},
        % dy_hist
        #{TargetProviderId => HrHist},
        % mth_hist
        #{TargetProviderId => DyHist}
    }};
upgrade_record(3, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {4, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred, 0,
        BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
        StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
    }};
upgrade_record(4, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred, FailedFiles,
    _BytesToTransfer, BytesTransferred, _FilesToInvalidate, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {5, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
        InvalidationStatus, SourceProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
    }};
upgrade_record(5, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
    FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {6, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
        InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        StartTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
        MthHist
    }};
upgrade_record(6, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
    FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
    ScheduleTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
    MthHist
}) ->
    {7, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, true, Status,
        InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        ScheduleTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
        MthHist
    }};
upgrade_record(7, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, Path, CallBack, Enqueued,
    Status, InvalidationStatus, SchedulingProviderId, SourceProviderId,
    TargetProviderId, _InvalidateSourceReplica, Pid, FilesToProcess,
    FilesProcessed, FailedFiles, FilesTransferred, BytesTransferred,
    FilesInvalidated, ScheduleTime, StartTime, FinishTime,
    LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {8, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, undefined, Path, CallBack, Enqueued,
        false, Status, InvalidationStatus, SchedulingProviderId,
        TargetProviderId, SourceProviderId, Pid, FilesToProcess,
        FilesProcessed, FailedFiles, FilesTransferred, BytesTransferred,
        FilesInvalidated, ScheduleTime, StartTime, FinishTime,
        LastUpdate, MinHist, HrHist, DyHist, MthHist
    }};
upgrade_record(8, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, RerunId, Path, CallBack, Enqueued,
    Cancel, ReplicationStatus, EvictionStatus, SchedulingProvider,
    ReplicatingProvider, EvictingProvider, Pid, FilesToProcess,
    FilesProcessed, FailedFiles, FilesReplicated, BytesReplicated,
    FilesEvicted, ScheduleTime, StartTime, FinishTime,
    LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {9, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, RerunId, Path, CallBack, Enqueued,
        Cancel, ReplicationStatus, EvictionStatus, SchedulingProvider,
        ReplicatingProvider, EvictingProvider, Pid, FilesToProcess,
        FilesProcessed, FailedFiles, FilesReplicated, BytesReplicated,
        FilesEvicted, ScheduleTime, StartTime, FinishTime,
        LastUpdate, MinHist, HrHist, DyHist, MthHist
    }};
upgrade_record(9, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, RerunId, Path, CallBack, Enqueued,
    Cancel, ReplicationStatus, EvictionStatus, SchedulingProvider,
    ReplicatingProvider, EvictingProvider, Pid, FilesToProcess,
    FilesProcessed, FailedFiles, FilesReplicated, BytesReplicated,
    FilesEvicted, ScheduleTime, StartTime, FinishTime,
    LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {10, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, RerunId, Path, CallBack, Enqueued,
        Cancel, ReplicationStatus, EvictionStatus, SchedulingProvider,
        ReplicatingProvider, EvictingProvider, Pid, FilesToProcess,
        FilesProcessed, FailedFiles, FilesReplicated, BytesReplicated,
        FilesEvicted, ScheduleTime, StartTime, FinishTime,
        LastUpdate, MinHist, HrHist, DyHist, MthHist, undefined, []
    }};
upgrade_record(10, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, RerunId, Path, CallBack, Enqueued,
    Cancel, ReplicationStatus, EvictionStatus, SchedulingProvider,
    ReplicatingProvider, EvictingProvider, Pid, FilesToProcess,
    FilesProcessed, FailedFiles, FilesReplicated, BytesReplicated,
    FilesEvicted, ScheduleTime, StartTime, FinishTime,
    LastUpdate, MinHist, HrHist, DyHist, MthHist, IndexName, QueryViewParams
}) ->
    {11, {?TRANSFER_MODEL, FileUuid, SpaceId, UserId, RerunId, Path, CallBack, Enqueued,
        Cancel, ReplicationStatus, EvictionStatus, SchedulingProvider,
        ReplicatingProvider, EvictingProvider, Pid, FilesToProcess,
        FilesProcessed, FailedFiles, FilesReplicated, BytesReplicated,
        FilesEvicted, ScheduleTime, StartTime, FinishTime,
        LastUpdate, MinHist, HrHist, DyHist, MthHist, IndexName, QueryViewParams,
        undefined
    }}.

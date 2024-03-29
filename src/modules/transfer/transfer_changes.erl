%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling changes on transfer documents. The
%%% callback is called for all changes - remote (dbsync) and local (posthook).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_changes).
-author("Jakub Kudzia").

-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle/1]).

-define(run_if_is_self(ProviderId, F),
    case oneprovider:is_self(ProviderId) of
        true ->
            F();
        false ->
            ok
    end
).

-define(decode_pid(__PID), utils:decode_pid(__PID)).

-define(MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER,
    op_worker:get_env(max_file_transfer_failures_per_transfer, 10)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Callback called when transfer doc is modified (via dbsync or local provider).
%% Calls appropriate handler depending on transfer status if document wasn't
%% yet deleted (deleted docs are not removed from db but kept with 'deleted'
%% set to 'true' instead). Otherwise ignores changes.
%% @end
%%--------------------------------------------------------------------
-spec handle(transfer:doc()) -> ok.
handle(#document{deleted = true}) ->
    ok;

handle(Doc = #document{value = #transfer{
    replication_status = ?SCHEDULED_STATUS,
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        handle_scheduled_replication(Doc)
    end);

handle(Doc = #document{value = #transfer{
    replication_status = ?ENQUEUED_STATUS,
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        handle_enqueued_replication(Doc)
    end);

handle(Doc = #document{
    value = #transfer{
        replication_status = Status,
        enqueued = true
    }
}) when Status =/= ?SKIPPED_STATUS ->
    handle_dequeued_transfer(Doc);

handle(Doc = #document{value = #transfer{
    replication_status = ?ACTIVE_STATUS,
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        handle_active_replication(Doc)
    end);

handle(Doc = #document{value = #transfer{
    replication_status = ?ABORTING_STATUS,
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        handle_aborting_replication(Doc)
    end);

handle(Doc = #document{
    value = #transfer{
        replication_status = ReplicationStatus,
        eviction_status = ?SCHEDULED_STATUS,
        evicting_provider = EvictingProviderId
    }
}) when
    ReplicationStatus == ?COMPLETED_STATUS;
    ReplicationStatus == ?SKIPPED_STATUS
->
    ?run_if_is_self(EvictingProviderId, fun() ->
        handle_scheduled_replica_eviction(Doc)
    end);

handle(Doc = #document{value = #transfer{
    eviction_status = ?ENQUEUED_STATUS,
    evicting_provider = EvictingProviderId
}}) ->
    ?run_if_is_self(EvictingProviderId, fun() ->
        handle_enqueued_replica_eviction(Doc)
    end);

handle(Doc = #document{
    value = #transfer{
        replication_status = ?SKIPPED_STATUS,
        enqueued = true
    }
}) ->
    handle_dequeued_transfer(Doc);

handle(Doc = #document{value = #transfer{
    eviction_status = ?ACTIVE_STATUS,
    evicting_provider = EvictingProviderId
}}) ->
    ?run_if_is_self(EvictingProviderId, fun() ->
        handle_active_replica_eviction(Doc)
    end);

handle(Doc = #document{value = #transfer{
    eviction_status = ?ABORTING_STATUS,
    evicting_provider = EvictingProviderId
}}) ->
    ?run_if_is_self(EvictingProviderId, fun() ->
        handle_aborting_replica_eviction(Doc)
    end);

handle(Doc = #document{
    value = #transfer{
        replication_status = ReplicationStatus,
        eviction_status = EvictionStatus
    }
}) when ReplicationStatus =/= ?SKIPPED_STATUS andalso
    (EvictionStatus =:= ?COMPLETED_STATUS orelse
     EvictionStatus =:= ?FAILED_STATUS orelse
     EvictionStatus =:= ?CANCELLED_STATUS)
->
    handle_finished_migration(Doc);

handle(_Doc) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with replication_status = scheduled
%% is modified.
%% Starts new replication/migration or cancel it depending on cancel flag.
%% This will be done only by provider that performs replication.
%% @end
%%--------------------------------------------------------------------
-spec handle_scheduled_replication(transfer:doc()) -> ok.
handle_scheduled_replication(#document{key = TransferId, value = #transfer{
    cancel = true
}}) ->
    replication_status:handle_cancelled(TransferId),
    ok;

handle_scheduled_replication(Doc) ->
    new_replication_or_migration(Doc).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with replication_status = enqueued
%% is modified.
%% Depending on cancel flag and statistics about transferred bytes/files
%% does nothing or proceeds to active/aborting state.
%% This will be done only by provider that performs replication.
%% @end
%%--------------------------------------------------------------------
-spec handle_enqueued_replication(transfer:doc()) -> ok.
handle_enqueued_replication(Doc = #document{value = #transfer{cancel = true}}) ->
    abort_replication(Doc, cancellation);

handle_enqueued_replication(#document{key = TransferId, value = #transfer{
    replication_traverse_finished = TraverseFinished,
    files_processed = FilesProcessed,
    bytes_replicated = BytesReplicated
}}) when
    TraverseFinished;
    FilesProcessed > 0;
    BytesReplicated > 0
->
    replication_controller:mark_active(TransferId);

handle_enqueued_replication(_) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with replication_status = active
%% is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_active_replication(transfer:doc()) -> ok.
handle_active_replication(Doc = #document{value = #transfer{cancel = true}}) ->
    abort_replication(Doc, cancellation);

handle_active_replication(#document{key = TransferId, value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    replication_traverse_finished = true,
    failed_files = 0
}}) when FilesProcessed >= FilesToProcess ->
    replication_controller:mark_completed(TransferId);

handle_active_replication(#document{key = TransferId, value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    replication_traverse_finished = true
}}) when FilesProcessed >= FilesToProcess ->
    replication_controller:mark_aborting(TransferId, exceeded_number_of_failed_files);

handle_active_replication(#document{key = TransferId, value = #transfer{
    failed_files = FailedFiles
}}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER of
        true ->
            replication_controller:mark_aborting(
                TransferId, exceeded_number_of_failed_files
            );
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies replication controller about aborting replication.
%% This will be done only by provider that performs replication.
%% In case of dead controller process, directly marks replication as aborting.
%% @end
%%--------------------------------------------------------------------
-spec abort_replication(transfer:doc(), Reason :: term()) -> ok.
abort_replication(#document{key = TransferId}, Reason) ->
    case replication_controller:is_alive(TransferId) of
        true -> replication_controller:mark_aborting(TransferId, Reason);
        false -> replication_status:handle_aborting(TransferId)
    end,
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies replication controller about cancelled (if cancel flag is set) or
%% failed (if cancel flag is unset) replication or changes status manually if
%% controller process is dead.
%% This will be done only by provider that performs replication.
%% @end
%%--------------------------------------------------------------------
-spec handle_aborting_replication(transfer:doc()) -> ok.
handle_aborting_replication(#document{key = TransferId, value = #transfer{
    cancel = Cancel,
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    replication_traverse_finished = true
}}) when FilesProcessed >= FilesToProcess ->
    case {Cancel, replication_controller:is_alive(TransferId)} of
        {true, true} ->
            replication_controller:mark_cancelled(TransferId);
        {true, false} ->
            replication_status:handle_cancelled(TransferId);
        {false, true} ->
            replication_controller:mark_failed(TransferId);
        {false, false} ->
            replication_status:handle_failed(TransferId, false)
    end,
    ok;

handle_aborting_replication(_) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts replica_eviction or cancel it depending on cancel flag.
%% In case of starting, due to transfer doc conflict resolution and possible
%% races, this function can be called multiple times. To avoid spawning
%% multiple replica_eviction controllers, try to mark replica_eviction as
%% enqueued and spawn controller only if it succeed.
%% This will be done only by provider that performs replica_eviction.
%% @end
%%--------------------------------------------------------------------
-spec handle_scheduled_replica_eviction(transfer:doc()) -> ok.
handle_scheduled_replica_eviction(#document{key = TransferId, value = #transfer{
    cancel = true
}}) ->
    replica_eviction_status:handle_cancelled(TransferId),
    ok;

handle_scheduled_replica_eviction(Doc = #document{key = TransferId}) ->
    case replica_eviction_status:handle_enqueued(TransferId) of
        {ok, _} ->
            new_replica_eviction(Doc);
        {error, _Error} ->
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If cancel flag is set, cancels replica_eviction. Otherwise do nothing.
%% This will be done only by provider that performs replica_eviction.
%% @end
%%--------------------------------------------------------------------
-spec handle_enqueued_replica_eviction(transfer:doc()) -> ok.
handle_enqueued_replica_eviction(#document{key = TransferId, value = #transfer{
    cancel = true
}}) ->
    replica_eviction_status:handle_cancelled(TransferId),
    ok;

handle_enqueued_replica_eviction(_) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with eviction_status = active
%% is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_active_replica_eviction(transfer:doc()) -> ok.
handle_active_replica_eviction(#document{key = TransferId, value = #transfer{
    cancel = true
}}) ->
    case replica_eviction_controller:is_alive(TransferId) of
        true ->
            replica_eviction_controller:mark_aborting(TransferId, cancellation);
        false ->
            replica_eviction_status:handle_aborting(TransferId)
    end,
    ok;

handle_active_replica_eviction(#document{key = TransferId, value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    eviction_traverse_finished = true,
    failed_files = 0
}}) when FilesProcessed >= FilesToProcess ->
    replica_eviction_controller:mark_completed(TransferId);

handle_active_replica_eviction(#document{key = TransferId, value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    eviction_traverse_finished = true
}}) when FilesProcessed >= FilesToProcess ->
    replica_eviction_controller:mark_aborting(TransferId, exceeded_number_of_failed_files);

handle_active_replica_eviction(#document{key = TransferId, value = #transfer{
    failed_files = FailedFiles
}}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER of
        true ->
            replica_eviction_controller:mark_aborting(
                TransferId, exceeded_number_of_failed_files
            );
        false ->
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies replica_eviction_controller about cancelled (if cancel flag is set)
%% or failed (if cancel flag is unset) transfer or if controller is dead
%% manually changes status.
%% This will be done only by provider that performs eviction.
%% @end
%%--------------------------------------------------------------------
-spec handle_aborting_replica_eviction(transfer:doc()) -> ok.
handle_aborting_replica_eviction(#document{key = TransferId, value = #transfer{
    cancel = Cancel,
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    eviction_traverse_finished = true
}}) when FilesProcessed >= FilesToProcess ->
    case {Cancel, replica_eviction_controller:is_alive(TransferId)} of
        {true, true} ->
            replica_eviction_controller:mark_cancelled(TransferId);
        {false, true} ->
            replica_eviction_controller:mark_failed(TransferId);
        {true, false} ->
            replica_eviction_status:handle_cancelled(TransferId);
        {false, false} ->
            replica_eviction_status:handle_failed(TransferId, false)
    end,
    ok;

handle_aborting_replica_eviction(_) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes transfer from scheduled links tree and unset enqueued flag.
%% This will be done only by provider that scheduled transfer and added it to
%% scheduled links tree, namely scheduling provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_dequeued_transfer(transfer:doc()) -> ok.
handle_dequeued_transfer(#document{key = TransferId, value = #transfer{
    scheduling_provider = SchedulingProviderId
}}) ->
    ?run_if_is_self(SchedulingProviderId, fun() ->
        transfer:mark_dequeued(TransferId)
    end),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes transfer from active links tree in case of finished migration
%% (after finished eviction).
%% This will be done only by provider that carried replication and added
%% given transfer to active links tree, namely target provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_finished_migration(transfer:doc()) -> ok.
handle_finished_migration(Doc = #document{value = #transfer{
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        transfer_links:move_from_ongoing_to_ended(Doc)
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new transfer based on existing doc synchronized from other provider.
%% @end
%%--------------------------------------------------------------------
-spec new_replication_or_migration(transfer:doc()) -> ok.
new_replication_or_migration(#document{
    key = TransferId,
    value = Transfer = #transfer{callback = Callback}
}) ->
    worker_pool:cast(
        ?REPLICATION_CONTROLLERS_POOL,
        {start_replication, TransferId, Callback, transfer:is_migration(Transfer)}
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new replica_eviction based on existing doc synchronized
%% from other provider.
%% @end
%%--------------------------------------------------------------------
-spec new_replica_eviction(transfer:doc()) -> ok.
new_replica_eviction(#document{
    key = TransferId,
    value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        callback = Callback,
        replicating_provider = TargetProviderId,
        index_name = ViewName,
        query_view_params = QueryViewParams
    }
}) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    {ok, _Pid} = gen_server2:start(replica_eviction_controller,
        [session_utils:root_session_id(),
            TransferId, FileGuid, Callback, TargetProviderId,
            ViewName, QueryViewParams],
        []),
    ok.

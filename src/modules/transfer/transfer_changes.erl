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

-define(decode_pid(__PID), transfer_utils:decode_pid(__PID)).

-define(MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER,
    application:get_env(?APP_NAME, max_file_transfer_failures_per_transfer, 10)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Callback called when transfer doc is modified (via dbsync or local provider)
%% @end
%%--------------------------------------------------------------------
-spec handle(transfer:doc()) -> ok.
handle(Doc = #document{value = #transfer{replication_status = scheduled}}) ->
    handle_scheduled_replication(Doc);

handle(Doc = #document{value = #transfer{replication_status = enqueued}}) ->
    handle_enqueued_replication(Doc);

handle(Doc = #document{value = #transfer{
    replication_status = Status,
    enqueued = true
}}) when Status =/= skipped ->
    handle_dequeued_transfer(Doc);

handle(Doc = #document{value = #transfer{replication_status = active}}) ->
    handle_active_replication(Doc);

handle(Doc = #document{value = #transfer{replication_status = aborting}}) ->
    handle_aborting_replication(Doc);

handle(Doc = #document{value = #transfer{
    replication_status = ReplicationStatus,
    invalidation_status = scheduled
}}) when ReplicationStatus == completed orelse ReplicationStatus == skipped ->
    handle_scheduled_invalidation(Doc);

handle(Doc = #document{value = #transfer{invalidation_status = enqueued}}) ->
    handle_enqueued_invalidation(Doc);

handle(Doc = #document{value = #transfer{
    replication_status = skipped,
    enqueued = true
}}) ->
    handle_dequeued_transfer(Doc);

handle(Doc = #document{value = #transfer{invalidation_status = active}}) ->
    handle_active_invalidation(Doc);

handle(Doc = #document{value = #transfer{invalidation_status = aborting}}) ->
    handle_aborting_invalidation(Doc);

handle(Doc = #document{value = #transfer{
    replication_status = ReplicationStatus,
    invalidation_status = InvalidationStatus
}}) when ReplicationStatus =/= skipped andalso
    (InvalidationStatus =:= completed orelse
     InvalidationStatus =:= failed orelse
     InvalidationStatus =:= cancelled)
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
handle_scheduled_replication(Doc = #document{
    key = TransferId,
    value = #transfer{
        replication_status = scheduled,
        replicating_provider = ReplicatingProviderId,
        cancel = Cancel
    }
}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        case Cancel of
            true ->
                replication_status:handle_cancelled(TransferId);
            false ->
                new_replication_or_migration(Doc)
        end
    end).


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
handle_enqueued_replication(Doc = #document{value = #transfer{
    replication_status = enqueued,
    replicating_provider = ReplicatingProviderId,
    cancel = Cancel,
    files_processed = FilesProcessed,
    bytes_replicated = BytesReplicated,
    pid = Pid
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        case Cancel of
            true ->
                abort_replication(Doc, cancellation);
            false ->
                case {FilesProcessed, BytesReplicated} > {0, 0} of
                    true ->
                        replication_controller:mark_active(?decode_pid(Pid));
                    false ->
                        ok
                end
        end
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with replication_status = active
%% is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_active_replication(transfer:doc()) -> ok.
handle_active_replication(Doc = #document{value = #transfer{
    cancel = true,
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        abort_replication(Doc, cancellation)
    end);

handle_active_replication(#document{value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesToProcess,
    failed_files = 0,
    replicating_provider = ReplicatingProviderId,
    pid = Pid
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        replication_controller:mark_completed(?decode_pid(Pid))
    end);

handle_active_replication(#document{value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesToProcess,
    replicating_provider = ReplicatingProviderId,
    pid = Pid
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        replication_controller:mark_aborting(
            ?decode_pid(Pid), exceeded_number_of_failed_files)
    end);

handle_active_replication(#document{value = #transfer{
    failed_files = FailedFiles,
    replicating_provider = ReplicatingProviderId,
    pid = Pid
}}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER of
        true ->
            ?run_if_is_self(ReplicatingProviderId, fun() ->
                replication_controller:mark_aborting(
                    ?decode_pid(Pid), exceeded_number_of_failed_files)
            end);
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
abort_replication(#document{key = TransferId, value = Transfer}, Reason) ->
    DecodedPid = ?decode_pid(Transfer#transfer.pid),
    case is_process_alive(DecodedPid) of
        true ->
            replication_controller:mark_aborting(DecodedPid, Reason);
        false ->
            replication_status:handle_aborting(TransferId)
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
    replicating_provider = ReplicatingProviderId,
    pid = Pid
}}) ->
    case FilesProcessed >= FilesToProcess of
        false ->
            ok;
        true ->
            ?run_if_is_self(ReplicatingProviderId, fun() ->
                DecodedPid = ?decode_pid(Pid),
                case {Cancel, is_process_alive(DecodedPid)} of
                    {true, true} ->
                        replication_controller:mark_cancelled(DecodedPid);
                    {true, false} ->
                        replication_status:handle_cancelled(TransferId);
                    {false, true} ->
                        replication_controller:mark_failed(DecodedPid);
                    {false, false} ->
                        replication_status:handle_failed(TransferId, false)
                end
            end)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts invalidation or cancel it depending on cancel flag.
%% In case of starting, due to transfer doc conflict resolution and possible
%% races, this function can be called multiple times. To avoid spawning
%% multiple invalidation controllers, try to mark invalidation as enqueued and
%% spawn controller only if it succeed.
%% This will be done only by provider that performs invalidation.
%% @end
%%--------------------------------------------------------------------
-spec handle_scheduled_invalidation(transfer:doc()) -> ok.
handle_scheduled_invalidation(Doc = #document{
    key = TransferId,
    value = #transfer{
        invalidation_status = scheduled,
        invalidating_provider = InvalidatingProviderId,
        cancel = Cancel
    }
}) ->
    ?run_if_is_self(InvalidatingProviderId, fun() ->
        case Cancel of
            true ->
                invalidation_status:handle_cancelled(TransferId);
            false ->
                case invalidation_status:handle_enqueued(TransferId) of
                    {ok, _} ->
                        new_invalidation(Doc);
                    {error, _Error} ->
                        ok
                end
        end
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If cancel flag is set, cancels invalidation. Otherwise do nothing.
%% This will be done only by provider that performs invalidation.
%% @end
%%--------------------------------------------------------------------
-spec handle_enqueued_invalidation(transfer:doc()) -> ok.
handle_enqueued_invalidation(#document{
    key = TransferId,
    value = #transfer{
        invalidation_status = enqueued,
        invalidating_provider = InvalidatingProviderId,
        cancel = Cancel
    }
}) ->
    ?run_if_is_self(InvalidatingProviderId, fun() ->
        case Cancel of
            true -> invalidation_status:handle_cancelled(TransferId);
            false -> ok
        end
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with invalidation_status = active
%% is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_active_invalidation(transfer:doc()) -> ok.
handle_active_invalidation(#document{key = TransferId, value = #transfer{
    cancel = true,
    invalidating_provider = InvalidatingProviderId,
    pid = Pid
}}) ->
    ?run_if_is_self(InvalidatingProviderId, fun() ->
        DecodedPid = ?decode_pid(Pid),
        case is_process_alive(DecodedPid) of
            true ->
                invalidation_controller:mark_aborting(DecodedPid, cancellation);
            false ->
                invalidation_status:handle_aborting(TransferId)
        end
    end);

handle_active_invalidation(#document{value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesToProcess,
    failed_files = 0,
    invalidating_provider = InvalidatingProviderId,
    pid = Pid
}}) ->
    ?run_if_is_self(InvalidatingProviderId, fun() ->
        invalidation_controller:mark_completed(?decode_pid(Pid))
    end);

handle_active_invalidation(#document{value = #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesToProcess,
    invalidating_provider = InvalidatingProviderId,
    pid = Pid
}}) ->
    ?run_if_is_self(InvalidatingProviderId, fun() ->
        invalidation_controller:mark_aborting(
            ?decode_pid(Pid), exceeded_number_of_failed_files)
    end);

handle_active_invalidation(#document{value = #transfer{
    failed_files = FailedFiles,
    invalidating_provider = InvalidatingProviderId,
    pid = Pid
}}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER of
        true ->
            ?run_if_is_self(InvalidatingProviderId, fun() ->
                invalidation_controller:mark_aborting(
                    ?decode_pid(Pid), exceeded_number_of_failed_files)
            end);
        false ->
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies invalidation controller about cancelled (if cancel flag is set) or
%% failed (if cancel flag is unset) transfer or if controller is dead manually
%% changes status.
%% This will be done only by provider that performs invalidation.
%% @end
%%--------------------------------------------------------------------
-spec handle_aborting_invalidation(transfer:doc()) -> ok.
handle_aborting_invalidation(#document{key = TransferId, value = #transfer{
    cancel = Cancel,
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    invalidating_provider = InvalidatingProviderId,
    pid = Pid
}}) ->
    case FilesProcessed >= FilesToProcess of
        false ->
            ok;
        true ->
            ?run_if_is_self(InvalidatingProviderId, fun() ->
                DecodedPid = ?decode_pid(Pid),
                case {Cancel, is_process_alive(DecodedPid)} of
                    {true, true} ->
                        invalidation_controller:mark_cancelled(DecodedPid);
                    {false, true} ->
                        invalidation_controller:mark_failed(DecodedPid);
                    {true, false} ->
                        invalidation_status:handle_cancelled(TransferId);
                    {false, false} ->
                        invalidation_status:handle_failed(TransferId, false)
                end
            end)
    end.


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
    scheduling_provider_id = SchedulingProviderId
}}) ->
    ?run_if_is_self(SchedulingProviderId, fun() ->
        transfer:mark_dequeued(TransferId)
    end),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes transfer from active links tree in case of finished migration
%% (after finished invalidation).
%% This will be done only by provider that carried replication and added
%% given transfer to active links tree, namely target provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_finished_migration(transfer:doc()) -> ok.
handle_finished_migration(Doc = #document{value = #transfer{
    replicating_provider = ReplicatingProviderId
}}) ->
    ?run_if_is_self(ReplicatingProviderId, fun() ->
        transfer_links:move_transfer_link_from_ongoing_to_ended(Doc)
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
    value = Transfer = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        callback = Callback
    }
}) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    worker_pool:cast(?REPLICATION_CONTROLLERS_POOL, {
        start_replication,
        session:root_session_id(),
        TransferId,
        FileGuid,
        Callback,
        transfer:is_migration(Transfer)
    }).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new invalidation based on existing doc synchronized
%% from other provider.
%% @end
%%--------------------------------------------------------------------
-spec new_invalidation(transfer:doc()) -> ok.
new_invalidation(#document{
    key = TransferId,
    value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        callback = Callback,
        replicating_provider = TargetProviderId
    }
}) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    {ok, _Pid} = gen_server2:start(invalidation_controller,
        [session:root_session_id(), TransferId, FileGuid, Callback, TargetProviderId], []),
    ok.

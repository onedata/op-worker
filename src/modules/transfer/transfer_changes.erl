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

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
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
handle(TransferDoc = #document{value = #transfer{status = scheduled}}) ->
    handle_scheduled_transfer(TransferDoc);
handle(TransferDoc = #document{value = #transfer{
    status = Status,
    enqueued = true
}}) when Status =/= enqueued -> % scheduled is covered by top function case
    handle_dequeued_transfer(TransferDoc);
handle(TransferDoc = #document{
    value = #transfer{
        status = TransferStatus,
        invalidation_status = scheduled,
        invalidate_source_replica = true
    }})
    when TransferStatus == completed orelse TransferStatus == skipped ->
    handle_scheduled_invalidation(TransferDoc);
handle(TransferDoc = #document{
    value = #transfer{
        status = active
    }}) ->
    handle_active_transfer(TransferDoc);
handle(TransferDoc = #document{
    value = #transfer{
        invalidation_status = active
    }}) ->
    handle_active_invalidation(TransferDoc);
handle(TransferDoc = #document{
    value = #transfer{
        invalidation_status = InvalidationStatus,
        invalidate_source_replica = true
    }}) when
        InvalidationStatus =:= completed orelse
        InvalidationStatus =:= failed orelse
        InvalidationStatus =:= cancelled
    ->
    handle_finished_invalidation(TransferDoc);
handle(_ExistingTransferDoc) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with status=scheduled is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_scheduled_transfer(transfer:doc()) -> ok.
handle_scheduled_transfer(TransferDoc = #document{
    key = TransferId,
    value = #transfer{
        space_id = SpaceId,
        source_provider_id = SourceProviderId,
        target_provider_id = TargetProviderId,
        finish_time = FinishTime,
        invalidate_source_replica = true
}}) ->
    % new migration
    case oneprovider:get_id() of
        TargetProviderId ->
            % ensure that there is no duplicate in past transfers tree
            transfer_links:delete_past_transfer_link(TransferId, SpaceId,
                FinishTime),
            new_transfer(TransferDoc);
        SourceProviderId ->
            % ensure that there is no duplicate in past transfers tree
            transfer_links:delete_past_transfer_link(TransferId, SpaceId,
                FinishTime);
        _ ->
            ok
    end;
handle_scheduled_transfer(TransferDoc = #document{
    value = #transfer{
        target_provider_id = TargetProviderId
}}) ->
    % new replication
    ?run_if_is_self(TargetProviderId, fun() ->
        new_transfer(TransferDoc)
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with invalidation_status=scheduled is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_scheduled_invalidation(transfer:doc()) -> ok.
handle_scheduled_invalidation(TransferDoc = #document{
    value = #transfer{
        invalidation_status = scheduled,
        source_provider_id = SourceProviderId,
        invalidate_source_replica = true
}}) ->
    % this transfer is a migration or invalidation
    % replication part has already been finished or skipped
    % so invalidation procedure can be started
    ?run_if_is_self(SourceProviderId, fun() ->
        new_invalidation(TransferDoc)
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with status = active is modified.
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
%% Function called when transfer doc with status = active is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_active_transfer(transfer:doc()) -> ok.
handle_active_transfer(#document{
    value = #transfer{
        files_to_process = FilesToProcess,
        files_processed = FilesToProcess,
        failed_files = 0,
        target_provider_id = TargetProviderId,
        pid = ControllerPid
}}) ->
    ?run_if_is_self(TargetProviderId, fun() ->
        transfer_controller:mark_finished(transfer_utils:decode_pid(ControllerPid))
    end);
handle_active_transfer(#document{
    value = #transfer{
        files_to_process = FilesToProcess,
        files_processed = FilesToProcess,
        target_provider_id = TargetProviderId,
        pid = ControllerPid
}}) ->
    ?run_if_is_self(TargetProviderId, fun() ->
        DecodedPid = transfer_utils:decode_pid(ControllerPid),
        transfer_controller:mark_failed(DecodedPid, exceeded_number_of_failed_files)
    end);
handle_active_transfer(#document{
    value = #transfer{
        failed_files = FailedFiles,
        target_provider_id = TargetProviderId,
        pid = ControllerPid
}}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER of
        true ->
            ?run_if_is_self(TargetProviderId, fun() ->
                DecodedPid = transfer_utils:decode_pid(ControllerPid),
                transfer_controller:mark_failed(DecodedPid,
                    exceeded_number_of_failed_files)
            end);
        false ->
        ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc with invalidation_status = active is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_active_invalidation(transfer:doc()) -> ok.
handle_active_invalidation(#document{
    value = #transfer{
        files_to_process = FilesToProcess,
        files_processed = FilesToProcess,
        failed_files = 0,
        source_provider_id = SourceProviderId,
        pid = ControllerPid
}}) ->
    ?run_if_is_self(SourceProviderId, fun() ->
        invalidation_controller:mark_finished(transfer_utils:decode_pid(ControllerPid))
    end);
handle_active_invalidation(#document{
    value = #transfer{
        files_to_process = FilesToProcess,
        files_processed = FilesToProcess,
        source_provider_id = SourceProviderId,
        pid = ControllerPid
    }}) ->
    ?run_if_is_self(SourceProviderId, fun() ->
        DecodedPid = transfer_utils:decode_pid(ControllerPid),
        invalidation_controller:mark_failed(DecodedPid,
            exceeded_number_of_failed_files)
    end);
handle_active_invalidation(#document{
    value = #transfer{
        failed_files = FailedFiles,
        source_provider_id = SourceProviderId,
        pid = ControllerPid
    }}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER of
        true ->
            ?run_if_is_self(SourceProviderId, fun() ->
                DecodedPid = transfer_utils:decode_pid(ControllerPid),
                transfer_controller:mark_failed(DecodedPid,
                    exceeded_number_of_failed_files)
            end);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function called when transfer doc of finished invalidation is modified.
%% @end
%%--------------------------------------------------------------------
-spec handle_finished_invalidation(transfer:doc()) -> ok.
handle_finished_invalidation(#document{
    key = TransferId,
    value = #transfer{
        space_id = SpaceId,
        target_provider_id = TargetProviderId,
        invalidate_source_replica = true,
        schedule_time = ScheduleTime
    }}) ->
    ?run_if_is_self(TargetProviderId, fun() ->
        % deleting finished migration from active transfers tree
        % ensure that there is no duplicate in active transfers tree
        ok = transfer_links:delete_active_transfer_link(TransferId,
            SpaceId, ScheduleTime)
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new transfer based on existing doc synchronized from other provider
%% @end
%%--------------------------------------------------------------------
-spec new_transfer(transfer:doc()) -> ok.
new_transfer(#document{
    key = TransferId,
    value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        callback = Callback,
        invalidate_source_replica = InvalidateSourceReplica
    }
}) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    worker_pool:cast(?TRANSFER_CONTROLLERS_POOL, {
        start_transfer,
        session:root_session_id(),
        TransferId,
        FileGuid,
        Callback,
        InvalidateSourceReplica
    }).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new transfer based on existing doc synchronized from other provider
%% @end
%%--------------------------------------------------------------------
-spec new_invalidation(transfer:doc()) -> ok.
new_invalidation(#document{
    key = TransferId,
    value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        callback = Callback
    }
}) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    {ok, _Pid} = gen_server2:start(invalidation_controller,
        [session:root_session_id(), TransferId, FileGuid, Callback], []),
    ok.

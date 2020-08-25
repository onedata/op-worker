%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle replication status transition
%%% for transfers. This includes updating/marking status and moving from one
%%% link tree to other according to following state machine.
%%%
%%%
%%%                      REPLICATION STATE MACHINE:
%%%
%%% WAITING LINKS TREE ||     ONGOING LINKS TREE      ||    ENDED LINKS TREE
%%%                    ||                             ||
%%%                    ||                             ||
%%% +-----------+      ||                             ||       +-----------+
%%% | scheduled |------||------ cancel := true -------||------>| cancelled |<--+
%%% +-----------+      ||                             ||       +-----------+   |
%%%       |            ||                             ||                       |
%%%       |            ||                             ||                       |
%%%       |            ||                             ||                       |
%%%       v          FR > 0                           ||                       |
%%% +-----------+   or BR > 0   +------------+    FTP == FP     +-----------+  |
%%% | enqueued  |------||------>|   active   |--------||------->| completed |  |
%%% +-----------+      ||       +------------+        ||        +-----------+  |
%%%       |            ||              |              ||                       |
%%%       |            ||              |              ||                       |
%%%       |            ||       cancel := true        ||                       |
%%% cancel := true     ||     or failed_files > 0     ||                       |
%%%       |            ||              |              ||                       |
%%%       |            ||              |              ||                       |
%%%       |            ||              v              ||      FTP == FP        |
%%%       |            ||       +------------+        ||  and cancel := true   |
%%%       +------------||------>|  aborting  |--------||-----------------------+
%%%                    ||       +------------+        ||
%%%                    ||              |              ||
%%%                    ||              |              ||
%%%                    ||          FTP == FP          ||
%%%                    ||     and cancel := false     ||
%%%                    ||              |              ||
%%%                    ||              |              ||       +-----------+
%%%                    ||              +--------------||------>|  failed   |
%%%                    ||                             ||       +-----------+
%%%                    ||                             ||
%%%
%%% Legend:
%%%     FTP = files_to_process
%%%     FP  = files_processed
%%%     FR  = files_replicated
%%%     BR  = bytes_replicated
%%%
%%%
%%% If necessary `failed` status can be forced from any waiting or ongoing status.
%%% It is used when transfer was interrupted abruptly (e.g. shutdown and restart
%%% of provider).
%%% Also there is one more status not shown on above fsm, namely `skipped`,
%%% which is only used when replication never happened/should not happen
%%% (e.g. replica eviction).
%%%
%%% Caution !!!
%%% Above state machine works also for first part of migration, which is eviction
%%% preceded by replication, with exception that when replication ends as `completed`
%%% transfer link is not moved from ongoing to ended tree (there is still eviction
%%% to do, so transfer is overall ongoing).
%%% @end
%%%-------------------------------------------------------------------
-module(replication_status).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").

-export([
    handle_enqueued/1, handle_active/1,
    handle_aborting/1, handle_completed/1,
    handle_failed/2, handle_cancelled/1,
    handle_restart/3
]).

-type error() :: {error, term()}.
-type transfer() :: transfer:transfer().


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_enqueued(transfer:id()) -> {ok, transfer:doc()} | error().
handle_enqueued(TransferId) ->
    EncodedPid = transfer_utils:encode_pid(self()),
    transfer:update(TransferId, fun(Transfer) ->
        case Transfer#transfer.replication_status of
            ?SCHEDULED_STATUS ->
                {ok, Transfer#transfer{
                    replication_status = ?ENQUEUED_STATUS,
                    start_time = provider_logic:zone_time_seconds(),
                    files_to_process = 1,
                    pid = EncodedPid
                }};
            Status ->
                {error, Status}
        end
    end).


-spec handle_active(transfer:id()) -> {ok, transfer:doc()} | error().
handle_active(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_active/1,
        fun transfer_links:add_ongoing/1
    ).


-spec handle_aborting(transfer:id()) -> {ok, transfer:doc()} | error().
handle_aborting(TransferId) ->
    OnSuccessfulUpdate = fun(Doc) ->
        replica_synchronizer:cancel(TransferId),
        transfer_links:add_ongoing(Doc)
    end,

    transfer:update_and_run(
        TransferId,
        fun mark_aborting/1,
        OnSuccessfulUpdate
    ).


-spec handle_completed(transfer:id()) -> {ok, transfer:doc()} | error().
handle_completed(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_completed/1,
        fun transfer_links:move_to_ended_if_not_migration/1
    ).


-spec handle_failed(transfer:id(), boolean()) -> {ok, transfer:doc()} | error().
handle_failed(TransferId, Force) ->
    UpdateFun = case Force of
        true -> fun mark_failed_forced/1;
        false -> fun mark_failed/1
    end,

    transfer:update_and_run(
        TransferId,
        UpdateFun,
        fun transfer_links:move_from_ongoing_to_ended/1
    ).


-spec handle_cancelled(transfer:id()) -> {ok, transfer:doc()} | error().
handle_cancelled(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_cancelled/1,
        fun transfer_links:move_from_ongoing_to_ended/1
    ).


-spec handle_restart(transfer:id(), transfer:id(), boolean()) -> {ok, transfer:doc()} | error().
handle_restart(TransferId, NewTransferId, MarkTransferFailed) ->
    case MarkTransferFailed of
        true ->
            UpdateFun = fun(Transfer) ->
                case mark_failed_forced(Transfer) of
                    {ok, UpdatedTransfer} -> {ok, UpdatedTransfer#transfer{rerun_id = NewTransferId}};
                    Other -> Other
                end
            end,

            UpdateAns = transfer:update_and_run(
                TransferId,
                UpdateFun,
                fun transfer_links:move_from_ongoing_to_ended/1
            ),

            case UpdateAns of
                {ok, _} -> UpdateAns;
                _ -> add_rerun_id(TransferId, NewTransferId)
            end
            ;
        false ->
            add_rerun_id(TransferId, NewTransferId)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mark_active(transfer()) -> {ok, transfer()} | error().
mark_active(Transfer = #transfer{replication_status = ?ENQUEUED_STATUS}) ->
    {ok, Transfer#transfer{replication_status = ?ACTIVE_STATUS}};
mark_active(#transfer{replication_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_aborting(transfer()) -> {ok, transfer()} | error().
mark_aborting(Transfer) ->
    case transfer:is_replication_ongoing(Transfer) of
        true ->
            {ok, Transfer#transfer{
                replication_status = ?ABORTING_STATUS,
                eviction_status = case transfer:is_migration(Transfer) of
                    true -> ?ABORTING_STATUS;
                    false -> Transfer#transfer.eviction_status
                end
            }};
        false ->
            {error, already_ended}
    end.


%% @private
-spec mark_completed(transfer()) -> {ok, transfer()} | error().
mark_completed(Transfer = #transfer{replication_status = ?ACTIVE_STATUS}) ->
    {ok, Transfer#transfer{
        replication_status = ?COMPLETED_STATUS,
        finish_time = case transfer:is_migration(Transfer) of
            true -> Transfer#transfer.finish_time;
            false -> provider_logic:zone_time_seconds()
        end
    }};
mark_completed(#transfer{replication_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_failed(transfer()) -> {ok, transfer()} | error().
mark_failed(Transfer = #transfer{replication_status = ?ABORTING_STATUS}) ->
    mark_failed_forced(Transfer);
mark_failed(#transfer{replication_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_failed_forced(transfer()) -> {ok, transfer()} | error().
mark_failed_forced(Transfer) ->
    case transfer:is_replication_ended(Transfer) of
        true ->
            {error, already_ended};
        false ->
            IsMigration = transfer:is_migration(Transfer),
            {ok, Transfer#transfer{
                replication_status = failed,
                finish_time = provider_logic:zone_time_seconds(),
                eviction_status = case IsMigration of
                    true -> failed;
                    false -> Transfer#transfer.eviction_status
                end
            }}
    end.


%% @private
-spec mark_cancelled(transfer()) -> {ok, transfer()} | error().
mark_cancelled(Transfer = #transfer{replication_status = ?SCHEDULED_STATUS}) ->
    {ok, Transfer#transfer{
        replication_status = ?CANCELLED_STATUS,
        eviction_status = case transfer:is_migration(Transfer) of
            true -> ?CANCELLED_STATUS;
            false -> Transfer#transfer.eviction_status
        end
    }};
mark_cancelled(Transfer = #transfer{replication_status = ?ABORTING_STATUS}) ->
    {ok, Transfer#transfer{
        replication_status = ?CANCELLED_STATUS,
        finish_time = provider_logic:zone_time_seconds(),
        eviction_status = case transfer:is_migration(Transfer) of
            true -> ?CANCELLED_STATUS;
            false -> Transfer#transfer.eviction_status
        end
    }};
mark_cancelled(#transfer{replication_status = Status}) ->
    {error, Status}.

-spec add_rerun_id(transfer:id(), transfer:id()) -> {ok, transfer:doc()} | error().
add_rerun_id(TransferId, NewTransferId) ->
    transfer:update(TransferId, fun(OldTransfer) ->
        {ok, OldTransfer#transfer{rerun_id = NewTransferId}}
    end).
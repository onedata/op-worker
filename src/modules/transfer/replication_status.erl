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
%%%       v            ||                             ||                       |
%%% +-----------+      ||       +------------+        ||       +-----------+   |
%%% | enqueued  |------||------>|   active   |--------||------>| completed |   |
%%% +-----------+      ||       +------------+        ||       +-----------+   |
%%%       |            ||              |              ||                       |
%%%       |            ||              |              ||                       |
%%%       |            ||       cancel := true        ||                       |
%%% cancel := true     ||     or failed_files > 0     ||                       |
%%%       |            ||              |              ||                       |
%%%       |            ||              |              ||                       |
%%%       |            ||              v              ||                       |
%%%       |            ||       +------------+        ||                       |
%%%       +------------||------>|  aborting  |-------- cancel := true ---------+
%%%                    ||       +------------+        ||
%%%                    ||              |              ||
%%%                    ||              |              ||       +-----------+
%%%                    ||              +--------------||------>|  failed   |
%%%                    ||                             ||       +-----------+
%%%                    ||                             ||
%%%
%%%
%%% Since migration is just replica_eviction preceded by replication, if
%%% replication fails/is cancelled, then whole migration fails/is cancelled.
%%% Also failed status can be forced from any waiting or ongoing status.
%%% It is necessary when transfer were interrupted abruptly,
%%% like for example shutdown and restart of provider.
%%% There is one more status not shown on state machines, which is skipped.
%%% It is used to mark that given replication never happened/should not happen.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_status).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").

-export([
    handle_enqueued/1, handle_active/1,
    handle_aborting/1, handle_completed/1,
    handle_failed/2, handle_cancelled/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_enqueued(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
handle_enqueued(TransferId) ->
    EncodedPid = transfer_utils:encode_pid(self()),
    transfer:update(TransferId, fun(Transfer) ->
        case Transfer#transfer.replication_status of
            scheduled ->
                {ok, Transfer#transfer{
                    replication_status = enqueued,
                    start_time = provider_logic:zone_time_seconds(),
                    files_to_process = 1,
                    pid = EncodedPid
                }};
            Status ->
                {error, Status}
        end
    end).


-spec handle_active(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
handle_active(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_active/1,
        fun transfer_links:add_ongoing/1
    ).


-spec handle_aborting(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
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


-spec handle_completed(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
handle_completed(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_completed/1,
        fun transfer_links:move_to_ended_if_not_migration/1
    ).


-spec handle_failed(transfer:id(), boolean()) ->
    {ok, transfer:doc()} | {error, term()}.
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


-spec handle_cancelled(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
handle_cancelled(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_cancelled/1,
        fun transfer_links:move_from_ongoing_to_ended/1
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec mark_active(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_active(Transfer = #transfer{replication_status = enqueued}) ->
    {ok, Transfer#transfer{replication_status = active}};
mark_active(#transfer{replication_status = Status}) ->
    {error, Status}.


-spec mark_aborting(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_aborting(Transfer) ->
    case transfer:is_replication_ongoing(Transfer) of
        true ->
            {ok, Transfer#transfer{replication_status = aborting}};
        false ->
            {error, already_ended}
    end.


-spec mark_completed(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_completed(Transfer = #transfer{replication_status = active}) ->
    {ok, Transfer#transfer{
        replication_status = completed,
        finish_time = case transfer:is_migration(Transfer) of
            true -> Transfer#transfer.finish_time;
            false -> provider_logic:zone_time_seconds()
        end
    }};
mark_completed(#transfer{replication_status = Status}) ->
    {error, Status}.


-spec mark_failed(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_failed(Transfer = #transfer{replication_status = aborting}) ->
    mark_failed_forced(Transfer);
mark_failed(#transfer{replication_status = Status}) ->
    {error, Status}.


-spec mark_failed_forced(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
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


-spec mark_cancelled(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_cancelled(Transfer = #transfer{replication_status = scheduled}) ->
    {ok, Transfer#transfer{
        replication_status = cancelled,
        eviction_status = case transfer:is_migration(Transfer) of
            true -> cancelled;
            false -> Transfer#transfer.eviction_status
        end
    }};
mark_cancelled(Transfer = #transfer{replication_status = aborting}) ->
    {ok, Transfer#transfer{
        replication_status = cancelled,
        finish_time = provider_logic:zone_time_seconds(),
        eviction_status = case transfer:is_migration(Transfer) of
            true -> cancelled;
            false -> Transfer#transfer.eviction_status
        end
    }};
mark_cancelled(#transfer{replication_status = Status}) ->
    {error, Status}.

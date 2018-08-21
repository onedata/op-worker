%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle replica_eviction status transition
%%% for transfers. This includes updating/marking status and moving from one
%%% link tree to other according to following state machine.
%%%
%%%
%%%                       REPLICA_EVICTION STATE MACHINE:
%%%
%%% WAITING LINKS TREE ||     ONGOING LINKS TREE      ||    ENDED LINKS TREE
%%%                    ||                             ||
%%%                    ||                             ||
%%% +-----------+      ||                             ||       +-----------+
%%% | scheduled |------||------ cancel := true -------||------>| cancelled |<--+
%%% +-----------+      ||                             ||       +-----------+   |
%%%       |            ||                             ||             ^         |
%%%       |            ||                             ||             |         |
%%%       |   +--------||------ cancel := true -------||-------------+         |
%%%       |   |        ||                             ||                       |
%%%       v   |        ||                             ||                       |
%%% +-----------+      ||       +------------+        ||       +-----------+   |
%%% | enqueued  |------||------>|   active   |--------||------>| completed |   |
%%% +-----------+      ||       +------------+        ||       +-----------+   |
%%%                    ||              |              ||                       |
%%%                    ||              |              ||                       |
%%%                    ||       cancel := true        ||                       |
%%%                    ||     or failed_files > 0     ||                       |
%%%                    ||              |              ||                       |
%%%                    ||              |              ||                       |
%%%                    ||              v              ||                       |
%%%                    ||       +------------+        ||                       |
%%%                    ||       |  aborting  |-------- cancel := true ---------+
%%%                    ||       +------------+        ||
%%%                    ||              |              ||
%%%                    ||              |              ||       +-----------+
%%%                    ||              +--------------||------>|  failed   |
%%%                    ||                             ||       +-----------+
%%%                    ||                             ||
%%%
%%%
%%% Also failed status can be forced from any waiting or ongoing status.
%%% It is necessary when transfer were interrupted abruptly,
%%% like for example shutdown and restart of provider.
%%% There is one more status not shown on state machines, which is skipped.
%%% It is used to mark that given REPLICA_EVICTION never happened/should not happen.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_status).
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
    transfer:update(TransferId, fun mark_enqueued/1).


-spec handle_active(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
handle_active(TransferId) ->
    EncodedPid = transfer_utils:encode_pid(self()),
    UpdateFun = fun(Transfer) ->
        case Transfer#transfer.eviction_status of
            enqueued ->
                {ok, Transfer#transfer{
                    eviction_status = active,
                    files_to_process = Transfer#transfer.files_to_process + 1,
                    pid = EncodedPid
                }};
            Status ->
                {error, Status}
        end
    end,

    OnSuccessfulUpdate = fun(Doc = #document{value = Transfer}) ->
        case transfer:is_migration(Transfer) of
            true -> ok;
            false -> transfer_links:add_ongoing_transfer_link(Doc)
        end
    end,
    transfer:update_and_run(
        TransferId,
        UpdateFun,
        OnSuccessfulUpdate
    ).


-spec handle_aborting(transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
handle_aborting(TransferId) ->
    transfer:update(TransferId, fun mark_aborting/1).


-spec handle_completed(transfer:id()) ->
    {ok, transfer:doc()} | {error, term()}.
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
        fun transfer_links:move_to_ended_if_not_migration/1
    ).


-spec handle_cancelled(transfer:id()) ->
    {ok, transfer:doc()} | {error, term()}.
handle_cancelled(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_cancelled/1,
        fun transfer_links:move_to_ended_if_not_migration/1
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec mark_enqueued(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_enqueued(T = #transfer{eviction_status = scheduled}) ->
    {ok, T#transfer{
        eviction_status = enqueued,
        start_time = case transfer:is_migration(T) of
            true -> T#transfer.start_time;
            false -> provider_logic:zone_time_seconds()
        end
    }};
mark_enqueued(#transfer{eviction_status = Status}) ->
    {error, Status}.


-spec mark_aborting(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_aborting(T = #transfer{eviction_status = active}) ->
    {ok, T#transfer{eviction_status = aborting}};
mark_aborting(#transfer{eviction_status = Status}) ->
    {error, Status}.


-spec mark_completed(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_completed(T = #transfer{eviction_status = active}) ->
    {ok, T#transfer{
        eviction_status = completed,
        finish_time = provider_logic:zone_time_seconds()
    }};
mark_completed(#transfer{eviction_status = Status}) ->
    {error, Status}.


-spec mark_failed(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_failed(T = #transfer{eviction_status = aborting}) ->
    mark_failed_forced(T);
mark_failed(#transfer{eviction_status = Status}) ->
    {error, Status}.


-spec mark_failed_forced(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_failed_forced(Transfer) ->
    case transfer:is_eviction_ended(Transfer) of
        true ->
            {error, already_ended};
        false ->
            {ok, Transfer#transfer{
                eviction_status = failed,
                finish_time = provider_logic:zone_time_seconds()
            }}
    end.


-spec mark_cancelled(transfer:transfer()) ->
    {ok, transfer:transfer()} | {error, term()}.
mark_cancelled(Transfer) ->
    case Transfer#transfer.eviction_status of
        scheduled ->
            {ok, Transfer#transfer{
                eviction_status = cancelled
            }};
        Status when Status == enqueued orelse Status == aborting ->
            {ok, Transfer#transfer{
                eviction_status = cancelled,
                finish_time = provider_logic:zone_time_seconds()
            }};
        Status ->
            {error, Status}
    end.

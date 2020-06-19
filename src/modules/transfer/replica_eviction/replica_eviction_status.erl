%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle replica_eviction status
%%% transition for transfers. This includes updating/marking status and moving
%%% from one link tree to other according to following state machine.
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
%%% +-----------+      ||       +------------+    FTP == FP    +-----------+   |
%%% | enqueued  |------||------>|   active   |--------||------>| completed |   |
%%% +-----------+      ||       +------------+        ||       +-----------+   |
%%%                    ||              |              ||                       |
%%%                    ||              |              ||                       |
%%%                    ||       cancel := true        ||                       |
%%%                    ||     or failed_files > 0     ||                       |
%%%                    ||              |              ||                       |
%%%                    ||              |              ||                       |
%%%                    ||              v              ||       FTP == FP       |
%%%                    ||       +------------+        ||  and cancel := true   |
%%%                    ||       |  aborting  |---------------------------------+
%%%                    ||       +------------+        ||
%%%                    ||              |              ||
%%%                    ||          FTP == FP          ||
%%%                    ||     and cancel := false     ||
%%%                    ||              |              ||       +-----------+
%%%                    ||              +--------------||------>|  failed   |
%%%                    ||                             ||       +-----------+
%%%                    ||                             ||
%%%
%%% Legend:
%%%     FTP = files_to_process
%%%     FP  = files_processed
%%%
%%%
%%% If necessary `failed` status can be forced from any waiting or ongoing status.
%%% It is used when transfer was interrupted abruptly (e.g. shutdown and restart
%%% of provider).
%%% Also there is one more status not shown on above fsm, namely `skipped`,
%%% which is only used when replica eviction never happened/should not happen
%%% (e.g. replication).
%%%
%%% Caution !!!
%%% Above state machine works also for second part of migration, which is eviction
%%% preceded by replication, with exception that transfer is already in ongoing links
%%% tree when eviction starts (it is there after replication ends).
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_status).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/replica_deletion/replica_deletion.hrl").

-export([
    handle_enqueued/1, handle_active/1,
    handle_aborting/1, handle_completed/1,
    handle_failed/2, handle_cancelled/1
]).

-type error() :: {error, term()}.
-type transfer() :: transfer:transfer().


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_enqueued(transfer:id()) -> {ok, transfer:doc()} | error().
handle_enqueued(TransferId) ->
    transfer:update(TransferId, fun mark_enqueued/1).


-spec handle_active(transfer:id()) -> {ok, transfer:doc()} | error().
handle_active(TransferId) ->
    EncodedPid = transfer_utils:encode_pid(self()),
    UpdateFun = fun(Transfer) ->
        case Transfer#transfer.eviction_status of
            ?ENQUEUED_STATUS ->
                {ok, Transfer#transfer{
                    eviction_status = ?ACTIVE_STATUS,
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
            false -> transfer_links:add_ongoing(Doc)
        end
    end,
    transfer:update_and_run(
        TransferId,
        UpdateFun,
        OnSuccessfulUpdate
    ).


-spec handle_aborting(transfer:id()) -> {ok, transfer:doc()} | error().
handle_aborting(TransferId) ->
    OnSuccessfulUpdate = fun(#document{value = #transfer{space_id = SpaceId}}) ->
        replica_deletion_master:cancel_request(SpaceId, TransferId, ?EVICTION_JOB)
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
        fun transfer_links:move_to_ended_if_not_migration/1
    ).


-spec handle_cancelled(transfer:id()) -> {ok, transfer:doc()} | error().
handle_cancelled(TransferId) ->
    transfer:update_and_run(
        TransferId,
        fun mark_cancelled/1,
        fun transfer_links:move_to_ended_if_not_migration/1
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mark_enqueued(transfer()) -> {ok, transfer()} | error().
mark_enqueued(T = #transfer{eviction_status = ?SCHEDULED_STATUS}) ->
    {ok, T#transfer{
        eviction_status = ?ENQUEUED_STATUS,
        start_time = case transfer:is_migration(T) of
            true -> T#transfer.start_time;
            false -> provider_logic:zone_time_seconds()
        end
    }};
mark_enqueued(#transfer{eviction_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_aborting(transfer()) -> {ok, transfer()} | error().
mark_aborting(T = #transfer{eviction_status = ?ACTIVE_STATUS}) ->
    {ok, T#transfer{eviction_status = ?ABORTING_STATUS}};
mark_aborting(#transfer{eviction_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_completed(transfer()) -> {ok, transfer()} | error().
mark_completed(T = #transfer{eviction_status = ?ACTIVE_STATUS}) ->
    {ok, T#transfer{
        eviction_status = ?COMPLETED_STATUS,
        finish_time = provider_logic:zone_time_seconds()
    }};
mark_completed(#transfer{eviction_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_failed(transfer:transfer()) -> {ok, transfer()} | error().
mark_failed(T = #transfer{eviction_status = ?ABORTING_STATUS}) ->
    mark_failed_forced(T);
mark_failed(#transfer{eviction_status = Status}) ->
    {error, Status}.


%% @private
-spec mark_failed_forced(transfer()) -> {ok, transfer()} | error().
mark_failed_forced(Transfer) ->
    case transfer:is_eviction_ended(Transfer) of
        true ->
            {error, already_ended};
        false ->
            {ok, Transfer#transfer{
                eviction_status = ?FAILED_STATUS,
                finish_time = provider_logic:zone_time_seconds()
            }}
    end.


%% @private
-spec mark_cancelled(transfer()) -> {ok, transfer()} | error().
mark_cancelled(Transfer) ->
    case Transfer#transfer.eviction_status of
        ?SCHEDULED_STATUS ->
            {ok, Transfer#transfer{
                eviction_status = ?CANCELLED_STATUS
            }};
        Status when Status == ?ENQUEUED_STATUS orelse Status == ?ABORTING_STATUS ->
            {ok, Transfer#transfer{
                eviction_status = ?CANCELLED_STATUS,
                finish_time = provider_logic:zone_time_seconds()
            }};
        Status ->
            {error, Status}
    end.

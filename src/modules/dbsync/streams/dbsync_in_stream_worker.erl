%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for sequencing and handling remote changes
%%% from a given space and provider.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_in_stream_worker).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/dbsync/dbsync.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    space_id :: od_space:id(),
    provider_id :: od_provider:id(),
    batch_manager_state :: dbsync_in_stream_batch_manager:state(),
    changes_request_ref :: undefined | reference(),
    apply_batch :: undefined | couchbase_changes:until(),
    % Reference of timer that triggers checking space sync progress in onezone
    zone_check_ref :: reference()
}).

-type state() :: #state{}.

-define(ZONE_CHECK_BASE_INTERVAL, application:get_env(?APP_NAME,
    dbsync_zone_check_base_interval, timer:minutes(5))).
-define(ZONE_CHECK_BACKOFF_RATE, application:get_env(?APP_NAME,
    dbsync_zone_check_backoff_rate, 1.5)).
-define(ZONE_CHECK_MAX_INTERVAL, application:get_env(?APP_NAME,
    dbsync_zone_checkt_max_interval, timer:minutes(60))).

% Internal messages
-define(CHECK_BATCH_STASH, check_batch_stash).
-define(CHECK_BATCH_STASH_AND_REQUEST, check_batch_stash_and_request).
-define(CHECK_SEQ_IN_ZONE, check_seq_in_zone).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts stream for incoming remote changes from a given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_space:id(), od_provider:id()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(SpaceId, ProviderId) ->
    gen_server2:start_link(?MODULE, [SpaceId, ProviderId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes DBSync incoming stream worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SpaceId, ProviderId]) ->
    Seq = dbsync_state:get_seq(SpaceId, ProviderId),
    {ok, #state{
        space_id = SpaceId,
        provider_id = ProviderId,
        batch_manager_state = dbsync_in_stream_batch_manager:init(ProviderId, Seq),
        zone_check_ref = schedule_seq_check_in_zone()
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({changes_batch, Batch}, State = #state{
    space_id = SpaceId,
    provider_id = ProviderId
}) ->
    LocalProviderId = oneprovider:get_id(),
    Supported = dbsync_utils:is_supported(SpaceId, [
        LocalProviderId,
        ProviderId
    ]),
    case Supported of
        true ->
            {noreply, reset_seq_check_in_zone_timer(handle_changes_batch(Batch, State))};
        {false, [LocalProviderId | _]} ->
            % TODO VFS-6135 - integrate with space unsupport
            {stop, normal, State};
        {false, [ProviderId]} ->
            {noreply, reset_seq_check_in_zone_timer(check_batch_in_zone_and_maybe_handle(Batch, State))}
    end;
handle_cast({?CHECK_BATCH_STASH, DistributorId}, State) ->
    State2 = check_stash_and_apply_or_request_batch(DistributorId, ?SCHEDULE_REQUEST_IF_MISSING, State),
    {noreply, State2};
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(?CHECK_BATCH_STASH_AND_REQUEST, #state{provider_id = ProviderId} = State) ->
    State2 = check_stash_and_apply_or_request_batch(ProviderId, ?REQUEST_IF_MISSING,
        State#state{changes_request_ref = undefined}),
    {noreply, State2};
handle_info({?BATCH_APPLICATION_RESULT, Batch, Ans}, #state{} = State) ->
    State2 = process_batch_application_result(Batch, Ans, State),
    {noreply, State2};
handle_info({?CHECK_SEQ_IN_ZONE, Delay}, #state{} = State) ->
    check_seq_in_zone(Delay, State);
handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, #state{} = State) ->
    ?log_terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions: main flow
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles changes batch. If changes concern awaited sequence numbers range,
%% they are immediately applied. Otherwise, changes are stashed and request
%% for missing changes batch is scheduled.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_batch(dbsync_worker:internal_changes_batch(), state()) -> state().
handle_changes_batch(
    #internal_changes_batch{
        since = RemoteSeq,
        until = RemoteSeq,
        docs = [],
        custom_request_extension = <<?CUSTOM_CHANGES_STREAM_INIT_MSG_PREFIX, RequestedSinceBin/binary>>,
        distributor_id = Distributor
    },
    State = #state{
        batch_manager_state = BatchManagerState,
        provider_id = ProviderId
    }
) ->
    Seq = dbsync_in_stream_batch_manager:get_sequence(BatchManagerState, ProviderId),
    % Init custom stream handling if it begins on current sequence
    case binary_to_integer(RequestedSinceBin) of
        Seq ->
            {_OldSeq, UpdatedBatchManagerState} =
                dbsync_in_stream_batch_manager:set_sequence(BatchManagerState, Distributor, RemoteSeq),
            State#state{batch_manager_state = UpdatedBatchManagerState};
        _ ->
            State
    end;
handle_changes_batch(
    Batch = #internal_changes_batch{
        distributor_id = Distributor
    },
    State = #state{
        batch_manager_state = BatchManagerState,
        apply_batch = Apply
    }
) ->
    {Action, BatchToHandle, UpdatedBatchManagerState} =
        dbsync_in_stream_batch_manager:handle_incoming_batch(BatchManagerState, Distributor, Batch, Apply),
    State2 = State#state{batch_manager_state = UpdatedBatchManagerState},
    case Action of
        ?APPLY -> apply_changes_batch(BatchToHandle, State2);
        ?CHANGES_STASHED -> schedule_changes_request(State2);
        _ -> State2
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies changes batch and changes that have been stashed, as long as they
%% constitute a continuous range.
%% @end
%%--------------------------------------------------------------------
-spec apply_changes_batch(dbsync_worker:internal_changes_batch(), state()) -> state().
apply_changes_batch(Batch = #internal_changes_batch{until = Until}, State) ->
    State2 = cancel_changes_request(State),
    dbsync_changes:apply_batch(Batch),

    case op_worker:get_env(dbsync_in_stream_worker_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    State2#state{apply_batch = Until}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence when changes applying ends.
%% @end
%%--------------------------------------------------------------------
-spec process_batch_application_result(dbsync_worker:internal_changes_batch(),
    dbsync_changes:dbsync_application_result() | timeout, state()) -> state().
process_batch_application_result(_, timeout, State) ->
    schedule_changes_request(State#state{apply_batch = undefined});
process_batch_application_result(#internal_changes_batch{
    until = Until,
    timestamp = Timestamp,
    distributor_id = DistributorId
}, #dbsync_application_result{
    min_erroneous_seq = undefined
} = Ans, #state{
    provider_id = DistributorId,
    space_id = SpaceId
} = State) ->
    % Distributor id is equal to stream's provider id - process pending sequences.
    dbsync_pending_seqs:process_dbsync_in_stream_seqs(SpaceId, DistributorId, Ans),
    gen_server2:cast(self(), {?CHECK_BATCH_STASH, DistributorId}),
    update_seq(Until, Timestamp, DistributorId, State#state{apply_batch = undefined});
process_batch_application_result(#internal_changes_batch{
    until = Until,
    timestamp = Timestamp,
    distributor_id = DistributorId,
    custom_request_extension = CustomRequestExtension
}, #dbsync_application_result{
    min_erroneous_seq = undefined
}, #state{
    provider_id = ProviderId
} = State) ->
    % Distributor id is not equal to stream's provider id - pending sequences cannot be processed,
    % use data provider in message extension instead.
    gen_server2:cast(self(), {?CHECK_BATCH_STASH, DistributorId}),
    State2 = update_seq(Until, Timestamp, DistributorId, State#state{apply_batch = undefined}),
    DecodedProviderSeqs = dbsync_processed_seqs_history:decode(CustomRequestExtension),
    case maps:get(ProviderId, DecodedProviderSeqs, undefined) of
        undefined -> State2;
        DecodedSeq -> update_seq(DecodedSeq, undefined, ProviderId, State2)
    end;
process_batch_application_result(#internal_changes_batch{
    distributor_id = DistributorId
}, #dbsync_application_result{
    min_erroneous_seq = MinErroneousSeq
}, State) ->
    State2 = update_seq(MinErroneousSeq, undefined, DistributorId, State#state{apply_batch = undefined}),
    % TODO VFS-7206 - schedule faster check of sequences in zone when needed
    % (lost changes were sent by distributor that is not provider for which this stream works)
    schedule_changes_request(State2).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence number of the beginning of expected changes range.
%% @end
%%--------------------------------------------------------------------
-spec update_seq(couchbase_changes:seq(), dbsync_changes:timestamp() | undefined,
    od_provider:id(), state()) -> state().
update_seq(Seq, Timestamp, DistributorId, State = #state{space_id = SpaceId, provider_id = DistributorId,
    batch_manager_state = BatchManagerState}) ->
    {OldSeq, UpdatedBatchManagerState} =
        dbsync_in_stream_batch_manager:set_sequence(BatchManagerState, DistributorId, Seq),
    case Seq of
        OldSeq -> ok;
        _ -> dbsync_state:set_sync_progress(SpaceId, DistributorId, Seq, Timestamp)
    end,
    State#state{batch_manager_state = UpdatedBatchManagerState};
update_seq(Seq, _Timestamp, DistributorId, State = #state{batch_manager_state = BatchManagerState}) ->
    {_OldSeq, UpdatedBatchManagerState} =
        dbsync_in_stream_batch_manager:set_sequence(BatchManagerState, DistributorId, Seq),
    State#state{batch_manager_state = UpdatedBatchManagerState}.


-spec check_stash_and_apply_or_request_batch(od_provider:id(),
    ?REQUEST_IF_MISSING | ?SCHEDULE_REQUEST_IF_MISSING, state()) -> state().
check_stash_and_apply_or_request_batch(ProviderId, MissingChangesHandlingMode,
    State = #state{
        batch_manager_state = BatchManagerState,
        space_id = SpaceId}
) ->
    {Ans, UpdatedBatchManagerState} = dbsync_in_stream_batch_manager:take_batch_or_request_range(BatchManagerState, ProviderId),
    State2 = State#state{batch_manager_state = UpdatedBatchManagerState},
    case {Ans, MissingChangesHandlingMode} of
        {#internal_changes_batch{} = Batch, _} ->
            apply_changes_batch(Batch, State2);
        {?EMPTY_STASH, _} ->
            State2;
        {{?REQUEST_CHANGES, Since, Until}, ?REQUEST_IF_MISSING} ->
            dbsync_communicator:request_changes(ProviderId, SpaceId, Since, Until),
            schedule_changes_request(State2);
        {{?REQUEST_CHANGES, _Since, _Until}, ?SCHEDULE_REQUEST_IF_MISSING} ->
            schedule_changes_request(State2)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules changes request, if there is not other request scheduled already.
%% @end
%%--------------------------------------------------------------------
-spec schedule_changes_request(state()) -> state().
schedule_changes_request(State = #state{
    changes_request_ref = undefined
}) ->
    Delay = op_worker:get_env(dbsync_changes_batch_await_period, timer:seconds(15)),
    State#state{changes_request_ref = erlang:send_after(
        Delay, self(), ?CHECK_BATCH_STASH_AND_REQUEST
    )};
schedule_changes_request(State = #state{
    changes_request_ref = Ref
}) ->
    case erlang:read_timer(Ref) of
        false ->
            schedule_changes_request(State#state{
                changes_request_ref = undefined
            });
        _ ->
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cancels changes request if awaited.
%% @end
%%--------------------------------------------------------------------
-spec cancel_changes_request(state()) -> state().
cancel_changes_request(State = #state{changes_request_ref = undefined}) ->
    State;
cancel_changes_request(State = #state{changes_request_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    State#state{changes_request_ref = undefined}.

%%%===================================================================
%%% Internal functions: requesting changes of unavailable providers
%%%===================================================================

-spec schedule_seq_check_in_zone() -> reference().
schedule_seq_check_in_zone() ->
    schedule_seq_check_in_zone(?ZONE_CHECK_BASE_INTERVAL).

-spec schedule_seq_check_in_zone(non_neg_integer()) -> reference().
schedule_seq_check_in_zone(Delay) ->
    erlang:send_after(Delay, self(), {?CHECK_SEQ_IN_ZONE, Delay}).

-spec reset_seq_check_in_zone_timer(state()) -> state().
reset_seq_check_in_zone_timer(State = #state{zone_check_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    State#state{zone_check_ref = schedule_seq_check_in_zone()}.

-spec increase_delay_and_schedule_seq_check_in_zone(non_neg_integer(), state()) -> state().
increase_delay_and_schedule_seq_check_in_zone(Delay, State = #state{zone_check_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    NewDelay = min(?ZONE_CHECK_MAX_INTERVAL, round(Delay * ?ZONE_CHECK_BACKOFF_RATE)),
    State#state{zone_check_ref = schedule_seq_check_in_zone(NewDelay)}.

-spec check_batch_in_zone_and_maybe_handle(dbsync_worker:internal_changes_batch(), state()) -> state().
check_batch_in_zone_and_maybe_handle(
    Batch = #internal_changes_batch{
        since = Since,
        until = Until
    },
    State = #state{
        space_id = SpaceId,
        provider_id = ProviderId
    }
) ->
    case space_logic:get_latest_emitted_seq(SpaceId, ProviderId) of
        {ok, Seq} when Seq >= Until ->
            handle_changes_batch(Batch, State);
        {ok, Seq} ->
            ?warning("Batch [~p, ~p] from provider ~p not supporting space ~p "
            "when last seq according to onezone is ~p", [Since, Until, ProviderId, SpaceId, Seq]),
            State;
        Error ->
            ?warning("Error ~p checking last emmited sequence for provider ~p in space ~p",
                [Error, ProviderId, SpaceId]),
            State
    end.

-spec check_seq_in_zone(non_neg_integer(), state()) -> {noreply, state()} | {stop, normal, state()}.
check_seq_in_zone(Delay, State = #state{
    space_id = SpaceId,
    provider_id = ProviderId,
    batch_manager_state = BatchManagerState
}) ->
    % TODO VFS-7206 - destroy stashes of other providers than provider connected with the stream
    % when they are not needed anymore
    Seq = dbsync_in_stream_batch_manager:get_sequence(BatchManagerState, ProviderId),
    case space_logic:get_latest_emitted_seq(SpaceId, ProviderId) of
        {ok, ZoneSeq} when ZoneSeq > Seq ->
            request_changes_from_other_provider(Seq, ZoneSeq, State),
            {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)};
        {ok, _} ->
            case dbsync_utils:should_terminate_stream(SpaceId, ProviderId) of
                true ->
                    {stop, normal, State};
                false ->
                    {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)};
                RetireCheckError ->
                    ?warning("Error ~p checking support for provider ~p in space ~p",
                        [RetireCheckError, ProviderId, SpaceId]),
                    {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)}
            end;
        SeqGetError ->
            ?warning("Error ~p checking last emmited sequence for provider ~p in space ~p",
                [SeqGetError, ProviderId, SpaceId]),
            {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)}
    end.

-spec request_changes_from_other_provider(couchbase_changes:seq(), couchbase_changes:seq(), state()) ->
    ok | {error, Reason :: term()}.
request_changes_from_other_provider(Since, Until, #state{
    space_id = SpaceId,
    provider_id = ProviderId
}) ->
    % TODO VFS-7036 - limit request range, handle message loss
    case dbsync_utils:get_providers(SpaceId) -- [ProviderId, oneprovider:get_id()] of
        [TargetProvider | _] -> dbsync_communicator:request_changes(TargetProvider, ProviderId, SpaceId, Since, Until);
        _ -> ok
    end.
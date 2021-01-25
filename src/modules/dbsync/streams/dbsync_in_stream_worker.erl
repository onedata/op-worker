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
    seq :: couchbase_changes:seq(),
    changes_stash :: dbsync_in_stream_stash:stash(),
    changes_request_ref :: undefined | reference(),
    apply_batch :: undefined | couchbase_changes:until(),
    zone_check_ref :: reference()
}).

-type state() :: #state{}.

-define(ZONE_CHECK_BASE_INTERVAL, application:get_env(?APP_NAME,
    dbsync_zone_check_base_interval, timer:minutes(5))).
-define(ZONE_CHECK_BACKOFF_RATE, application:get_env(?APP_NAME,
    dbsync_zone_check_backoff_rate, 1.5)).
-define(ZONE_CHECK_MAX_INTERVAL, application:get_env(?APP_NAME,
    dbsync_zone_checkt_max_interval, timer:minutes(60))).

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
    {ok, #state{
        space_id = SpaceId,
        provider_id = ProviderId,
        seq = dbsync_state:get_seq(SpaceId, ProviderId),
        changes_stash = dbsync_in_stream_stash:init(),
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
handle_cast(check_batch_stash, State = #state{
    changes_stash = Stash,
    provider_id = ProviderId,
    seq = Seq
}) ->
    {Changes, UpdatedStash} = dbsync_in_stream_stash:get_changes_to_apply(Stash, ProviderId, Seq),
    State2 = State#state{changes_stash = UpdatedStash},
    case Changes of
        {Docs, Until, Timestamp} -> {noreply, apply_changes_batch(Seq, Until, Timestamp, Docs, undefined, State2)};
        missing_changes -> {noreply, schedule_changes_request(State2)};
        empty_stash -> {noreply, State2}
    end;
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
handle_info(request_changes, State = #state{
    space_id = SpaceId,
    provider_id = ProviderId,
    seq = Seq,
    changes_stash = Stash
}) ->
    {UpperRange, UpdatedStash} = dbsync_in_stream_stash:get_request_upper_range(Stash, ProviderId, Seq),
    State2 = State#state{changes_stash = UpdatedStash},
    case UpperRange of
        empty_stash ->
            {noreply, State2#state{changes_request_ref = undefined}};
        Until when Until == Seq ->
            gen_server2:cast(self(), check_batch_stash),
            {noreply, State2#state{changes_request_ref = undefined}};
        Until ->
            dbsync_communicator:request_changes(ProviderId, SpaceId, Seq, Until),
            {noreply, schedule_changes_request(State2#state{changes_request_ref = undefined})}
    end;
handle_info({batch_application_result, {Since, Until}, Timestamp, CustomRequestExtension, Ans}, #state{} = State) ->
    State2 = process_batch_application_result(Since, Until, Timestamp, CustomRequestExtension, Ans, State),
    {noreply, State2};
handle_info({check_seq_in_zone, Delay}, #state{} = State) ->
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
        since = Since,
        until = Until,
        timestamp = Timestamp,
        docs = Docs,
        sender_id = Sender
    },
    State = #state{
        provider_id = Sender,
        changes_stash = Stash,
        seq = Seq,
        apply_batch = Apply
    }
) ->
    % Provider connected with this stream is sender of changes
    case {Since, Apply} of
        {Seq, undefined} ->
            apply_changes_batch(Since, Until, Timestamp, Docs, undefined, State);
        {Higher, undefined} when Higher > Seq ->
            UpdatedStash = dbsync_in_stream_stash:stash_changes_batch(Stash, Sender, Seq, Since, Until, Timestamp, Docs),
            schedule_changes_request(State#state{changes_stash = UpdatedStash});
        {Higher, _} when Higher >= Apply ->
            UpdatedStash = dbsync_in_stream_stash:stash_changes_batch(Stash, Sender, Seq, Since, Until, Timestamp, Docs),
            schedule_changes_request(State#state{changes_stash = UpdatedStash});
        _ ->
            State
    end;
handle_changes_batch(
    #internal_changes_batch{
        since = Since,
        until = Until,
        timestamp = Timestamp,
        docs = Docs,
        custom_request_extension = CustomRequestExtension
    },
    State = #state{
        provider_id = ProviderId
    }
) ->
    dbsync_changes:apply_batch(Docs, {Since, Until}, Timestamp, ProviderId, CustomRequestExtension),
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies changes batch and changes that have been stashed, as long as they
%% constitute a continuous range.
%% @end
%%--------------------------------------------------------------------
-spec apply_changes_batch(couchbase_changes:since(), couchbase_changes:until(), dbsync_changes:timestamp(),
    dbsync_worker:batch_docs(), dbsync_worker:custom_request_extension() | undefined, state()) -> state().
apply_changes_batch(Since, Until, Timestamp, Docs, CustomRequestExtension, #state{provider_id = ProviderId} = State) ->
    State2 = cancel_changes_request(State),
    {Docs2, Timestamp2, Until2, State3} = prepare_batch(Docs, Timestamp, Until, State2),
    dbsync_changes:apply_batch(Docs2, {Since, Until2}, Timestamp2, ProviderId, CustomRequestExtension),

    case op_worker:get_env(dbsync_in_stream_worker_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    State3#state{apply_batch = Until2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence when changes applying ends.
%% @end
%%--------------------------------------------------------------------
-spec process_batch_application_result(couchbase_changes:since(), couchbase_changes:until(),
    dbsync_changes:timestamp(), dbsync_worker:custom_request_extension() | undefined,
    dbsync_changes:dbsync_application_result() | timeout, state()) -> state().
process_batch_application_result(_Since, Until, Timestamp, CustomRequestExtension, Ans,
    #state{provider_id = ProviderId, space_id = SpaceId} = State) ->
    State2 = State#state{apply_batch = undefined},
    case {Ans, CustomRequestExtension} of
        {#dbsync_application_result{min_erroneous_seq = undefined}, undefined} ->
            dbsync_pending_seqs:process_dbsync_in_stream_seqs(SpaceId, ProviderId, Ans),
            gen_server2:cast(self(), check_batch_stash),
            update_seq(Until, Timestamp, State2);
        {#dbsync_application_result{min_erroneous_seq = undefined}, ProviderSeqs} ->
            gen_server2:cast(self(), check_batch_stash),
            DecodedProviderSeqs = dbsync_processed_seqs_history:decode(ProviderSeqs),
            case maps:get(ProviderId, DecodedProviderSeqs, undefined) of
                undefined -> State2;
                DecodedSeq -> update_seq(DecodedSeq, undefined, State2)
            end;
        {#dbsync_application_result{min_erroneous_seq = Seq}, undefined} ->
            dbsync_pending_seqs:process_dbsync_in_stream_seqs(SpaceId, ProviderId, Ans),
            State3 = update_seq(Seq, undefined, State2),
            schedule_changes_request(State3);
        _ ->
            schedule_changes_request(State2)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares batch to be applied. If there are no missing changes between batches
%% it merges them.
%% @end
%%--------------------------------------------------------------------
-spec prepare_batch(dbsync_worker:batch_docs(), dbsync_changes:timestamp(), couchbase_changes:until(), state()) ->
    {dbsync_worker:batch_docs(), dbsync_changes:timestamp(), couchbase_changes:until(), state()}.
prepare_batch(Docs, Timestamp, Until, State = #state{
    changes_stash = Stash,
    provider_id = ProviderId
}) ->
    {{{Docs2, Until2, Timestamp2}, ExtendedInfo}, UpdatedStash} =
        dbsync_in_stream_stash:extend_batch_with_stashed_changes(Stash, ProviderId, Docs, Timestamp, Until),
    case ExtendedInfo of
        missing_changes -> {Docs2, Timestamp2, Until2, schedule_changes_request(State#state{changes_stash = UpdatedStash})};
        _ -> {Docs2, Timestamp2, Until2, State#state{changes_stash = UpdatedStash}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence number of the beginning of expected changes range.
%% @end
%%--------------------------------------------------------------------
-spec update_seq(couchbase_changes:seq(), dbsync_changes:timestamp() | undefined, state()) -> state().
update_seq(Seq, _Timestamp, State = #state{seq = Seq}) ->
    State;
update_seq(Seq, Timestamp, State = #state{space_id = SpaceId, provider_id = ProviderId}) ->
    dbsync_state:set_sync_progress(SpaceId, ProviderId, Seq, Timestamp),
    State#state{seq = Seq}.

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
        Delay, self(), request_changes
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
    erlang:send_after(Delay, self(), {check_seq_in_zone, Delay}).

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
    seq = Seq,
    space_id = SpaceId,
    provider_id = ProviderId
}) ->
    case space_logic:get_latest_emitted_seq(SpaceId, ProviderId) of
        {ok, ZoneSeq} when ZoneSeq > Seq ->
            request_changes_from_other_provider(ZoneSeq, State),
            {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)};
        {ok, _} ->
            case space_logic:get_support_stage_registry(SpaceId) of
                {ok, SupportStageRegistry} ->
                    case support_stage:is_provider_retired(SupportStageRegistry, ProviderId) of
                        false -> {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)};
                        _ -> {stop, normal, State}
                    end;
                SupportGetError ->
                    ?warning("Error ~p getting support registry for provider ~p in space ~p",
                        [SupportGetError, ProviderId, SpaceId]),
                    {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)}
            end;
        SeqGetError ->
            ?warning("Error ~p checking last emmited sequence for provider ~p in space ~p",
                [SeqGetError, ProviderId, SpaceId]),
            {noreply, increase_delay_and_schedule_seq_check_in_zone(Delay, State)}
    end.

-spec request_changes_from_other_provider(couchbase_changes:seq(), state()) -> ok | {error, Reason :: term()}.
request_changes_from_other_provider(ZoneSeq, #state{
    seq = Seq,
    space_id = SpaceId,
    provider_id = ProviderId
}) ->
    % TODO VFS-7036 - limit request range, handle message loss
    case dbsync_utils:get_providers(SpaceId) -- [ProviderId, oneprovider:get_id()] of
        [TargetProvider | _] -> dbsync_communicator:request_changes(TargetProvider, ProviderId, SpaceId, Seq, ZoneSeq);
        _ -> ok
    end.
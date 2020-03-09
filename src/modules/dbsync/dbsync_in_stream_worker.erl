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
    changes_stash :: ets:tid(),
    changes_request_ref :: undefined | reference(),
    apply_batch :: undefined | couchbase_changes:until(),
    first_batch_processed = false :: boolean(),
    lower_changes_count = 0 :: non_neg_integer(),
    first_lower_seq = 1 :: non_neg_integer()
}).

-type state() :: #state{}.

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
    {Seq, _} = dbsync_state:get_seq(SpaceId, ProviderId),
    {ok, #state{
        space_id = SpaceId,
        provider_id = ProviderId,
        seq = Seq,
        changes_stash = ets:new(changes_stash, [ordered_set, private])
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
handle_cast({changes_batch, Since, Until, Timestamp, Docs}, State = #state{
    space_id = SpaceId,
    provider_id = ProviderId
}) ->
    Supported = dbsync_utils:is_supported(SpaceId, [
        oneprovider:get_id(),
        ProviderId
    ]),
    case Supported of
        true -> {noreply, handle_changes_batch(Since, Until, Timestamp, Docs, State)};
        false -> {stop, normal, State}
    end;
handle_cast(check_batch_stash, State = #state{
    changes_stash = Stash,
    seq = Seq
}) ->
    case ets:first(Stash) of
        '$end_of_table' ->
            {noreply, State};
        {Seq, Until} ->
            Key = {Seq, Until},
            {Timestamp, Docs} = ets:lookup_element(Stash, Key, 2),
            ets:delete(Stash, Key),
            {noreply, apply_changes_batch(Seq, Until, Timestamp, Docs, State)};
        _ ->
            {noreply, schedule_changes_request(State)}
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
    case ets:first(Stash) of
        '$end_of_table' ->
            {noreply, State#state{changes_request_ref = undefined}};
        {Since, _} when Since == Seq ->
            gen_server2:cast(self(), check_batch_stash),
            {noreply, State#state{changes_request_ref = undefined}};
        {Since, _} = Key when Since < Seq ->
            ets:delete(Stash, Key),
            gen_server2:cast(self(), check_batch_stash),
            {noreply, State#state{changes_request_ref = undefined}};
        {Until, _} ->
            MaxSize = application:get_env(?APP_NAME,
                dbsync_changes_max_request_size, 20000),
            Until2 = min(Until, Seq + MaxSize),
            dbsync_communicator:request_changes(
                ProviderId, SpaceId, Seq, Until2
            ),
            {noreply, schedule_changes_request(State#state{
                changes_request_ref = undefined
            })}
    end;
handle_info({batch_applied, {Since, Until}, Timestamp, Ans}, #state{} = State) ->
    State2 = change_applied(Since, Until, Timestamp, Ans, State),
    {noreply, State2};
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
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles changes batch. If changes concern awaited sequence numbers range,
%% they are immediately applied. Otherwise, changes are stashed and request
%% for missing changes batch is scheduled.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_batch(couchbase_changes:since(), couchbase_changes:until(),
    couchbase_changes:timestamp(), [datastore:doc()], state()) -> state().
handle_changes_batch(Since, Until, Timestamp, Docs,
    State0 = #state{seq = Seq, apply_batch = Apply, first_batch_processed = FBP,
        lower_changes_count = LCC, first_lower_seq = FLS, space_id = SpaceID}) ->
    State = State0#state{first_batch_processed = true, lower_changes_count = 0},
    case {Since, Apply} of
        {Seq, undefined} ->
            apply_changes_batch(Since, Until, Timestamp, Docs, State);
        {Higher, undefined} when Higher > Seq ->
            State2 = stash_changes_batch(Since, Until, Timestamp, Docs, State),
            schedule_changes_request(State2);
        {Higher, _} when Higher >= Apply ->
            State2 = stash_changes_batch(Since, Until, Timestamp, Docs, State),
            schedule_changes_request(State2);
        {Lower, _} when Lower < Seq ->
            case FBP of
                true ->
                    MaxLowerChanges = application:get_env(?APP_NAME,
                        lower_changes_before_reset, 10),
                    case {LCC < MaxLowerChanges, LCC, MaxLowerChanges} of
                        {true, 0, _} ->
                            State#state{lower_changes_count = 1,
                                first_lower_seq = Lower};
                        {true, _, _} ->
                            State#state{lower_changes_count = LCC + 1};
                        {_, _, 0} ->
                            ?info("Reset changes seq for space ~p,"
                            " old ~p, new ~p", [SpaceID, Seq, Lower]),
                            State#state{seq = Lower};
                        _ ->
                            ?info("Reset changes seq for space ~p,"
                            " old ~p, new ~p", [SpaceID, Seq, FLS]),
                            State#state{seq = FLS}
                    end;
                _ ->
                    ?info("Reset changes seq with first batch for space ~p,"
                    " old ~p, new ~p", [SpaceID, Seq, Lower]),
                    State#state{seq = Lower}
            end;
        _ ->
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves changes batch in a cache. If save operation would cause cache size
%% limit overflow, changes are dropped.
%% @end
%%--------------------------------------------------------------------
-spec stash_changes_batch(couchbase_changes:since(), couchbase_changes:until(),
    couchbase_changes:timestamp(), [datastore:doc()], state()) -> state().
stash_changes_batch(Since, Until, Timestamp, Docs, State = #state{
    changes_stash = Stash,
    seq = Seq
}) ->
    Max = application:get_env(?APP_NAME, dbsync_changes_stash_max_size, 100000),
    case Until > Seq + Max of
        true ->
            case ets:first(Stash) of
                '$end_of_table' ->
                    % Init stash after failure to have range for changes request
                    ets:insert(Stash, {{Since, Until}, {Timestamp, Docs}});
                _ ->
                    ok
            end,
            State;
        false ->
            ets:insert(Stash, {{Since, Until}, {Timestamp, Docs}}),
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies changes batch and changes that have been stashed, as long as they
%% constitute a continuous range.
%% @end
%%--------------------------------------------------------------------
-spec apply_changes_batch(couchbase_changes:since(), couchbase_changes:until(),
    couchbase_changes:timestamp(), [datastore:doc()], state()) -> state().
apply_changes_batch(Since, Until, Timestamp, Docs, State) ->
    State2 = cancel_changes_request(State),
    {Docs2, Timestamp2, Until2, State3} = prepare_batch(Docs, Timestamp, Until, State2),
    dbsync_changes:apply_batch(Docs2, {Since, Until2}, Timestamp2),

    case application:get_env(?APP_NAME, dbsync_in_stream_worker_gc, on) of
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
-spec change_applied(couchbase_changes:since(), couchbase_changes:until(), couchbase_changes:timestamp(),
    ok | timeout | {error, datastore_doc:seq(), term()}, state()) -> state().
change_applied(_Since, Until, Timestamp, Ans, State) ->
    State2 = State#state{apply_batch = undefined},
    case Ans of
        ok ->
            gen_server2:cast(self(), check_batch_stash),
            update_seq(Until, Timestamp, State2);
        {error, Seq, _} ->
            State3 = update_seq(Seq - 1, 0, State2),
            schedule_changes_request(State3);
        timeout ->
            schedule_changes_request(State2)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares batch to be applied. If there are no missing changes between batches
%% it merges them.
%% @end
%%--------------------------------------------------------------------
-spec prepare_batch([datastore:doc()], couchbase_changes:timestamp(), couchbase_changes:until(), state()) ->
    {[datastore:doc()], couchbase_changes:timestamp(), couchbase_changes:until(), state()}.
prepare_batch(Docs, Timestamp, Until, State = #state{
    changes_stash = Stash
}) ->
    case ets:first(Stash) of
        '$end_of_table' ->
            {Docs, Timestamp, Until, State};
        {Until, NextUntil} = Key ->
            MaxSize = application:get_env(?APP_NAME,
                dbsync_changes_apply_max_size, 500),
            case length(Docs) > MaxSize of
                true ->
                    {Docs, Timestamp, Until, State};
                _ ->
                    {NextTimestamp, NextDocs} = ets:lookup_element(Stash, Key, 2),
                    ets:delete(Stash, Key),
                    prepare_batch(Docs ++ NextDocs, NextTimestamp, NextUntil, State)
            end;
        _ ->
            {Docs, Timestamp, Until, schedule_changes_request(State)}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence number of the beginning of expected changes range.
%% @end
%%--------------------------------------------------------------------
-spec update_seq(couchbase_changes:seq(), couchbase_changes:timestamp(), state()) -> state().
update_seq(Seq, _Timestamp, State = #state{seq = Seq}) ->
    State;
update_seq(Seq, Timestamp, State = #state{space_id = SpaceId, provider_id = ProviderId}) ->
    dbsync_state:set_seq(SpaceId, ProviderId, Seq, Timestamp),
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
    Delay = application:get_env(
        ?APP_NAME, dbsync_changes_request_delay, timer:seconds(15)
    ),
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
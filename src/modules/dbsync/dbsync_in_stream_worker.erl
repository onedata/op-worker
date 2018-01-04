%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
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
    apply_batch :: undefined | couchbase_changes:until()
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
    {ok, #state{
        space_id = SpaceId,
        provider_id = ProviderId,
        seq = dbsync_state:get_seq(SpaceId, ProviderId),
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
handle_cast({changes_batch, Since, Until, Docs}, State = #state{
    space_id = SpaceId,
    provider_id = ProviderId
}) ->
    Supported = dbsync_utils:is_supported(SpaceId, [
        oneprovider:get_id(fail_with_throw),
        ProviderId
    ]),
    case Supported of
        true -> {noreply, handle_changes_batch(Since, Until, Docs, State)};
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
            Docs = ets:lookup_element(Stash, Key, 2),
            ets:delete(Stash, Key),
            {noreply, apply_changes_batch(Seq, Until, Docs, State)};
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
handle_info({batch_applied, {Since, Until}, Ans}, #state{} = State) ->
    State2 = change_applied(Since, Until, Ans, State),
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
    [datastore:doc()], state()) -> state().
handle_changes_batch(Since, Until, Docs,
    State = #state{seq = Seq, apply_batch = Apply}) ->
    case {Since, Apply} of
        {Seq, undefined} ->
            apply_changes_batch(Since, Until, Docs, State);
        {Higher, undefined} when Higher > Seq ->
            State2 = stash_changes_batch(Since, Until, Docs, State),
            schedule_changes_request(State2);
        {Higher, _} when Higher >= Apply ->
            State2 = stash_changes_batch(Since, Until, Docs, State),
            schedule_changes_request(State2);
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
    [datastore:doc()], state()) -> state().
stash_changes_batch(Since, Until, Docs, State = #state{
    changes_stash = Stash,
    seq = Seq
}) ->
    Max = application:get_env(?APP_NAME, dbsync_changes_stash_max_size, 100000),
    case Until > Seq + Max of
        true ->
            case ets:first(Stash) of
                '$end_of_table' ->
                    % Init stash after failure to have range for changes request
                    ets:insert(Stash, {{Since, Until}, Docs});
                _ ->
                    ok
            end,
            State;
        false ->
            ets:insert(Stash, {{Since, Until}, Docs}),
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
    [datastore:doc()], state()) -> state().
apply_changes_batch(Since, Until, Docs, State) ->
    State2 = cancel_changes_request(State),
    {Docs2, Until2, State3} = prepare_batch(Docs, Until, State2),
    dbsync_changes:apply_batch(Docs2, {Since, Until2}),
    State3#state{apply_batch = Until2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence when changes applying ends.
%% @end
%%--------------------------------------------------------------------
-spec change_applied(couchbase_changes:since(), couchbase_changes:until(),
    ok | timeout | {error, datastore:seq(), term()}, state()) -> state().
change_applied(_Since, Until, Ans, State) ->
    State2 = State#state{apply_batch = undefined},
    case Ans of
        ok ->
            gen_server2:cast(self(), check_batch_stash),
            update_seq(Until, State2);
        {error, Seq, _} ->
            State3 = update_seq(Seq - 1, State2),
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
-spec prepare_batch([datastore:doc()], couchbase_changes:until(), state()) ->
    {[datastore:doc()], couchbase_changes:until(), state()}.
prepare_batch(Docs, Until, State = #state{
    changes_stash = Stash
}) ->
    case ets:first(Stash) of
        '$end_of_table' ->
            {Docs, Until, State};
        {Until, NextUntil} = Key ->
            MaxSize = application:get_env(?APP_NAME,
                dbsync_changes_apply_max_size, 500),
            case length(Docs) > MaxSize of
                true ->
                    {Docs, Until, State};
                _ ->
                    NextDocs = ets:lookup_element(Stash, Key, 2),
                    ets:delete(Stash, Key),
                    prepare_batch(Docs ++ NextDocs, NextUntil, State)
            end;
        _ ->
            {Docs, Until, schedule_changes_request(State)}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates sequence number of the beginning of expected changes range.
%% @end
%%--------------------------------------------------------------------
-spec update_seq(couchbase_changes:seq(), state()) -> state().
update_seq(Seq, State = #state{seq = Seq}) ->
    State;
update_seq(Seq, State = #state{space_id = SpaceId, provider_id = ProviderId}) ->
    dbsync_state:set_seq(SpaceId, ProviderId, Seq),
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
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
    changes_stash_size :: non_neg_integer(),
    changes_request_ref :: undefined | reference()
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
        seq = dbsync_state2:get_seq(SpaceId, ProviderId),
        changes_stash = ets:new(changes_stash, [ordered_set, private]),
        changes_stash_size = 0
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
    Supported = dbsync_utils2:is_supported(SpaceId, [
        oneprovider:get_provider_id(),
        ProviderId
    ]),
    case Supported of
        true -> {noreply, handle_changes_batch(Since, Until, Docs, State)};
        false -> {stop, normal, State}
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
handle_info({request_changes, Seq}, State = #state{
    space_id = SpaceId,
    provider_id = ProviderId,
    seq = Seq,
    changes_stash = Stash
}) ->
    case ets:first(Stash) of
        '$end_of_table' ->
            {noreply, State#state{changes_request_ref = undefined}};
        {Until, _} ->
            dbsync_communicator:request_changes(
                ProviderId, SpaceId, Seq, Until
            ),
            {noreply, schedule_changes_request(State#state{
                changes_request_ref = undefined
            })}
    end;
handle_info(_Info, #state{} = State) ->
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
handle_changes_batch(Since, Until, Docs, State = #state{seq = Seq}) ->
    case Since == Seq of
        true ->
            apply_changes_batch(Since, Until, Docs, State);
        false ->
            State2 = stash_changes_batch(Since, Until, Docs, State),
            schedule_changes_request(State2)
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
    changes_stash_size = Size
}) ->
    DocsNum = length(Docs),
    Max = application:get_env(?APP_NAME, dbsync_changes_stash_max_size, 1000),
    case Size + DocsNum > Max of
        true ->
            State;
        false ->
            ets:insert(Stash, {{Since, Until}, Docs}),
            State#state{changes_stash_size = Size + DocsNum}
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
apply_changes_batch(_Since, Until, Docs, State = #state{
    changes_stash = Stash,
    changes_stash_size = Size
}) ->
    State2 = cancel_changes_request(State),
    lists:foreach(fun(Doc) ->
        dbsync_changes:apply(Doc)
    end, Docs),
    State3 = update_seq(Until, State2),
    case ets:first(Stash) of
        '$end_of_table' ->
            State3;
        {Until, NextUntil} = Key ->
            NextDocs = ets:lookup_element(Stash, Key, 2),
            ets:delete(Stash, Key),
            apply_changes_batch(Until, NextUntil, NextDocs, State3#state{
                changes_stash_size = Size - length(NextDocs)
            });
        _ ->
            schedule_changes_request(State3)
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
    dbsync_state2:set_seq(SpaceId, ProviderId, Seq),
    State#state{seq = Seq}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules changes request, if there is not other request scheduled already.
%% @end
%%--------------------------------------------------------------------
-spec schedule_changes_request(state()) -> state().
schedule_changes_request(State = #state{
    seq = Seq,
    changes_request_ref = undefined
}) ->
    Delay = application:get_env(
        ?APP_NAME, dbsync_changes_request_delay, timer:seconds(5)
    ),
    State#state{changes_request_ref = erlang:send_after(
        Delay, self(), {request_changes, Seq}
    )};
schedule_changes_request(State = #state{}) ->
    State.

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
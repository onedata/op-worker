%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for aggregation and broadcasting changes from
%%% a space to all supporting providers. It works in two modes: default
%%% and recovery. Default outgoing streams are globally registered and they
%%% broadcast changes infinitely for all spaces supported by this provider.
%%% Recovery streams are started ad hoc, when remote provider spots that he
%%% is missing some changes, that have already been broadcast by this provider.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_out_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type filter() :: fun((datastore:doc()) -> boolean()).
-type handler() :: fun((couchbase_changes:since(),
                        couchbase_changes:until() | end_of_stream,
                        [datastore:doc()]) -> any()).
-type option() :: {register, boolean()} |
                  {filter, filter()} |
                  {handler, handler()} |
                  {handling_interval, non_neg_integer()} |
                  couchbase_changes:option().

-export_type([option/0]).

-record(state, {
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    changes :: [datastore:doc()],
    filter :: filter(),
    handler :: handler(),
    handling_ref :: undefined | reference(),
    handling_interval :: non_neg_integer()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts stream for outgoing changes from a space and registers
%% it globally.
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_space:id(), [option()]) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(SpaceId, Opts) ->
    case proplists:get_value(register, Opts, false) of
        true ->
            Name = {?MODULE, SpaceId},
            gen_server2:start_link({global, Name}, ?MODULE, [SpaceId, Opts], []);
        false ->
            gen_server2:start_link(?MODULE, [SpaceId, Opts], [])
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes DBSync outgoing stream.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SpaceId, Opts]) ->
    Stream = self(),
    Bucket = dbsync_utils:get_bucket(),
    Since = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    Callback = fun(Change) -> gen_server:cast(Stream, {change, Change}) end,
    case proplists:get_value(register, Opts, false) of
        true -> {ok, _} = couchbase_changes_worker:start_link(Bucket, SpaceId);
        false -> ok
    end,
    {ok, _} = couchbase_changes_stream:start_link(Bucket, SpaceId, Callback, [
        {since, proplists:get_value(since, Opts, Since)},
        {until, proplists:get_value(until, Opts, infinity)},
        {except_mutator, proplists:get_value(except_mutator, Opts, <<>>)}
    ]),
    {ok, schedule_docs_handling(#state{
        since = proplists:get_value(since, Opts, Since),
        until = proplists:get_value(since, Opts, Since),
        changes = [],
        filter = proplists:get_value(filter, Opts, fun(_) -> true end),
        handler = proplists:get_value(handler, Opts, fun(_, _, _) -> ok end),
        handling_interval = proplists:get_value(handling_interval, Opts, 5000)
    })}.

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
handle_cast({change, {ok, Docs}}, State = #state{
    filter = Filter
}) when is_list(Docs)->
    FinalState = lists:foldl(fun(Doc, TmpState) ->
        handle_doc_change(Doc, Filter, TmpState)
    end, State, Docs),
    {noreply, FinalState};
handle_cast({change, {ok, #document{} = Doc}}, State = #state{
    filter = Filter
}) ->
    {noreply, handle_doc_change(Doc, Filter, State)};
handle_cast({change, {ok, end_of_stream}}, State = #state{
    since = Since,
    until = Until,
    changes = Docs,
    handler = Handler
}) ->
    Handler(Since, end_of_stream, lists:reverse(Docs)),
    {stop, normal, State#state{since = Until, changes = []}};
handle_cast({change, {error, Seq, Reason}}, State = #state{
    since = Since,
    changes = Docs,
    handler = Handler
}) ->
    Handler(Since, Seq, lists:reverse(Docs)),
    {stop, Reason, State#state{since = Seq, changes = []}};
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
handle_info(handle_changes, State = #state{}) ->
    {noreply, handle_changes(State)};
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
%% Aggregates change. Handles aggregated changes if batch size is reached.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_change(datastore:doc(), state()) -> state().
aggregate_change(Doc = #document{seq = Seq}, State = #state{changes = Docs}) ->
    State2 = State#state{
        until = Seq + 1,
        changes = [Doc | Docs]
    },
    Len = application:get_env(?APP_NAME, dbsync_changes_broadcast_batch_size, 100),
    case erlang:length(Docs) + 1 >= Len of
        true -> handle_changes(State2);
        false -> State2
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles changes by executing provided handler.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes(state()) -> state().
handle_changes(State = #state{
    since = Since,
    until = Until,
    changes = Docs,
    handler = Handler
}) ->
    MinSize = application:get_env(?APP_NAME, dbsync_handler_spawn_size, 10),
    case length(Docs) >= MinSize of
        true ->
            spawn(fun() ->
                Handler(Since, Until, lists:reverse(Docs))
            end);
        _ ->
            try
                Handler(Since, Until, lists:reverse(Docs))
            catch
                _:_ ->
                    % Handle should catch own errors
                    % try/catch only to protect stream
                    ok
            end
    end,

    case application:get_env(?APP_NAME, dbsync_out_stream_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    schedule_docs_handling(State#state{since = Until, changes = []}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules handling of aggregated changes.
%% @end
%%--------------------------------------------------------------------
-spec schedule_docs_handling(state()) -> state().
schedule_docs_handling(#state{
    handling_ref = undefined,
    handling_interval = Interval
} = State) ->
    State#state{
        handling_ref = erlang:send_after(Interval, self(), handle_changes)
    };
schedule_docs_handling(State = #state{handling_ref = Ref}) ->
    erlang:cancel_timer(Ref),
    schedule_docs_handling(State#state{handling_ref = undefined}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles change that include document.
%% @end
%%--------------------------------------------------------------------
-spec handle_doc_change(datastore:doc(), filter(), state()) -> state().
handle_doc_change(#document{seq = Seq} = Doc, Filter,
    State = #state{until = Until}) when Seq >= Until ->
    case Filter(Doc) of
        true -> aggregate_change(Doc, State);
        false -> State#state{until = Seq + 1}
    end;
handle_doc_change(#document{seq = Seq} = Doc, _Filter,
    State = #state{until = Until}) ->
    ?error("Received change with old sequence ~p. Expected sequences"
    " greater than or equal to ~p~n~p", [Seq, Until, Doc]),
    State.
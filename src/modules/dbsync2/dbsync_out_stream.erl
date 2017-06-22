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

-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type filter() :: fun((datastore:doc()) -> boolean()).
-type handler() :: fun((couchbase_changes:since(), couchbase_changes:until(),
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
    docs :: [datastore:doc()],
    filter :: filter(),
    handler :: handler(),
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
    Bucket = dbsync_utils2:get_bucket(),
    Since = dbsync_state2:get_seq(SpaceId, oneprovider:get_provider_id()),
    Callback = fun(Change) -> gen_server:cast(Stream, {change, Change}) end,
    case proplists:get_value(register, Opts, false) of
        true -> couchbase_changes_worker:start_link(Bucket, SpaceId);
        false -> ok
    end,
    couchbase_changes_stream:start_link(Bucket, SpaceId, Callback, [
        {since, proplists:get_value(since, Opts, Since)},
        {until, proplists:get_value(until, Opts, infinity)},
        {except_mutator, proplists:get_value(except_mutator, Opts, <<>>)}
    ]),
    {ok, schedule_handling(#state{
        since = proplists:get_value(since, Opts, Since),
        until = proplists:get_value(since, Opts, Since),
        docs = [],
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
handle_cast({change, {ok, #document{seq = Seq} = Doc}}, State = #state{
    docs = Docs,
    filter = Filter
}) ->
    case Filter(Doc) of
        true -> {noreply, State#state{until = Seq + 1, docs = [Doc | Docs]}};
        false -> {noreply, State#state{until = Seq + 1}}
    end;
handle_cast({change, {ok, end_of_stream}}, State = #state{
    since = Since,
    until = Until,
    docs = Docs,
    handler = Handler
}) ->
    Handler(Since, end_of_stream, lists:reverse(Docs)),
    {stop, normal, State#state{since = Until, docs = []}};
handle_cast({change, {error, Seq, Reason}}, State = #state{
    since = Since,
    docs = Docs,
    handler = Handler
}) ->
    ?info("ccccc2 ~p", [{Since, Docs}]),
    Handler(Since, Seq, lists:reverse(Docs)),
    {stop, Reason, State#state{since = Seq, docs = []}};
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
handle_info(handle_changes, State = #state{
    since = Since,
    until = Until,
    docs = Docs,
    handler = Handler
}) ->
    ?info("ccccc ~p", [{Since, Until, Docs}]),
    Handler(Since, Until, lists:reverse(Docs)),
    {noreply, schedule_handling(State#state{since = Until, docs = []})};
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
%% Schedules handling of aggregated changes (broadcast or direct send).
%% @end
%%--------------------------------------------------------------------
-spec schedule_handling(state()) -> state().
schedule_handling(#state{handling_interval = Interval} = State) ->
    erlang:send_after(Interval, self(), handle_changes),
    State.
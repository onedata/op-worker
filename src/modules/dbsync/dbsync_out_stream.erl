%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for aggregation and broadcasting changes from
%%% a space to all supporting providers. It works in two modes: default
%%% and recovery. Main outgoing streams broadcast changes infinitely for
%%% all spaces supported by this provider.
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
-export([start_link/3, try_terminate/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type filter() :: fun((datastore:doc()) -> boolean()).
-type handler() :: fun((couchbase_changes:since(),
                        couchbase_changes:until() | end_of_stream,
                        dbsync_changes:timestamp(),
                        [datastore:doc()]) -> any()).
-type option() :: {main_stream, boolean()} |
                  {filter, filter()} |
                  {handler, handler()} |
                  {handling_interval, non_neg_integer()} |
                  couchbase_changes:option().

-export_type([option/0]).

-record(state, {
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    changes :: [datastore:doc()],
    last_handled_change :: datastore:doc() | undefined,
    filter :: filter(),
    handler :: handler(),
    handling_ref :: undefined | reference(),
    handling_interval :: non_neg_integer(),
    stream_pid :: pid() | undefined
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
-spec start_link(binary(), od_space:id(), [option()]) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Name, SpaceId, Opts) ->
    gen_server2:start_link({global, {?MODULE, Name}}, ?MODULE, [SpaceId, Opts], []).


-spec try_terminate(binary(), couchbase_changes:since()) -> ok | ignore.
try_terminate(Name, SinceToIgnoreTerminate) ->
    try
        gen_server2:call({global, {?MODULE, Name}}, {terminate, SinceToIgnoreTerminate}, infinity)
    catch
        _:{noproc, _} ->
            ok; % Ignore terminated process
        exit:{normal, _} ->
            ok  % Ignore terminated process
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

    StreamPid = case proplists:get_value(main_stream, Opts, false) of
        true ->
            {ok, _} = couchbase_changes_worker:start_link(Bucket, SpaceId, Callback,
                proplists:get_value(since, Opts, Since)),
            undefined;
        false ->
            {ok, Pid} = couchbase_changes_stream:start_link(Bucket, SpaceId, Callback, [
                {since, proplists:get_value(since, Opts, Since)},
                {until, proplists:get_value(until, Opts, infinity)},
                {except_mutator, proplists:get_value(except_mutator, Opts, <<>>)}
            ], []),
            Pid
    end,

    {ok, schedule_docs_handling(#state{
        since = proplists:get_value(since, Opts, Since),
        until = proplists:get_value(since, Opts, Since),
        changes = [],
        filter = proplists:get_value(filter, Opts, fun(_) -> true end),
        handler = proplists:get_value(handler, Opts, fun(_, _, _) -> ok end),
        handling_interval = proplists:get_value(handling_interval, Opts, 5000),
        stream_pid = StreamPid
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
handle_call({terminate, SinceToIgnoreTerminate}, _From, #state{since = SinceToIgnoreTerminate} = State) ->
    {reply, ignore, State};
handle_call({terminate, _}, _From, #state{stream_pid = undefined} = State) ->
    {stop, normal, ok, State};
handle_call({terminate, _}, _From, #state{stream_pid = Pid} = State) ->
    couchbase_changes_stream:stop_async(Pid),
    {stop, normal, ok, State};
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
    FinalState = lists:foldl(fun({change, Doc}, TmpState) ->
        handle_doc_change(Doc, Filter, TmpState)
    end, State, Docs),
    {noreply, FinalState};
handle_cast({change, {ok, {change, #document{} = Doc}}}, State = #state{
    filter = Filter
}) ->
    {noreply, handle_doc_change(Doc, Filter, State)};
handle_cast({change, {ok, end_of_stream}}, State = #state{
    since = Since,
    until = Until,
    changes = Docs,
    handler = Handler
}) ->
    Handler(Since, end_of_stream, get_batch_timestamp(Docs), lists:reverse(Docs)),
    {stop, normal, State#state{since = Until, changes = []}};
handle_cast({change, {error, Seq, Reason}}, State = #state{
    since = Since,
    changes = Docs,
    handler = Handler
}) ->
    Handler(Since, Seq, get_batch_timestamp(Docs), lists:reverse(Docs)),
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
    Len = op_worker:get_env(dbsync_changes_broadcast_batch_size, 100),
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
    changes = ReversedDocs,
    last_handled_change = LastHandledChange,
    handler = Handler
}) ->
    MinSize = op_worker:get_env(dbsync_handler_spawn_size, 10),
    case length(ReversedDocs) >= MinSize of
        true ->
            spawn(fun() ->
                Handler(Since, Until, get_batch_timestamp(ReversedDocs), lists:reverse(ReversedDocs))
            end);
        _ ->
            try
                Handler(Since, Until, get_batch_timestamp(ReversedDocs), lists:reverse(ReversedDocs))
            catch
                _:_ ->
                    % Handle should catch own errors
                    % try/catch only to protect stream
                    ok
            end
    end,

    case op_worker:get_env(dbsync_out_stream_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    NewLastHandledChange = case ReversedDocs of
        [LastDoc | _] -> LastDoc;
        _ -> LastHandledChange
    end,

    schedule_docs_handling(State#state{since = Until, changes = [], last_handled_change = NewLastHandledChange}).

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
handle_doc_change(#document{seq = Seq} = Doc, Filter, State = #state{until = Until}) when Seq >= Until ->
    case Filter(Doc) of
        true -> aggregate_change(Doc, State);
        false -> State#state{until = Seq + 1}
    end;
handle_doc_change(#document{seq = Seq} = Doc, _Filter, State = #state{until = Until}) ->
    case get_last_change(State) of
        undefined ->
            % couchbase_changes_worker can be initialized with lower sequence if some sequence documents have
            % not been cleaned before restart - ignore errors until first document is handled
            ok;
        LastChange ->
            ?error("Received change with old sequence ~tp. Expected sequences"
            " greater than or equal to ~tp~nDoc: ~tp~nLast change: ~tp", [Seq, Until, Doc, LastChange]),
            case LastChange of
                #document{seq = Seq} -> update_doc_seq(Doc);
                _ -> ok
            end
    end,
    State.

%% @private
-spec get_batch_timestamp([datastore:doc()]) -> dbsync_changes:timestamp().
get_batch_timestamp([]) ->
    undefined;
get_batch_timestamp([#document{timestamp = null} | _]) ->
    undefined;
get_batch_timestamp([#document{timestamp = Timestamp} | _]) ->
    Timestamp.

%% @private
-spec get_last_change(state()) -> datastore:doc() | undefined.
get_last_change(#state{changes = [LastChange | _]}) ->
    LastChange;
get_last_change(#state{last_handled_change = LastHandledChange}) ->
    LastHandledChange.


%% @private
-spec update_doc_seq(datastore:doc()) -> ok.
update_doc_seq(#document{value = #links_forest{model = Model, key = RoutingKey}} = Doc) ->
    update_link_doc_seq(Model, RoutingKey, Doc);
update_doc_seq(#document{value = #links_node{model = Model, key = RoutingKey}} = Doc) ->
    update_link_doc_seq(Model, RoutingKey, Doc);
update_doc_seq(#document{value = #links_mask{model = Model, key = RoutingKey}} = Doc) ->
    update_link_doc_seq(Model, RoutingKey, Doc);
update_doc_seq(#document{key = Key, value = Value} = Doc) ->
    Model = element(1, Value),
    Ctx = dbsync_changes:get_ctx(Model, Doc),
    Ctx2 = Ctx#{sync_change => true, hooks_disabled => true, include_deleted => true},
    case datastore_model:update(Ctx2, Key, fun(Record) -> {ok, Record} end) of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end.


%% @private
-spec update_link_doc_seq(datastore_model:model(), datastore:key(), datastore:doc()) -> ok.
update_link_doc_seq(Model, RoutingKey, #document{key = Key} = Doc) ->
    Ctx = dbsync_changes:get_ctx(Model, Doc),
    Ctx2 = Ctx#{
        local_links_tree_id => oneprovider:get_id(),
        routing_key => RoutingKey,
        include_deleted => true
    },
    Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
    case datastore_router:route(update, [Ctx3, Key, fun(Record) -> {ok, Record} end]) of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end.
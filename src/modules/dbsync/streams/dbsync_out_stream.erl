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
-include("modules/dbsync/dbsync.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type batch_handler() :: fun((couchbase_changes:since(),
                              couchbase_changes:until() | end_of_stream,
                              dbsync_changes:timestamp(),
                              [datastore:doc()]) -> any()).
-type option() :: {main_stream, boolean()} |
                  {local_changes_only, boolean()} |
                  {batch_handler, batch_handler()} |
                  {handling_interval, non_neg_integer()} |
                  couchbase_changes:option().
-type remote_sequence_info() :: #remote_sequence_info{}.

-export_type([option/0, remote_sequence_info/0]).
-record(state, {
    space_id :: od_space:id(),
    main_stream :: boolean(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    local_changes_only :: boolean(),
    except_mutator = <<>> :: oneprovider:id(),
    % TODO VFS-7031 eliminate undefined/null/0 from protocol
    % (init last_timestamp from db, do not clean on flush)
    last_timestamp :: dbsync_changes:timestamp(),
    batch_handler :: batch_handler(),
    handling_ref :: undefined | reference(),
    handling_interval :: non_neg_integer(),
    batch_cache :: dbsync_out_stream_batch_cache:cache()
}).

-type state() :: #state{}.

-define(DEFAULT_HANDLING_INTERVAL,
    application:get_env(?APP_NAME, dbsync_out_stream_handling_interval, timer:seconds(5))).

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
    MainStream = proplists:get_value(main_stream, Opts, false),

    {ok, _} = case MainStream of
        true ->
            couchbase_changes_worker:start_link(Bucket, SpaceId, Callback,
                proplists:get_value(since, Opts, Since));
        false ->
            couchbase_changes_stream:start_link(Bucket, SpaceId, Callback, [
                {since, proplists:get_value(since, Opts, Since)},
                {until, proplists:get_value(until, Opts, infinity)}
            ], [])
    end,

    {ok, schedule_docs_handling(#state{
        space_id = SpaceId,
        main_stream = MainStream,
        since = proplists:get_value(since, Opts, Since),
        until = proplists:get_value(since, Opts, Since),
        local_changes_only = proplists:get_value(local_changes_only, Opts, false),
        except_mutator = proplists:get_value(except_mutator, Opts, <<>>),
        batch_handler = proplists:get_value(batch_handler, Opts, fun(_, _, _) -> ok end),
        handling_interval = proplists:get_value(handling_interval, Opts, ?DEFAULT_HANDLING_INTERVAL),
        batch_cache = dbsync_out_stream_batch_cache:empty()
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
    local_changes_only = LocalChangesOnly,
    except_mutator = ExceptMutator
}) when is_list(Docs)->
    FinalState = lists:foldl(fun(Doc, TmpState) ->
        handle_doc_change(Doc, LocalChangesOnly, ExceptMutator, TmpState)
    end, State, Docs),
    {noreply, FinalState};
handle_cast({change, {ok, #document{} = Doc}}, State = #state{
    local_changes_only = LocalChangesOnly,
    except_mutator = ExceptMutator
}) ->
    {noreply, handle_doc_change(Doc, LocalChangesOnly, ExceptMutator, State)};
handle_cast({change, {ok, end_of_stream}}, State = #state{
    since = Since,
    until = Until,
    batch_cache = Cache,
    batch_handler = Handler,
    last_timestamp = Timestamp
}) ->
    Handler(Since, end_of_stream, Timestamp, dbsync_out_stream_batch_cache:get_changes(Cache)),
    % TODO VFS-7033 - verify behaviour if node fails here
    % (after handler execution but before correlation processing)
    process_remote_mutations(State),
    {stop, normal, State#state{since = Until,
        batch_cache = dbsync_out_stream_batch_cache:empty(), last_timestamp = undefined}};
handle_cast({change, {error, Seq, Reason}}, State = #state{
    since = Since,
    batch_cache = Cache,
    batch_handler = Handler,
    last_timestamp = Timestamp
}) ->
    Handler(Since, Seq, Timestamp, dbsync_out_stream_batch_cache:get_changes(Cache)),
    process_remote_mutations(State),
    {stop, Reason, State#state{since = Seq,
        batch_cache = dbsync_out_stream_batch_cache:empty(), last_timestamp = undefined}};
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
handle_info(handle_changes_batch, State = #state{}) ->
    {noreply, handle_changes_batch(State)};
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
aggregate_change(Doc = #document{seq = Seq}, State = #state{batch_cache = Cache}) ->
    UpdatedCache = dbsync_out_stream_batch_cache:cache_change(Doc, Cache),
    State2 = State#state{
        until = Seq + 1,
        batch_cache = UpdatedCache
    },
    Len = application:get_env(?APP_NAME, dbsync_changes_broadcast_batch_size, 100),
    case dbsync_out_stream_batch_cache:get_changes_num(UpdatedCache) >= Len of
        true -> handle_changes_batch(State2);
        false -> State2
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles changes batch by executing provided handler.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_batch(state()) -> state().
handle_changes_batch(State = #state{
    since = Since,
    until = Until,
    batch_cache = Cache,
    batch_handler = Handler,
    last_timestamp = Timestamp
}) ->
    Docs = dbsync_out_stream_batch_cache:get_changes(Cache),
    MinSize = application:get_env(?APP_NAME, dbsync_handler_spawn_size, 10),
    case dbsync_out_stream_batch_cache:get_changes_num(Cache) >= MinSize of
        true ->
            % TODO VFS-7034 - possible race between two consecutive batches?
            spawn(fun() ->
                Handler(Since, Until, Timestamp, Docs),
                process_remote_mutations(State)
            end);
        _ ->
            try
                Handler(Since, Until, Timestamp, Docs)
            catch
                _:_ ->
                    % Handle should catch own errors
                    % try/catch only to protect stream
                    ok
            end,
            process_remote_mutations(State)
    end,

    case application:get_env(?APP_NAME, dbsync_out_stream_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    schedule_docs_handling(State#state{since = Until,
        batch_cache = dbsync_out_stream_batch_cache:empty(), last_timestamp = undefined}).

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
        handling_ref = erlang:send_after(Interval, self(), handle_changes_batch)
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
-spec handle_doc_change(datastore:doc(), boolean(), oneprovider:id(), state()) -> state().
handle_doc_change(#document{seq = Seq} = Doc, LocalChangesOnly, ExceptMutator,
    State = #state{until = Until}) when Seq >= Until ->
    StateWithTimestampAndMutations = update_timestamp(Doc, cache_remote_mutations(Doc, State)),
    case is_synced_change(Doc, LocalChangesOnly, ExceptMutator) of
        true -> aggregate_change(Doc#document{remote_sequences = #{}}, StateWithTimestampAndMutations);
        false -> StateWithTimestampAndMutations#state{until = Seq + 1}
    end;
handle_doc_change(#document{seq = Seq} = Doc, _LocalChangesOnly, _ExceptMutator,
    State = #state{until = Until}) ->
    ?error("Received change with old sequence ~p. Expected sequences"
    " greater than or equal to ~p~n~p", [Seq, Until, Doc]),
    State.

%% @private
-spec update_timestamp(datastore:doc(), state()) -> state().
update_timestamp(#document{timestamp = null}, State) ->
    State;
update_timestamp(#document{timestamp = Timestamp}, State) ->
    State#state{last_timestamp = Timestamp}.

%% @private
-spec is_synced_change(datastore:doc(), boolean(), oneprovider:id()) -> boolean().
is_synced_change(#document{mutators = [Mutator | _]}, true, _ExceptMutator) ->
    Mutator =:= oneprovider:get_id();
is_synced_change(#document{mutators = [Mutator | _]}, false, ExceptMutator) ->
    Mutator =/= ExceptMutator;
is_synced_change(#document{} = Doc, _LocalChangesOnly, _ExceptMutator) ->
    ?error("Document without mutator in ~p: ~p", [?MODULE, Doc]),
    false.

%% @private
-spec cache_remote_mutations(datastore:doc(), state()) -> state().
cache_remote_mutations(_Doc, #state{main_stream = false} = State) ->
    State;
cache_remote_mutations(Doc, #state{batch_cache = Cache} = State) ->
    State#state{batch_cache = dbsync_out_stream_batch_cache:cache_remote_mutations(Doc, Cache)}.

%% @private
-spec process_remote_mutations(state()) -> ok.
process_remote_mutations(#state{main_stream = false}) ->
    ok;
process_remote_mutations(#state{
    space_id = SpaceId,
    until = Until,
    batch_cache = Cache
}) ->
    % `Until` is number of next sequence that will appear in stream so subtract 1
    dbsync_seqs_correlation:set_sequences_correlation(SpaceId, Until - 1,
        dbsync_out_stream_batch_cache:get_remote_mutations(Cache)).
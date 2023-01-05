%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of stream workers
%%% {@link dbsync_in_stream_worker} associated with given space.
%%% It routes remote changes from a provider to a suitable worker. It ignores
%%% recent changes that have been already processed, by keeping bounded history
%%% of message ids.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_in_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/dbsync/dbsync.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    space_id :: od_space:id(),
    workers = #{} :: #{od_provider:id() => pid()},
    msg_id_history = queue:new() :: msg_id_history()
}).

-type msg_id_history() :: queue:queue(binary()).
-type state() :: #state{}.
-type mutators() :: [binary() | od_provider:id()]. % NOTE: special id values (values that are not provider ids)
                                                   %       are defined in dbsync.hrl
-export_type([mutators/0]).


-define(RESYNCHRONIZED_SEQS_ON_CLOSING_PROCEDURE_FAILURE,
    op_worker:get_env(resynchronized_seqs_on_closing_procedure_failure, 1000000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts stream for incoming remote changes from a space and registers
%% it globally.
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_space:id()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(SpaceId) ->
    Name = {?MODULE, SpaceId},
    gen_server2:start_link({global, Name}, ?MODULE, [SpaceId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes DBSync incoming stream.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SpaceId]) ->
    process_flag(trap_exit, true),
    {ok, #state{space_id = SpaceId}}.

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
handle_call({resynchronize, ProviderId, IncludedMutators, StartSeq, TargetSeq}, _From, State) ->
    {reply, ok, resynchronize(ProviderId, IncludedMutators, StartSeq, TargetSeq, State)};
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
handle_cast({changes_batch, MsgId, ProviderId, Since, Until, Timestamp, Docs}, State) ->
    {noreply, handle_changes_batch(
        MsgId, ProviderId, Since, Until, Timestamp, Docs, State
    )};
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
handle_info({'EXIT', Pid, _Reason}, State = #state{workers = Workers}) ->
    Workers2 = maps:fold(fun
        (_ProviderId, Worker, Acc) when Worker =:= Pid -> Acc;
        (ProviderId, Worker, Acc) -> maps:put(ProviderId, Worker, Acc)
    end, #{}, Workers),
    {noreply, State#state{workers = Workers2}};
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
%% Handles changes batch. Ignores changes that have recently been processed,
%% otherwise forwards then to an associated stream worker.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_batch(undefined | dbsync_communicator:msg_id(),
    od_provider:id(), couchbase_changes:since(), couchbase_changes:until(),
    dbsync_changes:timestamp(), [datastore:doc()], state()) -> state().
handle_changes_batch(undefined, ProviderId, Since, Until, Timestamp, Docs, State) ->
    forward_changes_batch(ProviderId, Since, Until, Timestamp, Docs, State);
handle_changes_batch(MsgId, ProviderId, Since, Until, Timestamp, Docs, State = #state{
    msg_id_history = History
}) ->
    case queue:member(MsgId, History) of
        true ->
            State;
        false ->
            forward_changes_batch(ProviderId, Since, Until, Timestamp, Docs, State#state{
                msg_id_history = save_msg_id(MsgId, History)
            })
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards changes batch to an associated worker. If worker is missing,
%% it is started.
%% @end
%%--------------------------------------------------------------------
-spec forward_changes_batch(od_provider:id(), couchbase_changes:since(), couchbase_changes:until(),
    dbsync_changes:timestamp(), [datastore:doc()], state()) -> state().
forward_changes_batch(ProviderId, Since, Until, Timestamp, Docs, State = #state{
    space_id = SpaceId,
    workers = Workers
}) ->
    State2 = case maps:find(ProviderId, Workers) of
        {ok, Worker} ->
            gen_server:cast(Worker, {changes_batch, Since, Until, Timestamp, Docs}),
            State;
        error ->
            resynchronize_if_closing_procedure_failed(SpaceId, ProviderId),
            {ok, Worker} = dbsync_in_stream_worker:start_link(
                SpaceId, ProviderId
            ),
            gen_server:cast(Worker, {changes_batch, Since, Until, Timestamp, Docs}),
            State#state{
                workers = maps:put(ProviderId, Worker, Workers)
            }
    end,

    case op_worker:get_env(dbsync_in_stream_gc, on) of
        on ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    State2.

%% @private
-spec resynchronize(od_provider:id(), mutators(), integer(), couchbase_changes:seq() | current, state()) -> state().
resynchronize(ProviderId, IncludedMutators, StartSeq, TargetSeq, State = #state{
    space_id = SpaceId,
    workers = Workers
}) ->
    State2 = case maps:find(ProviderId, Workers) of
        {ok, Worker} ->
            gen_server:call(Worker, terminate, infinity),
            State#state{
                workers = maps:remove(ProviderId, Workers)
            };
        error ->
            State
    end,

    dbsync_state:resynchronize_stream(SpaceId, ProviderId, IncludedMutators, StartSeq, TargetSeq),
    State2.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds message ID to a bounded history queue.
%% @end
%%--------------------------------------------------------------------
-spec save_msg_id(dbsync_communicator:msg_id(), msg_id_history()) ->
    msg_id_history().
save_msg_id(MsgId, History) ->
    MaxSize = op_worker:get_env(dbsync_msg_id_history_len, 10000),
    History2 = queue:in(MsgId, History),
    case queue:len(History2) > MaxSize of
        true ->
            {_, History3} = queue:out(History2),
            History3;
        false ->
            History2
    end.


%% @private
-spec resynchronize_if_closing_procedure_failed(od_space:id(), od_provider:id()) -> ok.
resynchronize_if_closing_procedure_failed(SpaceId, ProviderId) ->
    case check_closing_procedure() of
        succeeded ->
            ok;
        Error ->
            case has_resynchronized_on_closing_procedure_failure(SpaceId, ProviderId) of
                true ->
                    ok;
                false ->
                    case ?RESYNCHRONIZED_SEQS_ON_CLOSING_PROCEDURE_FAILURE of
                        0 ->
                            ?error("Possible errors dbsync in stream {~p, ~p} due to node closing problems: ~p",
                                [SpaceId, ProviderId, Error]),
                            ok;
                        Seq ->
                            ?info("Resynchronizing ~p sequences on dbsync in stream {~p, ~p} due to: ~p",
                                [Seq, SpaceId, ProviderId, Error]),
                            ok = dbsync_state:resynchronize_stream(
                                SpaceId, ProviderId, ?ALL_MUTATORS_EXCEPT_SENDER, -1 * Seq, current)
                    end
            end
    end.


%% @private
-spec check_closing_procedure() -> succeeded | {bad_closing_statuses | bad_nodes, list()}.
check_closing_procedure() ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, datastore_worker, get_application_closing_status, []),

    case BadNodes of
        [] ->
            FilteredRes = lists:filtermap(fun
                (?CLOSING_PROCEDURE_SUCCEEDED) -> false;
                (Error) -> {true, Error}
            end, Res),

            case FilteredRes of
                [] -> succeeded;
                _ -> {bad_closing_statuses, FilteredRes}
            end;
        _ ->
            {bad_nodes, BadNodes}
    end.


%% @private
-spec has_resynchronized_on_closing_procedure_failure(od_space:id(), od_provider:id()) -> boolean().
has_resynchronized_on_closing_procedure_failure(SpaceId, ProviderId) ->
    ProvidersResynchronized = node_cache:get({providers_resynchronized_on_closing_procedure_failure, SpaceId}, []),
    case lists:member(ProviderId, ProvidersResynchronized) of
        true ->
            true;
        false ->
            node_cache:put({providers_resynchronized_on_closing_procedure_failure, SpaceId},
                [ProviderId | ProvidersResynchronized]),
            false
    end.
%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @author Tomasz Lichon
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module updates worker_map on cluster state change
%%% @end
%%%-------------------------------------------------------------------
-module(request_dispatcher).
-author("Michal Wrzeszcz").
-author("Tomasz Lichon").
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% ETS used to hold workers mapping.
-define(LB_ADVICE_KEY, lb_advice).
-define(WORKER_MAP_ETS, workers_ets).

% Types used in request routing.
-type worker_name() :: atom().
-type worker_ref() :: worker_name() | {WorkerName :: worker_name(), Node :: node()}.
-export_type([worker_name/0, worker_ref/0]).

%% This record is used by requests_dispatcher (it contains its state).
-record(state, {
    % Time of last ld advice update received from dispatcher
    last_update = {0, 0, 0} :: {integer(), integer(), integer()}
}).

%% API
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Request routing API
-export([get_worker_node/1, get_worker_nodes/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts request_dispatcher
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Result when
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link() ->
    gen_server:start_link({local, ?DISPATCHER_NAME}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops request_dispatcher
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:cast(?DISPATCHER_NAME, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
init(_) ->
    process_flag(trap_exit, true),
    ets:new(?WORKER_MAP_ETS, [set, protected, named_table, {read_concurrency, true}]),
    % Insert undefined as LB advice - it means that the node is not yet initialized
    % and it should not accept requests to workers.
    ets:insert(?WORKER_MAP_ETS, {?LB_ADVICE_KEY, undefined}),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: nagios_handler:healthcheck_response() | term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
handle_call(healthcheck, _From, #state{last_update = LastUpdate} = State) ->
    % Report error as long as no LB advice has been received.
    Reply = case ets:lookup(?WORKER_MAP_ETS, ?LB_ADVICE_KEY) of
                [{?LB_ADVICE_KEY, undefined}] ->
                    {error, no_lb_advice_received};
                _ ->
                    {ok, Threshold} = application:get_env(?APP_NAME, dns_disp_out_of_sync_threshold),
                    % Threshold is in millisecs, now_diff is in microsecs
                    case utils:milliseconds_diff(now(), LastUpdate) > Threshold of
                        true -> out_of_sync;
                        false -> ok
                    end
            end,
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.

handle_cast({update_lb_advice, LBAdvice}, State) ->
    ?debug("Dispatcher update of load_balancing advice: ~p", [LBAdvice]),
    % Update LB advice
    ets:insert(?WORKER_MAP_ETS, {?LB_ADVICE_KEY, LBAdvice}),
    {noreply, State#state{last_update = now()}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_info({timer, Msg}, State) ->
    gen_server:cast(?DISPATCHER_NAME, Msg),
    {noreply, State};
handle_info(_Request, State) ->
    ?log_bad_request(_Request),
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
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
terminate(_Reason, _State) ->
    ets:delete(?WORKER_MAP_ETS),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Request routing API
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses to which worker (on which node) the request should be sent.
%%
%% NOTE: currently, all nodes host all workers, so worker type can be omitted.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node(WorkerName :: worker_name()) -> {ok, node()} | {error, dispatcher_out_of_sync}.
get_worker_node(WorkerName) ->
    case ets:lookup(?WORKER_MAP_ETS, ?LB_ADVICE_KEY) of
        [{?LB_ADVICE_KEY, undefined}] ->
            {error, dispatcher_out_of_sync};
        [{?LB_ADVICE_KEY, LBAdvice}] ->
            Node = load_balancing:choose_node_for_dispatcher(LBAdvice, WorkerName),
            {ok, Node}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns all workers that host given worker.
%%
%% NOTE: currently, all nodes host all workers, so worker type can be omitted.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_nodes(WorkerName :: worker_name()) -> {ok, [node()]} | {error, dispatcher_out_of_sync}.
get_worker_nodes(_WorkerName) ->
    case ets:lookup(?WORKER_MAP_ETS, ?LB_ADVICE_KEY) of
        [{?LB_ADVICE_KEY, undefined}] ->
            {error, dispatcher_out_of_sync};
        [{?LB_ADVICE_KEY, LBAdvice}] ->
            Nodes = load_balancing:all_nodes_for_dispatcher(LBAdvice),
            {ok, Nodes}
    end.

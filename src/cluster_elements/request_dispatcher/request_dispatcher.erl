%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
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

-behaviour(gen_server).

-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("cluster_elements/request_dispatcher/request_dispatcher_state.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
    catch gsi_handler:init(),   %% Failed initialization of GSI should not disturb dispacher's startup
    worker_map:init(),
    {ok, #dispatcher_state{}}.

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
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
handle_call(get_state_num, _From, State) ->
    {reply, State#dispatcher_state.state_num, State};

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
-notify_state_change(dispatcher).
handle_cast({check_state, NewStateNum}, State) ->
    NewState = check_state(State, NewStateNum),
    {noreply, NewState};

handle_cast({update_state, WorkersList, NewStateNum}, State) ->
    ?info("Dispatcher state updated, state num: ~p", [NewStateNum]),
    worker_map:update_workers(WorkersList),
    {noreply, State#dispatcher_state{state_num = NewStateNum}};

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
    worker_map:terminate(),
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
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether dispatcher state is up to date, if not - fetches
%% worker list from ccm and updates it.
%% @end
%%--------------------------------------------------------------------
-spec check_state(State :: #dispatcher_state{}, NewStateNum :: integer()) -> #dispatcher_state{}.
check_state(State = #dispatcher_state{state_num = StateNum}, NewStateNum) when StateNum >= NewStateNum ->
    gen_server:cast(?NODE_MANAGER_NAME, {dispatcher_up_to_date, NewStateNum}),
    State;
check_state(State, _) ->
    ?info("Dispatcher had old state number, starting update"),
    try gen_server:call({global, ?CCM}, get_workers) of
        {WorkersList, StateNum} ->
            worker_map:update_workers(WorkersList),
            gen_server:cast(?NODE_MANAGER_NAME, {dispatcher_up_to_date, StateNum}),
            ?info("Dispatcher state updated, state num: ~p", [StateNum]),
            State#dispatcher_state{state_num = StateNum}
    catch
        _:Error ->
        ?error("Dispatcher had old state number but could not update data, error: ~p",[Error]),
        State
    end.

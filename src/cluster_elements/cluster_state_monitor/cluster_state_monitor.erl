%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Gen server which handles notifications of cluster state changes
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_state_monitor).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, cast/1, call/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

%% gen_server state
-record(state, {node_state_numbers = [], ccm_state_number = 0, ccm_nodes_connected = [], init_subscribers = []}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv gen_server:cast({global, ?SERVER}, Req).
%% @end
%%--------------------------------------------------------------------
-spec cast(Req :: term()) -> ok.
cast(Req) ->
    gen_server:cast({global, ?SERVER}, Req).

%%--------------------------------------------------------------------
%% @doc
%% @equiv gen_server:call({global, ?SERVER}, Req).
%% @end
%%--------------------------------------------------------------------
-spec call(Req :: term()) -> ok.
call(Req) ->
    gen_server:call({global, ?SERVER}, Req).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast({subscribe_for_init, AnswerPid, HowManyWorkerNodes}, State = #state{init_subscribers = Subscribers}) ->
    NewState = try_notify_subscribers(State#state{init_subscribers = [{AnswerPid, HowManyWorkerNodes} | Subscribers]}),
    {noreply, NewState};
handle_cast({ccm_state_updated, Nodes, NewStateNumber}, State) ->
    NewState = State#state{ccm_nodes_connected = Nodes, ccm_state_number = NewStateNumber},
    NewState2 = try_notify_subscribers(NewState),
    {noreply, NewState2};
handle_cast({dispatcher_state_updated, Node, NewStateNumber}, State = #state{node_state_numbers = StateNumbers}) ->
    NewState = State#state{node_state_numbers = [{Node, NewStateNumber} | proplists:delete(Node, StateNumbers)]},
    NewState2 = try_notify_subscribers(NewState),
    {noreply, NewState2};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
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
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Notifies all subscribers that are satisfied with current state of
%% cluster (number of synchronized worker nodes)
%% @end
%%--------------------------------------------------------------------
-spec try_notify_subscribers(State :: #state{}) -> #state{}.
try_notify_subscribers(State = #state{init_subscribers = []}) ->
    State;
try_notify_subscribers(State = #state{node_state_numbers = StateNumbers, ccm_nodes_connected = AllNodes, ccm_state_number = CcmStateNum, init_subscribers = [{AnsPid, HowManyNodes} | Rest]}) ->
    case CcmStateNum > 1
        andalso HowManyNodes < length(AllNodes)
        andalso length(AllNodes) == length(StateNumbers)
        andalso lists:all(fun({_, StateNum}) -> StateNum =:= CcmStateNum end, StateNumbers)
    of
        true ->
            ?info("Cluster init successfull, state synchronized, notifying ~p", [AnsPid]),
            AnsPid ! init_finished,
            try_notify_subscribers(State#state{init_subscribers = Rest});
        false ->
            NewState = #state{init_subscribers = NewInitSubscribers} = try_notify_subscribers(State#state{init_subscribers = Rest}),
            NewState#state{init_subscribers = [{AnsPid, HowManyNodes} | NewInitSubscribers]}
    end.

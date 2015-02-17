%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This gen_server provides following functionalities:
%%% - loading TCP server mocks from description module
%%% - starting and stopping ranch listeners
%%% - in-memory persistence for state such as history of received packets.
%%% @end
%%%-------------------------------------------------------------------
-module(tcp_mock_server).
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("appmock_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, healthcheck/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
% Number of acceptors in cowboy listeners
-define(NUMBER_OF_ACCEPTORS, 10).

% Internal state of the gen server
-record(state, {
    listeners = [] :: [term()],
    request_history = [] :: [{Port :: integer(), Path :: binary()}]
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Should check if this gen_server and all underlying services (like cowboy listeners)
%% are ready and working properly. If any error occurs, it should be logged inside this function.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | error.
healthcheck() ->
    {ok, Timeout} = application:get_env(?APP_NAME, nagios_healthcheck_timeout),
    gen_server:call(?SERVER, healthcheck, Timeout).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the appmock application by creating an ETS table, initializing records in it,
%% loading given mock app description module and starting cowboy listenera.
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, AppDescriptionFile} = application:get_env(?APP_NAME, app_description_file),
    DescriptionModule = appmock_utils:load_description_module(AppDescriptionFile),
    ListenersIDs = start_listeners(DescriptionModule),
    {ok, #state{listeners = [ListenersIDs]}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(healthcheck, _From, #state{} = State) ->
    Reply = ok,
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% Cleans up by stopping previously started cowboy listeners and deleting the ETS table.
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, #state{listeners = Listeners}) ->
    % Stop all previously started ranch listeners
    lists:foreach(
        fun(Listener) ->
            ?info("Stopping ranch listener: ~p", [Listener]),
            ranch:stop_listener(Listener)
        end, Listeners),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts all TCP servers that were specified in the app desription module.
%% @end
%%--------------------------------------------------------------------
-spec start_listeners(AppDescriptionModule) -> ListenerIDs :: [term()].
start_listeners(AppDescriptionModule) ->
    TCPServerMocks = AppDescriptionModule:tcp_server_mocks(),
    _ListenerIDs = lists:map(
        fun(#tcp_server_mock{port = Port, ssl = UseSSL}) ->
            % Generate listener name
            ListenerID = "tcp" ++ integer_to_list(Port),

            ListenerID
        end, TCPServerMocks).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts a single cowboy https listener.
%% @end
%%--------------------------------------------------------------------
-spec start_listener(ListenerID :: term(), Port :: integer(), Dispatch :: term()) -> ok.
start_listener(ListenerID, Port, Dispatch) ->
    % Load certificates' paths from env
    {ok, CaCertFile} = application:get_env(?APP_NAME, ca_cert_file),
    {ok, CertFile} = application:get_env(?APP_NAME, cert_file),
    {ok, KeyFile} = application:get_env(?APP_NAME, key_file),
    % Start a https listener on given port
    ?info("Starting cowboy listener: ~p (~p)", [ListenerID, Port]),
    {ok, _} = cowboy:start_https(
        ListenerID,
        ?NUMBER_OF_ACCEPTORS,
        [
            {port, Port},
            {cacertfile, CaCertFile},
            {certfile, CertFile},
            {keyfile, KeyFile}
        ],
        [
            {env, [{dispatch, Dispatch}]}
        ]),
    ok.

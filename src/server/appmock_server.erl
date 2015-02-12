%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This gen_server provides following functionalities:
%%% - loading description module
%%% - starting and stopping cowboy listeners
%%% - in-memory persistence for appmock state such as the mappings, the state of endpoints or history of calls.
%%% @end
%%%-------------------------------------------------------------------
-module(appmock_server).
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("appmock_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).
-export([produce_mock_resp/2, verify_mock/1, verify_all_mocks/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
% Identifier of cowboy listener that handles all remote control requests.
-define(REMOTE_CONTROL_LISTENER, remote_control).
% Number of acceptors in cowboy listeners
-define(NUMBER_OF_ACCEPTORS, 10).

% Internal state of the gen server
-record(state, {
    listeners = [] :: [term()],
    mock_resp_history = [] :: [{Port :: integer(), Path :: binary()}],
    mapping_states = dict:dict() :: dict()
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
%% Handles a request on a mocked endpoint. The endpoint is uniquely recognized by a pair {Port, Path}.
%% If the endpoint had a responding function defined, the state is set to the state returned by the function.
%% @end
%%--------------------------------------------------------------------
-spec produce_mock_resp(Req :: cowboy_req:req(), ETSKey :: {Port :: integer(), Path :: binary()}) ->
    {ok, {Code :: integer(), Headers :: [{term(), term()}], Body :: binary()}}.
produce_mock_resp(Req, ETSKey) ->
    gen_server:call(?SERVER, {produce_mock_resp, Req, ETSKey}).


%%--------------------------------------------------------------------
%% @doc
%% Handles requests to verify if certain endpoint had been requested given amount of times.
%% Input args are in request body JSON.
%% Returned body is encoded to JSON.
%% @end
%%--------------------------------------------------------------------
-spec verify_mock(Req :: cowboy_req:req()) ->
    {ok, {Code :: integer(), Headers :: [{term(), term()}], Body :: binary()}}.
verify_mock(Req) ->
    gen_server:call(?SERVER, {verify_mock, Req}).


%%--------------------------------------------------------------------
%% @doc
%% Handles requests to verify if all endpoints have been requested in expected order.
%% Input args are in request body JSON.
%% Returned body is encoded to JSON.
%% @end
%%--------------------------------------------------------------------
-spec verify_all_mocks(Req :: cowboy_req:req()) ->
    {ok, {Code :: integer(), Headers :: [{term(), term()}], Body :: binary()}}.
verify_all_mocks(Req) ->
    gen_server:call(?SERVER, {verify_all_mocks, Req}).


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
    DescriptionModule = load_description_module(AppDescriptionFile),
    Mappings = get_mappings(DescriptionModule),
    StatesDict = convert_mappings_to_states_dict(Mappings),
    MappingsListenersIDs = start_listeners_for_mappings(Mappings),
    RCListenerID = start_remote_control_listener(),
    {ok, #state{mapping_states = StatesDict, listeners = [RCListenerID | MappingsListenersIDs]}}.

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
handle_call(get_history, _From, State) ->
    {reply, {ok, State#state.mock_resp_history}, State};

handle_call({append_to_history, Mapping}, _From, #state{mock_resp_history = History} = State) ->
    {reply, ok, State#state{mock_resp_history = History ++ [Mapping]}};

handle_call({produce_mock_resp, Req, ETSKey}, _From, State) ->
    {{Code, Headers, Body}, NewState} = internal_produce_mock_resp(Req, ETSKey, State),
    {reply, {ok, {Code, Headers, Body}}, NewState};

handle_call({verify_mock, Req}, _From, State) ->
    {{Code, Headers, Body}, NewState} = internal_verify_mock(Req, State),
    {reply, {ok, {Code, Headers, Body}}, NewState};

handle_call({verify_all_mocks, Req}, _From, State) ->
    {{Code, Headers, Body}, NewState} = internal_verify_all_mocks(Req, State),
    {reply, {ok, {Code, Headers, Body}}, NewState};

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
    % Stop all previously started cowboy listeners
    lists:foreach(
        fun(Listener) ->
            ?info("Stopping cowboy listener: ~p", [Listener]),
            cowboy:stop_listener(Listener)
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
%% Compiles and loads a given file.
%% @end
%%--------------------------------------------------------------------
-spec load_description_module(FilePath :: string()) -> module() | no_return().
load_description_module(FilePath) ->
    try
        FileName = filename:basename(FilePath),
        {ok, ModuleName} = compile:file(FilePath),
        {ok, Bin} = file:read_file(filename:rootname(FileName) ++ ".beam"),
        erlang:load_module(ModuleName, Bin),
        ModuleName
    catch T:M ->
        throw({invalid_app_description_module, {type, T}, {message, M}, {stacktrace, erlang:get_stacktrace()}})
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Analyses the list of mappings returned by response_mocks() of the description module
%% and produces a map {Port, Path} -> #mapping_state for every port.
%% @end
%%--------------------------------------------------------------------
-spec get_mappings(ModuleName :: atom()) ->
    [{Port :: integer(), [{{Port :: integer(), Path :: integer()}, #mapping_state{}}]}].
get_mappings(ModuleName) ->
    % Get all mappings by calling request_mappings/0 function
    Mappings = ModuleName:response_mocks(),
    lists:foldl(
        fun(#mock_resp_mapping{port = Port, path = Path, response = Resp, initial_state = InitState}, PortsProplist) ->
            EndpointsForPort = proplists:get_value(Port, PortsProplist, []),
            NewEndpoints = [{{Port, Path}, #mapping_state{response = Resp, state = InitState}} | EndpointsForPort],
            % Remember the mapping in the proplist
            [{Port, NewEndpoints} | proplists:delete(Port, PortsProplist)]
        end, [], Mappings).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decides on which port should the application listen based on mappings
%% in description module and starts the listeners.
%% @end
%%--------------------------------------------------------------------
-spec convert_mappings_to_states_dict(Mappings :: [{Port :: integer(), [{{Port :: integer(), Path :: integer()}, #mapping_state{}}]}]) -> map().
convert_mappings_to_states_dict(Mappings) ->
    Dict = dict:new(),
    lists:foldl(
        fun({_, EndpointsForPort}, CurrentDict) ->
            lists:foldl(
                fun({{Port, Path}, #mapping_state{} = MappingState}, CurrentDictInside) ->
                    dict:append({Port, Path}, MappingState, CurrentDictInside)
                end, CurrentDict, EndpointsForPort)
        end, Dict, Mappings).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decides on which port should the application listen based on mappings
%% in description module and starts the listeners.
%% @end
%%--------------------------------------------------------------------
-spec start_listeners_for_mappings(Mappings :: [{Port :: integer(), [{{Port :: integer(), Path :: integer()}, #mapping_state{}}]}]) ->
    ListenerIDs :: [term()].
start_listeners_for_mappings(Mappings) ->
    % Create pairs {Port, CowboyDispatch} for every port, where CowboyDispatch
    % includes all enpoints for a certain port
    _ListenerIDs = lists:map(
        fun({Port, EndpointsForPort}) ->
            Dispatch = cowboy_router:compile([
                {'_', lists:map(
                    fun({{MPort, MPath}, _}) ->
                        {binary_to_list(MPath), mock_resp_handler, [{MPort, MPath}]}
                    end, EndpointsForPort)
                }
            ]),
            % Generate listener name
            ListenerID = "https" ++ integer_to_list(Port),
            start_listener(ListenerID, Port, Dispatch),
            ListenerID
        end, Mappings).


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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts a cowboy listener that handles all remote control requests.
%% @end
%%--------------------------------------------------------------------
-spec start_remote_control_listener() -> ListenerID :: term().
start_remote_control_listener() ->
    {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
    Dispatch = cowboy_router:compile([
        {'_', [
            {?VERIFY_ALL_PATH, remote_control_handler, [?VERIFY_ALL_PATH]},
            {?VERIFY_MOCK_PATH, remote_control_handler, [?VERIFY_MOCK_PATH]}
        ]}
    ]),
    start_listener(?REMOTE_CONTROL_LISTENER, RemoteControlPort, Dispatch),
    ?REMOTE_CONTROL_LISTENER.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal function called from handle_call, creating a response from mocked endpoint.
%% @end
%%--------------------------------------------------------------------
-spec internal_produce_mock_resp(Req :: cowboy_req:req(), ETSKey :: {Port :: integer(), Path :: binary()}, State :: #state{}) ->
    {{ok, {Code :: integer(), Headers :: [{term(), term()}], Body :: binary()}}, NewState :: #state{}}.
internal_produce_mock_resp(Req, ETSKey, #state{mock_resp_history = History, mapping_states = MappingStatesDict} = ServerState) ->
    % Get the response term and current state by {Port, Path} key
    {ok, [MappingStateRec]} = dict:find(ETSKey, MappingStatesDict),
    #mapping_state{response = ResponseField, state = MappingState, counter = Counter} = MappingStateRec,
    % Get response and new state - either directly, cyclically from a list or by evaluating a fun
    {Response, NewMappingState} =
        case ResponseField of
            #mock_resp{} ->
                {ResponseField, MappingState};
            RespList when is_list(RespList) ->
                CurrentResp = lists:nth((Counter rem length(RespList)) + 1, RespList),
                {CurrentResp, MappingState};
            Fun when is_function(Fun, 2) ->
                {_Response, _NewState} = Fun(Req, MappingState)
        end,
    % Put new state in the dictionary
    NewMappingStatesDict = dict:update(ETSKey,
        fun(_) ->
            [MappingStateRec#mapping_state{state = NewMappingState, counter = Counter + 1}]
        end, MappingStatesDict),
    #mock_resp{code = Code, body = Body, content_type = CType, headers = Headers} = Response,
    AllHeaders = [{<<"content-type">>, CType}] ++ Headers,
    {Port, Path} = ETSKey,
    ?info("Got request at :~p~s~nResponding~n  Code:    ~p~n  Headers: ~p~n  Body:    ~s", [Port, Path, Code, AllHeaders, Body]),
    {{Code, AllHeaders, Body}, ServerState#state{mock_resp_history = History ++ [ETSKey], mapping_states = NewMappingStatesDict}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal function called from handle_call, veryfing if a mocked endpoint has been requested given number of times.
%% @end
%%--------------------------------------------------------------------
-spec internal_verify_mock(Req :: cowboy_req:req(), State :: #state{}) ->
    {{ok, {Code :: integer(), Headers :: [{term(), term()}], Body :: binary()}}, NewState :: #state{}}.
internal_verify_mock(Req, #state{mapping_states = MappingStatesDict} = State) ->
    {ok, JSONBody, _} = cowboy_req:body(Req),
    Body = appmock_utils:decode_from_json(JSONBody),
    {Port, Path, Number} = ?VERIFY_MOCK_UNPACK_REQUEST(Body),
    ReplyTerm = case dict:find({Port, Path}, MappingStatesDict) of
                    error ->
                        ?VERIFY_MOCK_PACK_ERROR_WRONG_ENDPOINT;
                    {ok, [#mapping_state{counter = ActualNumber}]} ->
                        case ActualNumber of
                            Number ->
                                ?OK_RESULT;
                            _ ->
                                ?VERIFY_MOCK_PACK_ERROR(ActualNumber)
                        end
                end,
    {{200, [{<<"content-type">>, <<"application/json">>}], appmock_utils:encode_to_json(ReplyTerm)}, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal function called from handle_call, veryfing if all mocked endpoint have been requested in given order.
%% @end
%%--------------------------------------------------------------------
-spec internal_verify_all_mocks(Req :: cowboy_req:req(), State :: #state{}) ->
    {{ok, {Code :: integer(), Headers :: [{term(), term()}], Body :: binary()}}, NewState :: #state{}}.
internal_verify_all_mocks(Req, #state{mock_resp_history = History} = State) ->
    {ok, JSONBody, _} = cowboy_req:body(Req),
    BodyStruct = appmock_utils:decode_from_json(JSONBody),
    Body = ?VERIFY_ALL_UNPACK_REQUEST(BodyStruct),
    ReplyTerm = case Body of
                    History ->
                        ?OK_RESULT;
                    _ ->
                        ?VERIFY_ALL_PACK_ERROR(History)
                end,
    {{200, [{<<"content-type">>, <<"application/json">>}], appmock_utils:encode_to_json(ReplyTerm)}, State}.

%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is the heart of appmock application. It's job is to:
%%% - initialize and cleanup after cowboy listeners
%%% - keep the application state in ETS
%%% - handle incoming requests
%%% @end
%%%-------------------------------------------------------------------
-module(appmock_logic).

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").


%% API
-export([initialize/1, terminate/0]).
-export([produce_mock_resp/2, verify_mock/1, verify_all_mocks/1]).

% ETS identifier
-define(MAPPINGS_ETS, mapping_ets).
% ETS key, under which list of all started listeners is remembered
-define(LISTENERS_KEY, listener_ids).
% ETS key, under which full history of incoming requests is remembered
-define(HISTORY_KEY, history).
% Identifier of cowboy listener that handles all remote control requests.
-define(REMOTE_CONTROL_LISTENER, remote_control).
% Number of acceptors in cowboy listeners
-define(NUMBER_OF_ACCEPTORS, 30).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the appmock application by creating an ETS table, inserting some records to it,
%% loading given mock app description module and starting cowboy listener.
%% @end
%%--------------------------------------------------------------------
-spec initialize(FilePath :: string()) -> ok | error.
initialize(FilePath) ->
    % Initialize an ETS table to store states and counters of certain stubs
    ets:new(?MAPPINGS_ETS, [set, named_table, public]),
    % Insert a tuple in ETS which will keep track of all started cowboy listeners
    ets:insert(?MAPPINGS_ETS, {?LISTENERS_KEY, []}),
    % Insert a tuple in ETS which will remember the history of requests
    ets:insert(?MAPPINGS_ETS, {?HISTORY_KEY, []}),
    DescriptionModule = load_description_module(FilePath),
    start_remote_control_listener(),
    start_listeners_for_mappings(DescriptionModule),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Cleans up by stopping previously started cowboy listeners and deleting the ETS table.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    % Stop all previously started cowboy listeners
    [{?LISTENERS_KEY, ListenersList}] = ets:lookup(?MAPPINGS_ETS, ?LISTENERS_KEY),
    lists:foreach(
        fun(Listener) ->
            ?info("Stopping cowboy listener: ~p", [Listener]),
            cowboy:stop_listener(Listener)
        end, ListenersList),
    ets:delete(?MAPPINGS_ETS),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request on a mocked endpoint. The endpoint is uniquely recognized by a pair {Port, Path}.
%% If the endpoint had a responding function defined, the state is set to the state returned by the function.
%% @end
%%--------------------------------------------------------------------
-spec produce_mock_resp(Req :: cowboy_req:req(), ETSKey :: {Port :: integer(), Path :: binary()}) -> {ok, cowboy_req:req()}.
produce_mock_resp(Req, ETSKey) ->
    % Append the request to history
    [{?HISTORY_KEY, History}] = ets:lookup(?MAPPINGS_ETS, ?HISTORY_KEY),
    ets:insert(?MAPPINGS_ETS, {?HISTORY_KEY, History ++ [ETSKey]}),
    % Get the response term and current state by {Port, Path} key
    [{ETSKey, MappingState}] = ets:lookup(?MAPPINGS_ETS, ETSKey),
    #mapping_state{response = ResponseField, state = State, counter = Counter} = MappingState,
    % Get response and new state - either directly, cyclically from a list or by evaluating a fun
    {Response, NewState} = case ResponseField of
                               #mock_resp{} ->
                                   {ResponseField, State};
                               RespList when is_list(RespList) ->
                                   CurrentResp = lists:nth((Counter rem length(RespList)) + 1, RespList),
                                   {CurrentResp, State};
                               Fun when is_function(Fun, 2) ->
                                   {_Response, _NewState} = Fun(Req, State)
                           end,
    % Put new state in the ETS
    ets:insert(?MAPPINGS_ETS, {ETSKey, MappingState#mapping_state{state = NewState, counter = Counter + 1}}),
    #mock_resp{code = Code, body = Body, content_type = CType, headers = Headers} = Response,
    AllHeaders = [{<<"content-type">>, CType}] ++ Headers,
    {Port, Path} = ETSKey,
    ?debug("Got request at :~p~s~nResponding~n  Code:    ~p~n  Headers: ~p~n  Body:   ~s", [Port, Path, Code, AllHeaders, Body]),
    % Respond
    Req2 = cowboy_req:set_resp_body(Body, Req),
    Req3 = lists:foldl(
        fun({HKey, HValue}, CurrReq) ->
            gui_utils:cowboy_ensure_header(HKey, HValue, CurrReq)
        end, Req2, AllHeaders),
    {ok, _NewReq} = cowboy_req:reply(Code, Req3).


%%--------------------------------------------------------------------
%% @doc
%% Handles requests to verify if certain endpoint had been requested given amount of times.
%% Input args are in request body JSON.
%% Returned body is encoded to JSON.
%% @end
%%--------------------------------------------------------------------
-spec verify_mock(Req :: cowboy_req:req()) -> {ok, term()}.
verify_mock(Req) ->
    {ok, JSONBody, _} = cowboy_req:body(Req),
    Body = appmock_utils:decode_from_json(JSONBody),
    {Port, Path, Number} = ?VERIFY_MOCK_UNPACK_REQUEST(Body),
    ReplyTerm = case ets:lookup(?MAPPINGS_ETS, {Port, Path}) of
                    [] ->
                        ?VERIFY_MOCK_PACK_ERROR_WRONG_ENDPOINT;
                    [{{Port, Path}, #mapping_state{counter = ActualNumber}}] ->
                        case ActualNumber of
                            Number ->
                                ?OK_RESULT;
                            _ ->
                                ?VERIFY_MOCK_PACK_ERROR(ActualNumber)
                        end
                end,
    {ok, _NewReq} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/json">>}],
        appmock_utils:encode_to_json(ReplyTerm), Req).


%%--------------------------------------------------------------------
%% @doc
%% Handles requests to verify if all endpoints have been requested in expected order.
%% Input args are in request body JSON.
%% Returned body is encoded to JSON.
%% @end
%%--------------------------------------------------------------------
-spec verify_all_mocks(Req :: cowboy_req:req()) -> {ok, term()}.
verify_all_mocks(Req) ->
    {ok, JSONBody, _} = cowboy_req:body(Req),
    BodyStruct = appmock_utils:decode_from_json(JSONBody),
    Body = ?VERIFY_ALL_UNPACK_REQUEST(BodyStruct),
    [{?HISTORY_KEY, History}] = ets:lookup(?MAPPINGS_ETS, ?HISTORY_KEY),
    Reply = case Body of
                History ->
                    appmock_utils:encode_to_json(?OK_RESULT);
                _ ->
                    appmock_utils:encode_to_json(?VERIFY_ALL_PACK_ERROR(History))
            end,
    {ok, _NewReq} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/json">>}], Reply, Req).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Compiles and loads a given file.
%% @end
%%--------------------------------------------------------------------
-spec load_description_module(FilePath :: binary()) -> module() | no_return().
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
%% @doc
%% Decides on which port should the application listen based on mappings
%% in description module and starts the listeners.
%% @end
%%--------------------------------------------------------------------
-spec start_listeners_for_mappings(ModuleName :: atom()) -> ok.
start_listeners_for_mappings(ModuleName) ->
    % Get all mappings by calling request_mappings/0 function
    Mappings = ModuleName:response_mocks(),
    % Partition mappings by the port
    PortList = lists:foldl(
        fun(#mock_resp_mapping{port = Port, path = Path, response = Response, initial_state = InitialState}, UniquePorts) ->
            % Remember a mapping in process dictionary
            add_endpoint(Port, Path, Response, InitialState),
            % Add the port to port list (uniquely)
            (UniquePorts -- [Port]) ++ [Port]
        end, [], Mappings),
    % Create pairs {Port, CowboyDispatch} for every port, where CowboyDispatch
    % includes all enpoints for a certain port
    Listeners = lists:map(
        fun(Port) ->
            Dispatch = cowboy_router:compile([
                {'_', lists:map(
                    fun({Path, Response, InitialState}) ->
                        ETSKey = {Port, Path},
                        ets:insert(?MAPPINGS_ETS,
                            {ETSKey, #mapping_state{response = Response, state = InitialState}}),
                        {binary_to_list(Path), mock_resp_handler, [ETSKey]}
                    end, get_endpoints(Port))
                }
            ]),
            {Port, Dispatch}
        end, PortList),
    lists:foreach(
        fun({Port, Dispatch}) ->
            % Generate listener name
            ListenerID = "https" ++ integer_to_list(Port),
            start_listener(ListenerID, Port, Dispatch)
        end, Listeners),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Starts a single cowboy https listener.
%% @end
%%--------------------------------------------------------------------
-spec start_listener(ListenerID :: term(), Port :: integer(), Dispatch :: term()) -> ok.
start_listener(ListenerID, Port, Dispatch) ->
    % Save started listener in ETS
    [{?LISTENERS_KEY, ListenersList}] = ets:lookup(?MAPPINGS_ETS, ?LISTENERS_KEY),
    ets:insert(?MAPPINGS_ETS, {?LISTENERS_KEY, ListenersList ++ [ListenerID]}),
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
%% @doc
%% Convenience function to save which enpoints should be hosted on which port.
%% @end
%%--------------------------------------------------------------------
-spec add_endpoint(Port :: integer(), Path :: binary(), Response :: term(), InitialState :: term()) -> term().
add_endpoint(Port, Path, Response, InitialState) ->
    CurrentList = case get(Port) of
                      undefined -> [];
                      Other -> Other
                  end,
    put(Port, CurrentList ++ [{Path, Response, InitialState}]).


%%--------------------------------------------------------------------
%% @doc
%% Convenience function to retrieve which enpoints should be hosted on which port.
%% @end
%%--------------------------------------------------------------------
-spec get_endpoints(Port :: integer()) -> [{Path :: binary(), Response :: term(), InitialState :: term()}].
get_endpoints(Port) ->
    get(Port).


%%--------------------------------------------------------------------
%% @doc
%% Starts a cowboy listener that handles all remote control requests.
%% @end
%%--------------------------------------------------------------------
-spec start_remote_control_listener() -> ok.
start_remote_control_listener() ->
    {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
    Dispatch = cowboy_router:compile([
        {'_', [
            {?VERIFY_ALL_PATH, remote_control_handler, [?VERIFY_ALL_PATH]},
            {?VERIFY_MOCK_PATH, remote_control_handler, [?VERIFY_MOCK_PATH]}
        ]}
    ]),
    start_listener(?REMOTE_CONTROL_LISTENER, RemoteControlPort, Dispatch).
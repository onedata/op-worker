-module(appmock_logic).

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% API
-export([]).

%% API
-export([initialize/1, terminate/0, produce_mock_resp/2, verify_mocks/1]).

-define(MAPPINGS_ETS, mapping_ets).
-define(LISTENERS_KEY, listener_ids).
-define(HISTORY_KEY, history).
-define(REMOTE_CONTROL_LISTENER, remote_control).


initialize(FilePath) ->
    % Initialize an ETS table to store states and counters of certain stubs
    ets:new(?MAPPINGS_ETS, [set, named_table, public]),
    % Insert a tuple in ETS which will keep track of all started cowboy listeners
    ets:insert(?MAPPINGS_ETS, {?LISTENERS_KEY, []}),
    % Insert a tuple in ETS which will remember the history of requests
    ets:insert(?MAPPINGS_ETS, {?HISTORY_KEY, []}),
    DescriptionModule = load_description_module(FilePath),
    start_remote_control_listener(),
    start_listeners_for_mappings(DescriptionModule).


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


produce_mock_resp(Req, ETSKey) ->
    % Append the request to history
    [{?HISTORY_KEY, History}] = ets:lookup(?MAPPINGS_ETS, ?HISTORY_KEY),
    ets:delete_object(?MAPPINGS_ETS, {?HISTORY_KEY, History}),
    ets:insert(?MAPPINGS_ETS, {?HISTORY_KEY, History ++ [ETSKey]}),
    ?dump(History ++ [ETSKey]),
    % Get the response term and current state by {Port, Path} key
    [{ETSKey, MappingState}] = ets:lookup(?MAPPINGS_ETS, ETSKey),
    ets:delete_object(?MAPPINGS_ETS, {ETSKey, MappingState}),
    #mapping_state{response = ResponseField, state = State} = MappingState,
    % Get response and new state - either directly or by evaluating a fun
    {Response, NewState} = case ResponseField of
                               #mock_resp{} ->
                                   {ResponseField, State};
                               Fun when is_function(Fun, 2) ->
                                   {_Response, _NewState} = Fun(Req, State)
                           end,
    % Put new state in the ETS
    ets:insert(?MAPPINGS_ETS, {ETSKey, MappingState#mapping_state{state = NewState}}),
    #mock_resp{code = Code, body = Body, content_type = CType, headers = Headers} = Response,
    % Respond
    {ok, _NewReq} = cowboy_req:reply(Code, [{<<"content-type">>, CType}] ++ Headers, Body, Req).


verify_mocks(Req) ->
    {ok, Body, _} = cowboy_req:body(Req),
    ?dump(Body),
    {ok, _NewReq} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/json">>}], <<"ok">>, Req).


load_description_module(FilePath) ->
    FileName = filename:basename(FilePath),
    {ok, ModuleName} = compile:file(FilePath),
    {ok, Bin} = file:read_file(filename:rootname(FileName) ++ ".beam"),
    erlang:load_module(ModuleName, Bin),
    ModuleName.


start_listeners_for_mappings(ModuleName) ->
    % Get all mappings by calling request_mappings/0 function
    Mappings = ModuleName:response_mocks(),
    % Partition mappings by the port
    PortList = lists:foldl(
        fun(#mock_resp_mapping{port = Port, path = Path, response = Response, initial_state = InitialState}, UniquePorts) ->
            % Remember a mapping in process dictionary
            add_mapping(Port, Path, Response, InitialState),
            % Add the port to port list (uniquely)
            (UniquePorts -- [Port]) ++ [Port]
        end, [], Mappings),
    % Create pairs {Port, CowboyDispatch} for every port, where CowboyDispatch
    % includes all mappings for a certain port
    Listeners = lists:map(
        fun(Port) ->
            Dispatch = cowboy_router:compile([
                {'_', lists:map(
                    fun({Path, Response, InitialState}) ->
                        ETSKey = {Port, Path},
                        ets:insert(?MAPPINGS_ETS,
                            {ETSKey, #mapping_state{response = Response, state = InitialState}}),
                        {Path, mock_resp_handler, [ETSKey]}
                    end, get_mappings(Port))
                }
            ]),
            {Port, Dispatch}
        end, PortList),
    lists:foreach(
        fun({Port, Dispatch}) ->
            % Generate listener name
            ListenerID = "https" ++ integer_to_list(Port),
            start_listener(ListenerID, Port, Dispatch)
        end, Listeners).


start_listener(ListenerID, Port, Dispatch) ->
    % Save started listener in ETS
    [{?LISTENERS_KEY, ListenersList}] = ets:lookup(?MAPPINGS_ETS, ?LISTENERS_KEY),
    ets:delete_object(?MAPPINGS_ETS, {?LISTENERS_KEY, ListenersList}),
    ets:insert(?MAPPINGS_ETS, {?LISTENERS_KEY, ListenersList ++ [ListenerID]}),
    % Load certificates' paths from env
    {ok, CaCertFile} = application:get_env(?APP_NAME, ca_cert_file),
    {ok, CertFile} = application:get_env(?APP_NAME, cert_file),
    {ok, KeyFile} = application:get_env(?APP_NAME, key_file),
    % Start a https listener on given port
    ?info("Starting cowboy listener: ~p (~p)", [ListenerID, Port]),
    {ok, _} = cowboy:start_https(
        ListenerID,
        100,
        [
            {port, Port},
            {cacertfile, CaCertFile},
            {certfile, CertFile},
            {keyfile, KeyFile}
        ],
        [
            {env, [{dispatch, Dispatch}]}
        ]).


add_mapping(Port, Path, Response, InitialState) ->
    CurrentList = case get(Port) of
                      undefined -> [];
                      Other -> Other
                  end,
    put(Port, CurrentList ++ [{Path, Response, InitialState}]).


get_mappings(Port) ->
    get(Port).


start_remote_control_listener() ->
    {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
    Dispatch = cowboy_router:compile([
        {'_', [
            {?REMOTE_CONTROL_VERIFY_PATH, remote_control_handler, [verify]}
        ]}
    ]),
    start_listener(?REMOTE_CONTROL_LISTENER, RemoteControlPort, Dispatch).
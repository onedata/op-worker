-module(appmock_logic).

-include_lib("ctool/include/logging.hrl").
-include("appmock.hrl").

%% API
-export([]).

%% API
-export([initialize/1, terminate/0, handle_request/2]).

-define(MAPPINGS_ETS, mapping_ets).
-define(LISTENERS_KEY, listener_ids).


initialize(FilePath) ->
    % Initialize an ETS table to store states and counters of certain stubs
    ets:new(?MAPPINGS_ETS, [set, named_table, public]),
    % Compile and load given file
    FileName = filename:basename(FilePath),
    {ok, ModuleName} = compile:file(FilePath),
    {ok, Bin} = file:read_file(filename:rootname(FileName) ++ ".beam"),
    erlang:load_module(ModuleName, Bin),
    % Get all mappings by calling request_mappings/0 function
    Mappings = ModuleName:request_mappings(),
    % Partition mappings by the port
    PortList = lists:foldl(
        fun(#mapping{port = Port, path = Path, response = Response, initial_state = InitialState}, UniquePorts) ->
            % remember a mapping in process dictionary
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
                        {Path, request_handler, [ETSKey]}
                    end, get_mappings(Port))
                }
            ]),
            {Port, Dispatch}
        end, PortList),
    % Load certificates' paths from env
    {ok, CaCertFile} = application:get_env(?APP_NAME, ca_cert_file),
    {ok, CertFile} = application:get_env(?APP_NAME, cert_file),
    {ok, KeyFile} = application:get_env(?APP_NAME, key_file),
    % Insert a tuple in ETS which will keep track of all started cowboy listeners
    ets:insert(?MAPPINGS_ETS, {?LISTENERS_KEY, []}),
    lists:foreach(
        fun({Port, Dispatch}) ->
            % Generate listener name and concatenate it to the list in ETS
            ListenerID = "https" ++ integer_to_list(Port),
            [{?LISTENERS_KEY, ListenersList}] = ets:lookup(?MAPPINGS_ETS, ?LISTENERS_KEY),
            ets:delete_object(?MAPPINGS_ETS, {?LISTENERS_KEY, ListenersList}),
            ets:insert(?MAPPINGS_ETS, {?LISTENERS_KEY, ListenersList ++ [ListenerID]}),
            % Start a https lsitener on given port
            ?info("Starting cowboy listener: ~p (~p)", [ListenerID, Port]),
            {ok, _} = cowboy:start_https(
                list_to_atom(ListenerID),
                100,
                [
                    {port, Port},
                    {cacertfile, CaCertFile},
                    {certfile, CertFile},
                    {keyfile, KeyFile}
                ],
                [
                    {env, [{dispatch, Dispatch}]}
                ])
        end, Listeners).


add_mapping(Port, Path, Response, InitialState) ->
    CurrentList = case get(Port) of
                      undefined -> [];
                      Other -> Other
                  end,
    put(Port, CurrentList ++ [{Path, Response, InitialState}]).


get_mappings(Port) ->
    get(Port).


terminate() ->
    [{?LISTENERS_KEY, ListenersList}] = ets:lookup(?MAPPINGS_ETS, ?LISTENERS_KEY),
    lists:foreach(
        fun(Listener) ->
            ?info("Stopping cowboy listener: ~p", [Listener]),
            cowboy:stop_listener(Listener)
        end, ListenersList),
    ets:delete(?MAPPINGS_ETS),
    ok.


handle_request(Req, ETSKey) ->
    [{ETSKey, MappingState}] = ets:lookup(?MAPPINGS_ETS, ETSKey),
    ets:delete_object(?MAPPINGS_ETS, {ETSKey, MappingState}),
    #mapping_state{response = ResponseField, state = State} = MappingState,
    {Response, NewState} = case ResponseField of
                               #response{} ->
                                   {ResponseField, State};
                               Fun when is_function(Fun, 2) ->
                                   {_Response, _NewState} = Fun(Req, State)
                           end,
    ets:insert(?MAPPINGS_ETS, {ETSKey, MappingState#mapping_state{state = NewState}}),
    #response{code = Code, body = Body, content_type = CType, headers = Headers} = Response,
    {ok, _NewReq} = cowboy_req:reply(Code,
        [{<<"content-type">>, CType}] ++ Headers, Body, Req).
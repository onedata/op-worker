%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a cowboy websocket handler that handles the connection
%%% between Ember ws_adapter and server. This channel is used for models
%%% synchronization and performing RPC to the server.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_ws_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_websocket).

-include("http/op_gui.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).
-export([terminate/3]).

-ifdef(TEST).
-compile(export_all).
-endif.


%% Interface between WebSocket Adapter client and server. Corresponding
%% interface is located in ws_adapter.js.

%% All in-coming JSONs have the following structure (opt = optional field)
%% {
%%   uuid
%%   msgType
%%   resourceType
%%   operation
%%   resourceIds (opt)
%%   data (opt)
%% }
%% All out-coming JSONs have the following structure (opt = optional field)
%% {
%%   uuid (opt, not used in push messages)
%%   msgType
%%   result
%%   data (opt)
%% }

%% Keys corresponding to above structure
-define(KEY_UUID, <<"uuid">>).
-define(KEY_MSG_TYPE, <<"msgType">>).
-define(KEY_RESOURCE_TYPE, <<"resourceType">>).
-define(KEY_OPERATION, <<"operation">>).
-define(KEY_RESOURCE_IDS, <<"resourceIds">>).
-define(KEY_DATA, <<"data">>).
-define(KEY_RESULT, <<"result">>).
%% Message types, identified by ?KEY_MSG_TYPE key
-define(TYPE_MODEL_REQ, <<"modelReq">>).
-define(TYPE_MODEL_RESP, <<"modelResp">>).
-define(TYPE_MODEL_CRT_PUSH, <<"modelPushCreated">>).
-define(TYPE_MODEL_UPT_PUSH, <<"modelPushUpdated">>).
-define(TYPE_MODEL_DLT_PUSH, <<"modelPushDeleted">>).
-define(TYPE_RPC_REQ, <<"RPCReq">>).
-define(TYPE_RPC_RESP, <<"RPCResp">>).
-define(TYPE_PUSH_MESSAGE, <<"pushMessage">>).
%% Operations on model, identified by ?KEY_OPERATION key
-define(OP_FIND_RECORD, <<"findRecord">>).
-define(OP_FIND_MANY, <<"findMany">>).
-define(OP_FIND_ALL, <<"findAll">>).
-define(OP_QUERY, <<"query">>).
-define(OP_QUERY_RECORD, <<"queryRecord">>).
-define(OP_CREATE_RECORD, <<"createRecord">>).
-define(OP_UPDATE_RECORD, <<"updateRecord">>).
-define(OP_DELETE_RECORD, <<"deleteRecord">>).
%% Defined concerning session RPC
-define(RESOURCE_TYPE_PUBLIC_RPC, <<"public">>).
-define(RESOURCE_TYPE_PRIVATE_RPC, <<"private">>).
-define(RESOURCE_TYPE_SESSION, <<"session">>).
-define(KEY_SESSION_VALID, <<"sessionValid">>).
-define(KEY_SESSION_DETAILS, <<"sessionDetails">>).
%% Operation results
-define(RESULT_OK, <<"ok">>).
-define(RESULT_ERROR, <<"error">>).

-define(DATA_INTERNAL_SERVER_ERROR, <<"Internal Server Error">>).

-define(KEEPALIVE_INTERVAL,
    application:get_env(?APP_NAME, gui_websocket_keepalive, timer:seconds(30))
).
-define(MAX_ASYNC_PROCESSES_PER_BATCH,
    application:get_env(?APP_NAME, gui_max_async_processes_per_batch, 10)
).

%%%===================================================================
%%% cowboy_websocket_handler API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Upgrades the protocol to WebSocket.
%% @end
%%--------------------------------------------------------------------
-spec init(Req :: cowboy_req:req(), Opts :: any()) ->
    {ok, cowboy_req:req(), any()} |
    {cowboy_websocket, cowboy_req:req(), {session:identity(), session:auth(), Host :: binary()}}.
init(Req, Opts) ->
    Host = cowboy_req:host(Req),
    case op_gui_session:authenticate(Req) of
        ?ERROR_UNAUTHORIZED ->
            {ok, cowboy_req:reply(401, #{<<"connection">> => <<"close">>}, Req), Opts};
        false ->
            {cowboy_websocket, Req, {?GUEST_IDENTITY, ?GUEST_AUTH, Host}};
        {ok, Identity, Auth} ->
            {cowboy_websocket, Req, {Identity, Auth, Host}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Init callback - sets the websocket state.
%% @end
%%--------------------------------------------------------------------
-spec websocket_init({session:identity(), session:auth(), Host :: binary()}) -> {ok, no_state}.
websocket_init({Identity, Auth, Host}) ->
    op_gui_session:initialize(Identity, Auth, Host),
    erlang:send_after(?KEEPALIVE_INTERVAL, self(), keepalive),
    {ok, no_state}.


%%--------------------------------------------------------------------
%% @doc
%% Handles the data received from the Websocket connection.
%% Performs unpacking of JSON, follows the data to handler and then encodes
%% its response to JSON and sends it back to the client.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle(InFrame, State) ->
    {ok, State} | {ok, State, hibernate} |
    {reply, OutFrame | [OutFrame], State} |
    {reply, OutFrame | [OutFrame], State, hibernate} |
    {stop, State} when
    InFrame :: {text | binary | ping | pong, binary()},
    State :: no_state,
    OutFrame :: cow_ws:frame().
websocket_handle({text, MsgJSON}, State) ->
    % Try to decode request
    DecodedMsg = try
        json_utils:decode_deprecated(MsgJSON)
    catch
        _:_ -> undefined
    end,
    case DecodedMsg of
        % Accept only batch requests
        [{<<"batch">>, Requests}] ->
            % Batch was decoded, try to process all the requests.
            % Request processing is asynchronous.
            case process_requests(Requests) of
                ok ->
                    {ok, State};
                {error_result, Data} ->
                    ResponseJSON = json_utils:encode_deprecated([{<<"batch">>, [Data]}]),
                    {reply, {text, ResponseJSON}, State}
            end;
        _ ->
            % Request could not be decoded, reply with an error
            {_, ErrorMsg} = op_gui_error:cannot_decode_message(),
            ResponseJSON = json_utils:encode_deprecated([{<<"batch">>, [ErrorMsg]}]),
            {reply, {text, ResponseJSON}, State}
    end;

websocket_handle(pong, State) ->
    {ok, State};

websocket_handle(Data, State) ->
    ?debug("Received unexpected data in GUI WS: ~p", [Data]),
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles any Erlang messages received.
%% Async processes can message the websocket process
%% (push_deleted, push_updated) to push data to the client.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(Info, State) ->
    {ok, State} | {ok, State, hibernate} |
    {reply, OutFrame | [OutFrame], State} |
    {reply, OutFrame | [OutFrame], State, hibernate} |
    {stop, State} when
    Info :: any(),
    State :: no_state,
    OutFrame :: cow_ws:frame().
% Sends any data to the client
websocket_info({send, Data}, State) ->
    Msg = [
        {<<"batch">>, [Data]}
    ],
    {reply, {text, json_utils:encode_deprecated(Msg)}, State};

% Sends a push message (server side event) to the client
websocket_info({push_message, Data}, State) ->
    Msg = [
        {<<"batch">>, [
            [
                {?KEY_MSG_TYPE, ?TYPE_PUSH_MESSAGE},
                {?KEY_DATA, Data}
            ]
        ]}
    ],
    {reply, {text, json_utils:encode_deprecated(Msg)}, State};

% Sends a push message informing about newly created item to the client
% Concerns only model level items
websocket_info({push_created, ResourceType, Data}, State) ->
    Msg = [
        {<<"batch">>, [
            [
                {?KEY_MSG_TYPE, ?TYPE_MODEL_CRT_PUSH},
                {?KEY_RESOURCE_TYPE, ResourceType},
                {?KEY_DATA, Data}
            ]
        ]}
    ],
    {reply, {text, json_utils:encode_deprecated(Msg)}, State};

% Sends a push message informing about updated item to the client
% Concerns only model level items
websocket_info({push_updated, ResourceType, Data}, State) ->
    Msg = [
        {<<"batch">>, [
            [
                {?KEY_MSG_TYPE, ?TYPE_MODEL_UPT_PUSH},
                {?KEY_RESOURCE_TYPE, ResourceType},
                {?KEY_DATA, Data}
            ]
        ]}
    ],
    {reply, {text, json_utils:encode_deprecated(Msg)}, State};

% Sends a push message informing about deleted item to the client
% Concerns only model level items
websocket_info({push_deleted, ResourceType, Ids}, State) ->
    Msg = [
        {<<"batch">>, [
            [
                {?KEY_MSG_TYPE, ?TYPE_MODEL_DLT_PUSH},
                {?KEY_RESOURCE_TYPE, ResourceType},
                {?KEY_DATA, Ids}
            ]
        ]}
    ],
    {reply, {text, json_utils:encode_deprecated(Msg)}, State};

websocket_info(keepalive, State) ->
    erlang:send_after(?KEEPALIVE_INTERVAL, self(), keepalive),
    {reply, ping, State};

websocket_info(_Info, State) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Performs any necessary cleanup of the state.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, Req, State) -> ok when
    Reason :: normal | stop | timeout |
    remote | {remote, cow_ws:close_code(), binary()} |
    {error, badencoding | badframe | closed | atom()} |
    {crash, error | exit | throw, any()},
    Req :: cowboy_req:req(),
    State :: any().
terminate(_Reason, _Req, _State) ->
    lists:foreach(
        fun(Handler) ->
            try
                Handler:terminate()
            catch Type:Message ->
                ?error_stacktrace("Error in ~p data_backend terminate - ~p:~p",
                    [Type, Message])
            end
        end, maps:values(get_data_backend_map())),
    op_gui_async:kill_async_processes(),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Handled a decoded websocket message. Based on the type of message and type
%% of requested resource, decides which handler module should be called.
%% @end
%%--------------------------------------------------------------------
-spec handle_decoded_message(Props :: proplists:proplist()) ->
    proplists:proplist().
handle_decoded_message(Props) ->
    MsgType = proplists:get_value(?KEY_MSG_TYPE, Props),
    MsgUUID = proplists:get_value(?KEY_UUID, Props, null),
    % Choose handling module depending on message type
    {Result, ReplyType} = case MsgType of
        ?TYPE_MODEL_REQ ->
            RsrcType = proplists:get_value(?KEY_RESOURCE_TYPE, Props),
            Handler = resolve_data_backend(RsrcType, false),
            Res = handle_model_req(Props, Handler),
            {Res, ?TYPE_MODEL_RESP};
        ?TYPE_RPC_REQ ->
            Res = handle_RPC_req(Props),
            {Res, ?TYPE_RPC_RESP}
    end,
    % Resolve returned values
    {RespResult, RespData} = case Result of
        ok ->
            {?RESULT_OK, null};
        {ok, Data} ->
            {?RESULT_OK, Data};
        {error_result, Data} ->
            {?RESULT_ERROR, Data}
    end,
    [
        {?KEY_MSG_TYPE, ReplyType},
        {?KEY_UUID, MsgUUID},
        {?KEY_RESULT, RespResult},
        {?KEY_DATA, RespData}
    ].


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Resolves data backend for given model synchronization request.
%% Data backends must be initialized on first call, so it uses a map to keep
%% track which backends are already initialized.
%% @end
%%--------------------------------------------------------------------
-spec resolve_data_backend(ResourceType :: binary(),
    RunInitialization :: boolean()) -> Handler :: atom().
resolve_data_backend(RsrcType, RunInitialization) ->
    HasSession = op_gui_session:is_logged_in(),

    Handler = try
        op_gui_routes:data_backend(HasSession, RsrcType)
    catch _:_ ->
        throw(unauthorized)
    end,

    % Initialized data backends are cached in process dictionary.
    case RunInitialization of
        false ->
            Handler;
        true ->
            DataBackendMap = get_data_backend_map(),
            case maps:get({RsrcType, HasSession}, DataBackendMap, undefined) of
                undefined ->
                    ok = Handler:init(),
                    set_data_backend_map(
                        DataBackendMap#{{RsrcType, HasSession} => Handler}
                    ),
                    Handler;
                InitializedHandler ->
                    InitializedHandler
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Handles message of type PULL REQUEST, which is a message requesting data
%% about certain model. Returns a proplist that is later encoded to JSON.
%% @end
%%--------------------------------------------------------------------
-spec handle_model_req(Props :: proplists:proplist(), Handler :: atom()) ->
    ok | {ok, Res :: proplists:proplist()} | op_gui_error:error_result().
handle_model_req(Props, Handler) ->
    RsrcType = proplists:get_value(?KEY_RESOURCE_TYPE, Props),
    Data = proplists:get_value(?KEY_DATA, Props),
    EntityIdOrIds = proplists:get_value(?KEY_RESOURCE_IDS, Props),
    % Catch crashes here so we can respond with an error corresponding
    % to given request rather that with generic error.
    try
        case proplists:get_value(?KEY_OPERATION, Props) of
            ?OP_FIND_RECORD ->
                erlang:apply(Handler, find_record, [RsrcType, EntityIdOrIds]);
            ?OP_FIND_MANY ->
                % Will return list of found entities only if all finds succeed.
                Res = lists:foldl(
                    fun(EntityId, Acc) ->
                        case Acc of
                            List when is_list(List) ->
                                case erlang:apply(Handler, find_record,
                                    [RsrcType, EntityId]) of
                                    {ok, Data} ->
                                        [Data | Acc];
                                    Error ->
                                        Error
                                end;
                            Error ->
                                Error
                        end
                    end, [], EntityIdOrIds),
                {ok, Res};
            ?OP_FIND_ALL ->
                erlang:apply(Handler, find_all, [RsrcType]);
            ?OP_QUERY ->
                erlang:apply(Handler, query, [RsrcType, Data]);
            ?OP_QUERY_RECORD ->
                erlang:apply(Handler, query_record, [RsrcType, Data]);
            ?OP_CREATE_RECORD ->
                erlang:apply(Handler, create_record, [RsrcType, Data]);
            ?OP_UPDATE_RECORD ->
                erlang:apply(Handler, update_record,
                    [RsrcType, EntityIdOrIds, Data]);
            ?OP_DELETE_RECORD ->
                erlang:apply(Handler, delete_record,
                    [RsrcType, EntityIdOrIds])
        end
    catch
        T:M ->
            % There was an error processing the request, reply with
            % an error.
            ?error_stacktrace("Error while handling GUI model request - ~p:~p",
                [T, M]),
            op_gui_error:internal_server_error()
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Handles message of type RPC_REQUEST, which is a message requesting that
%% the server performs some operation.
%% Returns a proplist that is later encoded to JSON.
%% @end
%%--------------------------------------------------------------------
-spec handle_RPC_req(Props :: proplists:proplist()) ->
    Res :: ok | {ok, proplists:proplist()} | op_gui_error:error_result().
handle_RPC_req(Props) ->
    ResourceType = proplists:get_value(?KEY_RESOURCE_TYPE, Props),
    Operation = proplists:get_value(?KEY_OPERATION, Props),
    Data = proplists:get_value(?KEY_DATA, Props),
    % Catch crashes here so we can respond with an error corresponding
    % to given request rather that with generic error.
    try
        case ResourceType of
            ?RESOURCE_TYPE_SESSION ->
                handle_session_RPC();
            ?RESOURCE_TYPE_PUBLIC_RPC ->
                public_rpc_backend:handle(Operation, Data);
            ?RESOURCE_TYPE_PRIVATE_RPC ->
                case op_gui_session:is_logged_in() of
                    true ->
                        private_rpc_backend:handle(Operation, Data);
                    false ->
                        op_gui_error:no_session()
                end
        end
    catch
        T:M ->
            % There was an error processing the request, reply with
            % an error.
            ?error_stacktrace("Error while handling GUI RPC request - ~p:~p",
                [T, M]),
            op_gui_error:internal_server_error()
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Handles an RPC call session details.
%% @end
%%--------------------------------------------------------------------
-spec handle_session_RPC() -> {ok, proplists:proplist()}.
handle_session_RPC() ->
    Data = case op_gui_session:is_logged_in() of
        true ->
            {ok, Props} = op_gui_routes:session_details(),
            [
                {?KEY_SESSION_VALID, true},
                {?KEY_SESSION_DETAILS, Props}
            ];
        false ->
            [
                {?KEY_SESSION_VALID, false}
            ]
    end,
    {ok, Data}.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Processes a batch of requests. A pool of processes is spawned and
%% the requests are split between them.
%% The requests are processed asynchronously and responses are sent gradually
%% to the client.
%% @end
%%--------------------------------------------------------------------
-spec process_requests(Requests :: [proplists:proplist()]) ->
    ok | op_gui_error:error_result().
process_requests(Requests) ->
    try
        % Initialize data backends (if needed)
        lists:foreach(
            fun(Request) ->
                MsgType = proplists:get_value(?KEY_MSG_TYPE, Request),
                ResourceType = proplists:get_value(?KEY_RESOURCE_TYPE, Request),
                case {MsgType, ResourceType} of
                    {?TYPE_MODEL_REQ, _} ->
                        resolve_data_backend(ResourceType, true);
                    _ ->
                        ok
                end
            end, Requests),
        % Split requests into batches and spawn parallel processes to handle them.
        Parts = split_into_sublists(Requests, ?MAX_ASYNC_PROCESSES_PER_BATCH),
        lists:foreach(
            fun(Part) ->
                op_gui_async:spawn(fun() -> process_requests_async(Part) end)
            end, Parts)
    catch
        throw:unauthorized ->
            op_gui_error:unauthorized();
        Type:Reason ->
            ?error_stacktrace("Cannot process websocket request - ~p:~p", [
                Type, Reason
            ]),
            op_gui_error:internal_server_error()
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Processes a batch of request. Responses are sent gradually to
%% the websocket process, which sends them to the client.
%% This should be done in an async process for scalability.
%% @end
%%--------------------------------------------------------------------
-spec process_requests_async(Requests :: [proplists:proplist()]) -> ok.
process_requests_async(Requests) ->
    lists:foreach(
        fun(Request) ->
            Result = try
                handle_decoded_message(Request)
            catch
                T:M ->
                    % There was an error processing the request, reply with
                    % an error.
                    ?error_stacktrace("Error while handling websocket message "
                    "- ~p:~p", [T, M]),
                    {_, ErrorMsg} = op_gui_error:internal_server_error(),
                    ErrorMsg
            end,
            op_gui_async:send(Result)
        end, Requests).


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Splits given list into a list of sublists with even length (+/- 1 element).
%% If the list length is smaller than the number of parts, it splits it into a
%% list of one element lists and the result list might be smaller than NumParts.
%% @end
%%--------------------------------------------------------------------
-spec split_into_sublists(List :: list(), NumberOfParts :: non_neg_integer()) ->
    [list()].
split_into_sublists(List, NumberOfParts) when length(List) =< NumberOfParts ->
    lists:map(fun(Element) -> [Element] end, List);
split_into_sublists(List, NumberOfParts) ->
    split_into_sublists(List, NumberOfParts, []).


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Splits given list into a list of sublists with even length (+/- 1 element).
%% @end
%%--------------------------------------------------------------------
-spec split_into_sublists(List :: list(), NumberOfParts :: non_neg_integer(),
    ResultList :: [list()]) -> [list()].
split_into_sublists(List, 1, ResultList) ->
    [List | ResultList];

split_into_sublists(List, NumberOfParts, ResultList) ->
    {Part, Tail} = lists:split(length(List) div NumberOfParts, List),
    split_into_sublists(Tail, NumberOfParts - 1, [Part | ResultList]).


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Retrieves data backend map from process dictionary, or empty map if
%% it is undefined.
%% @end
%%--------------------------------------------------------------------
-spec get_data_backend_map() -> map().
get_data_backend_map() ->
    case get(data_backend_map) of
        undefined -> #{};
        Map -> Map
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Saves data backend map in process dictionary.
%% @end
%%--------------------------------------------------------------------
-spec set_data_backend_map(map()) -> term().
set_data_backend_map(NewMap) ->
    put(data_backend_map, NewMap).

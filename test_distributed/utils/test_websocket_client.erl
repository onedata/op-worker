%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% A rudimentary WebSocket client that allows connecting and sending
%%% messages to a WebSocket server.
%%% @end
%%%-------------------------------------------------------------------
-module(test_websocket_client).
-author("Lukasz Opiola").

-export([connect_to_provider_node/4]).
-export([connect_to_url/4]).
-export([send/2]).

%%% websocket client API
-export([init/2, websocket_handle/3, websocket_info/3, websocket_terminate/3]).


-type client_ref() :: pid().
-type push_message_handler() :: fun((client_ref(), json_utils:json_term()) -> no_reply | {reply_text, json_utils:json_term()}).
-export_type([client_ref/0, push_message_handler/0]).

-record(state, {
    push_message_handler :: push_message_handler()
}).

-type state() :: no_state.

-define(HEARTBEAT_INTERVAL, timer:seconds(30)).


%%%===================================================================
%%% API
%%%===================================================================

-spec connect_to_provider_node(oct_background:node_selector(), binary(), http_client:headers(), push_message_handler()) ->
    {ok, client_ref()} | {error, term()}.
connect_to_provider_node(NodeSelector, Path, Headers, PushMessageHandler) ->
    Url = binary_to_list(opw_test_rpc:call(NodeSelector, oneprovider, build_url, [wss, Path])),
    TransportOpts = [{cacerts, opw_test_rpc:get_cert_chain_ders(NodeSelector)}],
    connect_to_url(Url, Headers, TransportOpts, PushMessageHandler).


-spec connect_to_url(binary(), http_client:headers(), proplists:proplist(), push_message_handler()) ->
    {ok, client_ref()} | {error, term()}.
connect_to_url(Url, Headers, TransportOpts, PushMessageHandler) ->
    case websocket_client:start_link(Url, Headers, ?MODULE, [PushMessageHandler], TransportOpts) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.


-spec send(client_ref(), binary()) -> ok.
send(ClientRef, Message) ->
    ClientRef ! {send, self(), Message},
    receive
        {send_confirmation, ClientRef} ->
            ok
    after 60000 ->
        error(timeout)
    end.

%%%===================================================================
%%% websocket client API
%%%===================================================================

-spec init([push_message_handler()], websocket_req:req()) -> {ok, state()}.
init([PushMessageHandler], _) ->
    erlang:send_after(?HEARTBEAT_INTERVAL, self(), do_heartbeat),
    {ok, #state{push_message_handler = PushMessageHandler}}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when data is received via WebSocket protocol.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle({text | binary | ping | pong, binary()},
    websocket_req:req(), state()) ->
    {ok, state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
websocket_handle({ping, <<"">>}, _, State) ->
    {ok, State};

websocket_handle({pong, <<"">>}, _, State) ->
    {ok, State};

websocket_handle({text, Payload}, _, State = #state{push_message_handler = PushMessageHandler}) ->
    try
        case PushMessageHandler(self(), Payload) of
            no_reply ->
                {ok, State};
            {reply_text, Reply} ->
                {reply, {text, Reply}, State}
        end
    catch
        Class:Reason:Stacktrace ->
            ct:print(
                "UNEXPECTED ERROR in ~w:~w - ~w:~tp~n"
                "Stacktrace: ~ts~n"
                "Payload: ~ts", [
                    ?MODULE, ?FUNCTION_NAME, Class, Reason,
                    lager:pr_stacktrace(Stacktrace),
                    Payload
                ]
            ),
            {ok, State}
    end;

websocket_handle(Message, _, State) ->
    ct:print("Unexpected message in ~w: ~ts", [?MODULE, Message]),
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(term(), websocket_req:req(), state()) ->
    {ok, state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
websocket_info(do_heartbeat, _, State) ->
    erlang:send_after(?HEARTBEAT_INTERVAL, self(), do_heartbeat),
    {reply, ping, State};

websocket_info({send, CallerPid, Data}, _, State) ->
    CallerPid ! {send_confirmation, self()},
    {reply, {text, Data}, State};

websocket_info(terminate, _, State) ->
    {close, <<"">>, State};

websocket_info(Msg, _, State) ->
    ct:print("Unexpected message in ~tp: ~tp", [?MODULE, Msg]),
    {ok, State}.


-spec websocket_terminate({Reason, term()} | {Reason, integer(), binary()},
    websocket_req:req(), state()) -> ok when
    Reason :: normal | error | remote.
websocket_terminate(_Reason, _ConnState, _State) ->
    ok.

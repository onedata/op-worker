%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests user authentication
%%% @end
%%%--------------------------------------------------------------------
-module(user_auth_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([token_authentication/1]).

-define(MACAROON, <<"macaroon">>).
-define(USER_ID, <<"test_id">>).
-define(USER_NAME, <<"test_name">>).

-performance({test_cases, []}).
all() -> [token_authentication].

%%%===================================================================
%%% Test functions
%%%===================================================================

token_authentication(Config) ->
    try
        % given
        [Worker1, _] = ?config(op_worker_nodes, Config),
        mock_gr_certificates(Config),
        SessionId = <<"SessionId">>,

        % when
        {ok, Sock} = connect_via_token(Worker1, ?MACAROON, SessionId),

        % then
        ?assertMatch(
            {ok, #document{value = #onedata_user{name = ?USER_NAME}}},
            rpc:call(Worker1, onedata_user, get, [?USER_ID])
        ),
        ?assertMatch(
            {ok, #document{value = #session{identity = #identity{user_id = ?USER_ID}}}},
            rpc:call(Worker1, session, get, [SessionId])
        ),
        ?assertMatch(
            {ok, #document{value = #identity{user_id = ?USER_ID}}},
            rpc:call(Worker1, identity, get, [#auth{macaroon = ?MACAROON}])
        ),
        unmock_gr_certificates(Config),
        ok = ssl2:close(Sock)
    catch T:M ->
        ct:print("ZOMG: ~p", [{T, M, erlang:get_stacktrace()}])
    end.
%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    Config.

end_per_testcase(_, _Config) ->
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, and custom sessionId
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node(), TokenVal :: binary(), SessionId :: session:id()) ->
    {ok, Sock :: term()}.
connect_via_token(Node, TokenVal, SessionId) ->
    % given
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, protocol_handler_port),
    TokenAuthMessage = #'ClientMessage'{message_body =
    {handshake_request, #'HandshakeRequest'{
        session_id = SessionId,
        token = #'Token'{value = TokenVal}
    }}},
    TokenAuthMessageRaw = messages:encode_msg(TokenAuthMessage),

    % when
    {ok, Sock} = ssl2:connect(utils:get_host(Node), Port, [{packet, 4}]),
    ct:print("~s", [TokenAuthMessageRaw]),
    timer:sleep(10000),
    ok = ssl2:send(Sock, TokenAuthMessageRaw),
    {ok, Data} = ssl2:recv(Sock, 5000, 5000),
    ct:print("Data2: ~p", [Data]),

    % then
    HandshakeResponse = receive_server_message(Sock),
    ?assertMatch(#'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{}}}, HandshakeResponse),
    {ok, Sock}.

receive_server_message(Sock) ->
    receive_server_message(Sock, [message_stream_reset]).

receive_server_message(Sock, IgnoredMsgList) ->
    {ok, Data} = ssl2:recv(Sock, 5000, 5000),
    ct:print("Data: ~p", [Data]),
    % ignore listed messages
    Msg = messages:decode_msg(Data, 'ServerMessage'),
    MsgType = element(1, Msg#'ServerMessage'.message_body),
    case lists:member(MsgType, IgnoredMsgList) of
        true ->
            receive_server_message(IgnoredMsgList);
        false ->
            Msg
    end.

mock_gr_certificates(Config) ->
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    Url = rpc:call(Worker1, gr_plugin, get_gr_url, []),
    {ok, KeyPath} = file:read_file(?TEST_FILE(Config, "grpkey.pem")),
    {ok, CertPath} = file:read_file(?TEST_FILE(Config, "grpcert.pem")),
    SSLOpts = {ssl_options, [{keyfile, KeyPath}, {certfile, CertPath}]},

    test_utils:mock_new(Workers, gr_endpoint),
    test_utils:mock_expect(Workers, gr_endpoint, auth_request,
        fun
            (provider, URN, Method, Headers, Body, Options) ->
                http_client:request(Method, Url ++ URN,
                    [{<<"content-type">>, <<"application/json">>} | Headers],
                    Body, [SSLOpts, insecure | Options]);
            (client, URN, Method, Headers, Body, Options) ->
                http_client:request(Method, Url ++ URN,
                    [{<<"content-type">>, <<"application/json">>} | Headers],
                    Body, [SSLOpts, insecure | Options]);
            ({_, undefined}, URN, Method, Headers, Body, Options) ->
                http_client:request(Method, Url ++ URN,
                    [{<<"content-type">>, <<"application/json">>} | Headers],
                    Body, [SSLOpts, insecure | Options]);
            % @todo for now, in rest we only use the root macaroon
            ({_, {Macaroon, []}}, URN, Method, Headers, Body, Options) ->
                AuthHdr =
                    http_client:request(Method, Url ++ URN, [
                        {<<"content-type">>, <<"application/json">>},
                        {<<"macaroon">>, Macaroon} | Headers
                    ], Body, [SSLOpts, insecure | Options])
        end
    ).

unmock_gr_certificates(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, gr_endpoint),
    test_utils:mock_unload(Workers, gr_endpoint).

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
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([token_authentication/1]).

-define(MACAROON, macaroon:create("a", "b", "c")).
-define(MACAROON_TOKEN, element(2, macaroon:serialize(?MACAROON))).
-define(USER_ID, <<"test_id">>).
-define(USER_NAME, <<"test_name">>).

all() -> ?ALL([token_authentication]).

%%%===================================================================
%%% Test functions
%%%===================================================================

token_authentication(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    mock_oz_certificates(Config),
    SessionId = <<"SessionId">>,

    % when
    {ok, Sock} = connect_via_token(Worker1, ?MACAROON_TOKEN, SessionId),

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
    test_utils:mock_validate_and_unload(Workers, oz_endpoint),
    ok = ssl2:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    mock_oz_spaces(Config),
    Config.

end_per_testcase(_, Config) ->
    unmock_oz_spaces(Config),
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
    {ok, Sock} = ssl2:connect(utils:get_host(Node), Port, [{packet, 4}, {active, true}]),
    ok = ssl2:send(Sock, TokenAuthMessageRaw),

    % then
    HandshakeResponse = receive_server_message(),
    ?assertMatch(#'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{}}}, HandshakeResponse),
    {ok, Sock}.

receive_server_message() ->
    receive_server_message([message_stream_reset]).

receive_server_message(IgnoredMsgList) ->
    receive
        {_, _, Data} ->
            % ignore listed messages
            Msg = messages:decode_msg(Data, 'ServerMessage'),
            MsgType = element(1, Msg#'ServerMessage'.message_body),
            case lists:member(MsgType, IgnoredMsgList) of
                true ->
                    receive_server_message(IgnoredMsgList);
                false -> Msg
            end
    after timer:seconds(5) ->
        {error, timeout}
    end.

mock_oz_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, oz_spaces, get_details,
        fun(_, _) -> {ok, #space_details{}} end),
    test_utils:mock_expect(Workers, oz_spaces, get_users,
        fun(_, _) -> {ok, []} end),
    test_utils:mock_expect(Workers, oz_spaces, get_groups,
        fun(_, _) -> {ok, []} end).

unmock_oz_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, oz_spaces).

mock_oz_certificates(Config) ->
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    OZUrl = rpc:call(Worker1, oz_plugin, get_oz_url, []),
    OZRestPort = rpc:call(Worker1, oz_plugin, get_oz_rest_port, []),
    OZRestApiPrefix = rpc:call(Worker1, oz_plugin, get_oz_rest_api_prefix, []),
    OzRestApiUrl = str_utils:format("~s:~B~s", [
        OZUrl,
        OZRestPort,
        OZRestApiPrefix
    ]),

    % save key and cert files on the workers
    % read the files
    {ok, KeyBin} = file:read_file(?TEST_FILE(Config, "grpkey.pem")),
    {ok, CertBin} = file:read_file(?TEST_FILE(Config, "grpcert.pem")),
    % choose paths for the files
    KeyPath = "/tmp/user_auth_test_key.pem",
    CertPath = "/tmp/user_auth_test_cert.pem",
    % and save them on workers
    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, file, write_file, [KeyPath, KeyBin]),
            ok = rpc:call(Node, file, write_file, [CertPath, CertBin])
        end, Workers),
    % Use the cert paths on workers to mock oz_endpoint
    SSLOpts = {ssl_options, [{keyfile, KeyPath}, {certfile, CertPath}]},

    test_utils:mock_new(Workers, oz_endpoint),
    test_utils:mock_expect(Workers, oz_endpoint, auth_request,
        fun
            (provider, URN, Method, Headers, Body, Options) ->
                http_client:request(Method, OzRestApiUrl ++ URN,
                    [{<<"content-type">>, <<"application/json">>} | Headers],
                    Body, [SSLOpts, insecure | Options]);
            (client, URN, Method, Headers, Body, Options) ->
                http_client:request(Method, OzRestApiUrl ++ URN,
                    [{<<"content-type">>, <<"application/json">>} | Headers],
                    Body, [SSLOpts, insecure | Options]);
            ({_, undefined}, URN, Method, Headers, Body, Options) ->
                http_client:request(Method, OzRestApiUrl ++ URN,
                    [{<<"content-type">>, <<"application/json">>} | Headers],
                    Body, [SSLOpts, insecure | Options]);
            % @todo for now, in rest we only use the root macaroon
            ({_, {Macaroon, []}}, URN, Method, Headers, Body, Options) ->
                {ok, SrlzdMacaroon} = macaroon:serialize(Macaroon),
                http_client:request(Method, OzRestApiUrl ++ URN, [
                    {<<"content-type">>, <<"application/json">>},
                    {<<"macaroon">>, SrlzdMacaroon} | Headers
                ], Body, [SSLOpts, insecure | Options])
        end
    ).

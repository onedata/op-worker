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
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
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
    ok = ssl:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ssl:start(),
    Config.

end_per_testcase(_, _Config) ->
    ssl:stop().

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
    {ok, Port} = rpc:call(Node, application, get_env, [?APP_NAME, protocol_handler_port]),
    TokenAuthMessage = #'ClientMessage'{message_body =
    {handshake_request, #'HandshakeRequest'{
        session_id = SessionId,
        token = #'Token'{value = TokenVal}
    }}},
    TokenAuthMessageRaw = messages:encode_msg(TokenAuthMessage),

    % when
    {ok, Sock} = ssl:connect(utils:get_host_as_atom(Node), Port, [binary, {packet, 4}, {active, true}]),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

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

mock_gr_certificates(Config) ->
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    Url = rpc:call(Worker1, gr_plugin, get_gr_url, []),
    {ok, Key} = file:read_file(?TEST_FILE(Config, "grpkey.pem")),
    {ok, Cert} = file:read_file(?TEST_FILE(Config, "grpcert.pem")),
    {ok, CACert} = file:read_file(?TEST_FILE(Config, "grpCA.pem")),
    [{KeyType, KeyEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [Key]),
    [{_, CertEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [Cert]),
    [{_, CACertEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [CACert]),
    SSLOptions = {ssl_options, [{cacerts, [CACertEncoded]}, {key, {KeyType, KeyEncoded}}, {cert, CertEncoded}]},

    test_utils:mock_new(Workers, [gr_endpoint]),
    test_utils:mock_expect(Workers, gr_endpoint, auth_request,
        fun
            (provider, URN, Method, Headers, Body, Options) ->
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"} | Headers], Method, Body, [SSLOptions | Options]);
            (client, URN, Method, Headers, Body, Options) ->
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"} | Headers], Method, Body, [SSLOptions | Options]);
            ({_, undefined}, URN, Method, Headers, Body, Options) ->
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"} | Headers], Method, Body, [SSLOptions | Options]);
            % @todo for now, in rest we only use the root macaroon
            ({_, {Macaroon, []}}, URN, Method, Headers, Body, Options) ->
                AuthorizationHeader = {"macaroon", binary_to_list(Macaroon)},
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"}, AuthorizationHeader | Headers], Method, Body, [SSLOptions | Options])
        end
    ).

unmock_gr_certificates(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, [gr_endpoint]),
    test_utils:mock_unload(Workers, [gr_endpoint]).

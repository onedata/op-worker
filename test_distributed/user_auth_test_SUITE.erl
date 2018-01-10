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
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([token_authentication/1]).

-define(MACAROON, <<"DUMMY-MACAROON">>).
-define(USER_ID, <<"test_id">>).
-define(USER_NAME, <<"test_name">>).

all() -> ?ALL([token_authentication]).

%%%===================================================================
%%% Test functions
%%%===================================================================

token_authentication(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = <<"SessionId">>,

    % when
    {ok, Sock} = connect_via_token(Worker1, ?MACAROON, SessionId),

    % then
    ?assertMatch(
        {ok, #document{value = #session{identity = #user_identity{user_id = ?USER_ID}}}},
        rpc:call(Worker1, session, get, [SessionId])
    ),
    ?assertMatch(
        {ok, #document{value = #user_identity{user_id = ?USER_ID}}},
        rpc:call(Worker1, user_identity, get, [#token_auth{token = ?MACAROON}])
    ),
    ok = ssl:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(_Case, Config) ->
    ssl:start(),
    mock_space_logic(Config),
    mock_user_logic(Config),
    Config.

end_per_testcase(_Case, Config) ->
    unmock_space_logic(Config),
    unmock_user_logic(Config),
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
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, protocol_handler_port),
    TokenAuthMessage = #'ClientMessage'{message_body =
    {client_handshake_request, #'ClientHandshakeRequest'{
        session_id = SessionId,
        token = #'Token'{value = TokenVal}
    }}},
    TokenAuthMessageRaw = messages:encode_msg(TokenAuthMessage),

    % when
    {ok, Sock} = ssl:connect(utils:get_host(Node), Port, [binary, {packet, 4}, {active, true}]),
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

mock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, space_logic, []),
    test_utils:mock_expect(Workers, space_logic, get,
        fun(_, _) ->
            {ok, #document{value = #od_space{}}}
        end).

unmock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, space_logic).

mock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, user_logic, []),
    test_utils:mock_expect(Workers, user_logic, get_by_auth,
        fun(#token_auth{token = ?MACAROON}) ->
            {ok, #document{key = ?USER_ID, value = #od_user{name = ?USER_NAME}}}
        end).

unmock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, user_logic).

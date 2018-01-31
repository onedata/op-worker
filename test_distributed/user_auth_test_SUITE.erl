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
    [Worker1 | _] = ?config(op_worker_nodes, Config),
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
    TokenAuthMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{session_id = SessionId, token = #'Token'{
            value = TokenVal
        }}
    }},
    TokenAuthMessageRaw = messages:encode_msg(TokenAuthMessage),
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, gui_https_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

    % then
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = 'OK'
    }}} = ?assertMatch(#'ServerMessage'{message_body = {handshake_response, _}},
        receive_server_message()
    ),
    {ok, Sock}.


connect_and_upgrade_proto(Hostname, Port) ->
    {ok, Sock} = (catch ssl:connect(Hostname, Port, [binary,
        {active, once}, {reuse_sessions, false}
    ], timer:minutes(1))),
    ssl:send(Sock, connection:protocol_upgrade_request(list_to_binary(Hostname))),
    receive {ssl, Sock, Data} ->
        ?assert(connection:verify_protocol_upgrade_response(Data)),
        ssl:setopts(Sock, [{active, once}, {packet, 4}]),
        {ok, Sock}
    after timer:minutes(1) ->
        exit(timeout)
    end.


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

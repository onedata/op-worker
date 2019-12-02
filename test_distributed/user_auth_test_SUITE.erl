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
-include("proto/common/handshake_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    token_authentication/1
]).

all() -> ?ALL([
    token_authentication
]).


-define(USER_ID, <<"test_id">>).
-define(USER_FULL_NAME, <<"test_name">>).


%%%===================================================================
%%% Test functions
%%%===================================================================


token_authentication(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"nonce">>,
    SerializedToken = initializer:create_token(?USER_ID),

    TokenAuth = #token_auth{
        token = SerializedToken,
        peer_ip = initializer:local_ip_v4(),
        interface = oneclient,
        data_access_caveats_policy = allow_data_access_caveats
    },

    % when
    {ok, {Sock, SessionId}} = fuse_test_utils:connect_via_token(Worker1, [], Nonce, TokenAuth),

    % then
    ?assertMatch(
        {ok, #document{value = #session{identity = #user_identity{user_id = ?USER_ID}}}},
        rpc:call(Worker1, session, get, [SessionId])
    ),
    ?assertMatch(
        {ok, ?USER(?USER_ID), undefined},
        rpc:call(Worker1, auth_manager, verify, [TokenAuth])
    ),
    ok = ssl:close(Sock).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_testcase(_Case, Config) ->
    ssl:start(),
    mock_provider_logic(Config),
    mock_space_logic(Config),
    mock_user_logic(Config),
    Config.


end_per_testcase(_Case, Config) ->
    unmock_provider_logic(Config),
    unmock_space_logic(Config),
    unmock_user_logic(Config),
    ssl:stop().


%%%===================================================================
%%% Internal functions
%%%===================================================================


mock_provider_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, provider_logic, []),
    test_utils:mock_expect(Workers, provider_logic, has_eff_user,
        fun(UserId) ->
            UserId =:= ?USER_ID
        end).


unmock_provider_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, provider_logic).


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
    test_utils:mock_expect(Workers, user_logic, get, fun
        (#token_auth{token = SerializedToken}, ?USER_ID) ->
            case tokens:deserialize(SerializedToken) of
                {ok, #token{subject = ?SUB(user, ?USER_ID)}} ->
                    {ok, #document{key = ?USER_ID, value = #od_user{}}};
                {error, _} = Error ->
                    Error
            end;
        (_, _) ->
            {error, not_found}
    end),
    test_utils:mock_expect(Workers, token_logic, verify_access_token, fun
        (#token_auth{token = SerializedToken}) ->
            case tokens:deserialize(SerializedToken) of
                {ok, #token{subject = ?SUB(user, ?USER_ID)}} ->
                    {ok, ?USER(?USER_ID), undefined};
                {error, _} = Error ->
                    Error
            end
    end).


unmock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, user_logic).

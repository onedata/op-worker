%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Basic rest_handler operations tests.
%%% @end
%%%-------------------------------------------------------------------
-module(rest_handler_test_SUITE).
-author("Tomasz Lichon").

-include("http/rest.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    token_auth_test/1,
    session_cookie_auth_test/1,
    internal_error_when_handler_crashes_test/1,
    custom_error_when_handler_throws_error_test/1
]).

all() -> ?ALL([
    token_auth_test,
    session_cookie_auth_test,
    internal_error_when_handler_crashes_test,
    custom_error_when_handler_throws_error_test
]).


-define(DEFAULT_TEMP_CAVEAT_TTL, 360000).


%%%===================================================================
%%% Tests
%%%===================================================================


token_auth_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    R = fun(Headers) -> rest_test_utils:request(Node, <<"spaces">>, get, Headers, <<>>) end,

    ?assertMatch({ok, ?HTTP_401_UNAUTHORIZED, _, _}, R(#{?HDR_X_AUTH_TOKEN => <<"invalid">>})),

    User1AccessToken = oct_background:get_user_access_token(user1),

    ?assertMatch({ok, ?HTTP_200_OK, _, _}, R(#{?HDR_X_AUTH_TOKEN => User1AccessToken})),
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, R(#{?HDR_AUTHORIZATION => <<"Bearer ", User1AccessToken/binary>>})),
    %% @todo VFS-5554 Deprecated, included for backward compatibility
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, R(#{?HDR_MACAROON => User1AccessToken})),

    User2Id = oct_background:get_user_id(user2),
    User2IdentityToken = create_temp_identity_token(User2Id),

    User3Id = oct_background:get_user_id(user3),
    User3IdentityToken = create_temp_identity_token(User3Id),

    User1AccessTokenWithConsumerCaveats = tokens:confine(User1AccessToken, [
        #cv_consumer{whitelist = [?SUB(user, User2Id)]}
    ]),

    ?assertMatch(
        {ok, ?HTTP_401_UNAUTHORIZED, _, _},
        R(#{?HDR_X_AUTH_TOKEN => User1AccessTokenWithConsumerCaveats})
    ),
    ?assertMatch(
        {ok, ?HTTP_401_UNAUTHORIZED, _, _},
        R(#{
            ?HDR_X_AUTH_TOKEN => User1AccessTokenWithConsumerCaveats,
            ?HDR_X_ONEDATA_CONSUMER_TOKEN => User3IdentityToken
        })
    ),
    ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        R(#{
            ?HDR_X_AUTH_TOKEN => User1AccessTokenWithConsumerCaveats,
            ?HDR_X_ONEDATA_CONSUMER_TOKEN => User2IdentityToken
        })
    ).


session_cookie_auth_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    RequestSpaces = fun(Headers) ->
        rest_test_utils:request(Node, <<"spaces">>, get, Headers, <<>>)
    end,
    RequestAtmStoreContentDump = fun(Headers) ->
        % Requesting nonexistent store should pass authentication and fails only on actual request processing
        rest_test_utils:request(Node, <<"automation/execution/stores/dummy_id/content_dump">>, get, Headers, <<>>)
    end,
    ?assertMatch({ok, ?HTTP_401_UNAUTHORIZED, _, _}, RequestSpaces([])),
    ?assertMatch({ok, ?HTTP_401_UNAUTHORIZED, _, _}, RequestAtmStoreContentDump([])),
    ?assertMatch({ok, ?HTTP_401_UNAUTHORIZED, _, _}, RequestSpaces([{<<"cookie">>, <<"SID=dummy_id">>}])),
    ?assertMatch({ok, ?HTTP_401_UNAUTHORIZED, _, _}, RequestAtmStoreContentDump([{<<"cookie">>, <<"SID=dummy_id">>}])),

    {ok, _, #{<<"set-cookie">> := SetCookieHeaderValue}, _} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, #{<<"set-cookie">> := _}, <<>>},
        http_client:request(
            post,
            <<"https://", (opw_test_rpc:get_provider_domain(Node))/binary, "/gui/acquire_session">>,
            #{?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(user1)},
            <<>>,
            [{recv_timeout, 60000} | rest_test_utils:cacerts_opts(Node)]
        )
    ),
    [SessionCookieValue | _] = string:split(SetCookieHeaderValue, ";", leading),
    % Valid session cookie is accepted only for specific endpoint
    ?assertMatch({ok, ?HTTP_401_UNAUTHORIZED, _, _}, RequestSpaces([{<<"cookie">>, SessionCookieValue}])),
    ?assertMatch({ok, ?HTTP_404_NOT_FOUND, _, _}, RequestAtmStoreContentDump([{<<"cookie">>, SessionCookieValue}])).


internal_error_when_handler_crashes_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    test_utils:mock_expect(Node, space_oz_middleware_handler, get, fun(_, _) -> throw(crash) end),

    AuthHeaders = #{?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(user1)},

    ?assertMatch(
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _, _},
        rest_test_utils:request(Node, <<"spaces">>, get, AuthHeaders, <<>>)
    ).


custom_error_when_handler_throws_error_test(_Config) ->
    Error = ?ERROR_BAD_VALUE_JSON(<<"dummy">>),

    Node = oct_background:get_random_provider_node(krakow),
    test_utils:mock_expect(Node, space_oz_middleware_handler, get, fun(_, _) -> throw(Error) end),

    AuthHeaders = #{?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(user1)},

    ExpRestError = rest_test_utils:get_rest_error(Error),
    {ok, Status, _, BodyEncoded} = rest_test_utils:request(Node, <<"spaces">>, get, AuthHeaders, <<>>),
    ?assertMatch(ExpRestError, {Status, json_utils:decode(BodyEncoded)}).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes_test;
    Case =:= custom_error_when_handler_throws_error_test
->
    Nodes = oct_background:get_provider_nodes(krakow),
    test_utils:mock_new(Nodes, space_oz_middleware_handler),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes_test;
    Case =:= custom_error_when_handler_throws_error_test
->
    Nodes = oct_background:get_provider_nodes(krakow),
    test_utils:mock_unload(Nodes, space_oz_middleware_handler),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, _Config) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
create_temp_identity_token(UserId) ->
    Now = ozw_test_rpc:timestamp_seconds(?RAND_ELEMENT(oct_background:get_zone_nodes())),

    TempToken = ozw_test_rpc:create_user_temporary_token(?USER(UserId), UserId, #{
        <<"type">> => ?IDENTITY_TOKEN,
        <<"caveats">> => [#cv_time{valid_until = Now + ?DEFAULT_TEMP_CAVEAT_TTL}]
    }),
    {ok, SerializedToken} = tokens:serialize(TempToken),
    SerializedToken.

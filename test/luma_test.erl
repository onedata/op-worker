%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of luma: which IdP a user's OAuth2 access token is acquired from.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/storage/luma/luma.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(USER_ID, <<"userId">>).
-define(SESSION_ID, <<"sessionId">>).
-define(USER_IDP, <<"userIdP">>).
-define(STORAGE_IDP, <<"storageIdP">>).
-define(ONEZONE_IDPS, [<<"zoneIdP1">>, <<"zoneIdP2">>]).


%%%===================================================================
%%% Tests
%%%===================================================================


user_idp_wins_over_storage_idp_test_() ->
    {setup, mock_offline_access_idps(?ONEZONE_IDPS), fun unmock/1, fun() ->
        ?assertMatch(
            {ok, #{<<"accessToken">> := _}},
            add_oauth2_token(user_credentials(?USER_IDP), helper_spec(?STORAGE_IDP))
        ),
        ?assertEqual(?USER_IDP, acquired_from_idp())
    end}.


%% The credentials that a local or external feed maps a user to need not name
%% an IdP.
storage_idp_applies_when_user_credentials_name_none_test_() ->
    {setup, mock_offline_access_idps(?ONEZONE_IDPS), fun unmock/1, fun() ->
        ?assertMatch(
            {ok, #{<<"accessToken">> := _}},
            add_oauth2_token(user_credentials(undefined), helper_spec(?STORAGE_IDP))
        ),
        ?assertEqual(?STORAGE_IDP, acquired_from_idp())
    end}.


only_onezone_idp_applies_when_none_is_named_test_() ->
    {setup, mock_offline_access_idps([<<"zoneIdP">>]), fun unmock/1, fun() ->
        ?assertMatch(
            {ok, #{<<"accessToken">> := _}},
            add_oauth2_token(user_credentials(undefined), helper_spec(undefined))
        ),
        ?assertEqual(<<"zoneIdP">>, acquired_from_idp())
    end}.


several_onezone_idps_are_ambiguous_when_none_is_named_test_() ->
    {setup, mock_offline_access_idps(?ONEZONE_IDPS), fun unmock/1, fun() ->
        ?assertEqual(
            {error, ambiguous_identity_provider},
            add_oauth2_token(user_credentials(undefined), helper_spec(undefined))
        ),
        ?assertEqual(0, meck:num_calls(idp_access_token, acquire, '_'))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
add_oauth2_token(UserCredentials, HelperSpec) ->
    luma:add_helper_specific_fields(?USER_ID, ?SESSION_ID, UserCredentials, HelperSpec, ?LOCAL_FEED).


%% @private
acquired_from_idp() ->
    meck:capture(first, idp_access_token, acquire, '_', 3).


%% @private
user_credentials(OAuth2IdP) ->
    with_oauth2_idp(#{
        <<"credentialsType">> => <<"oauth2">>,
        <<"credentials">> => <<"userCredentials">>
    }, OAuth2IdP).


%% @private
helper_spec(OAuth2IdP) ->
    #helper_spec{
        name = ?WEBDAV_HELPER_NAME,
        configuration = #{
            <<"endpoint">> => <<"https://webdav.example.com">>,
            <<"storagePathType">> => ?FLAT_STORAGE_PATH
        },
        credentials = with_oauth2_idp(#{
            <<"credentialsType">> => <<"oauth2">>,
            <<"credentials">> => <<"adminCredentials">>,
            <<"onedataAccessToken">> => <<"onedataAccessToken">>,
            <<"adminId">> => <<"adminId">>
        }, OAuth2IdP)
    }.


%% @private
with_oauth2_idp(Credentials, undefined) ->
    Credentials;
with_oauth2_idp(Credentials, OAuth2IdP) ->
    Credentials#{<<"oauth2IdP">> => OAuth2IdP}.


%% @private
mock_offline_access_idps(OAuth2IdPs) ->
    fun() ->
        meck:new(onedata_logger, [passthrough, no_link]),
        meck:expect(onedata_logger, should_log, fun(_Loglevel) -> true end),
        meck:expect(onedata_logger, log, fun(_Loglevel, _Metadata, _FormattedLog) -> ok end),
        meck:new([provider_logic, idp_access_token], [no_link]),
        meck:expect(provider_logic, zone_get_offline_access_idps, fun() -> {ok, OAuth2IdPs} end),
        meck:expect(idp_access_token, acquire, fun(_UserId, _Client, _OAuth2IdP) ->
            {ok, {<<"idpAccessToken">>, 3600}}
        end)
    end.


%% @private
unmock(_) ->
    meck:unload().

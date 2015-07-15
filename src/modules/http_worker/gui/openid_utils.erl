%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This library is used to authenticate users that have been redirected
%% from global registry.
%% @end
%% ===================================================================
-module(openid_utils).

-include("modules/http_worker/http_common.hrl").
-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([validate_login/0]).

%% validate_login/0
%% ====================================================================
%% @doc
%% Authenticates a user via Global Registry.
%% Should be called from n2o page rendering context.
%% TODO For now, this funciton just returns the information obtained from GR.
%% @end
-spec validate_login() -> ok | {error, PredefinedErrorID :: atom()}.
%% ====================================================================
validate_login() ->
    try
        AuthorizationCode = gui_ctx:url_param(<<"code">>),
        {ok, #token_response{
            access_token = AccessToken,
            refresh_token = RefreshToken,
            expires_in = ExpiresIn,
            id_token = #id_token{
                sub = GRUID,
                name = Name,
                logins = Logins,
                emails = EmailList}
        }} = gr_openid:get_token_response(
            provider,
            [{<<"code">>, AuthorizationCode}, {<<"grant_type">>, <<"authorization_code">>}]
        ),
        {ok, _} = onedata_user:fetch(#token{value = AccessToken}),
        [
            {access_token, AccessToken},
            {refresh_token, RefreshToken},
            {expires_in, ExpiresIn},
            {sub, GRUID},
            {name, Name},
            {logins, Logins},
            {emails, EmailList}
        ]
    catch
        Type:Message ->
            ?error_stacktrace("Cannot validate login ~p:~p", [Type, Message]),
            {error, cannot_validate_login}
    end.



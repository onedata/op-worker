%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Dynamic page visited by GUI to acquire a session via an HTTP cookie.
%%% The cookie can be presented as authorization on several Oneprovider
%%% endpoints (see http_auth.erl).
%%% @end
%%%-------------------------------------------------------------------
-module(page_gui_acquire_session).
-author("Bartosz Walkowicz").

-behaviour(dynamic_page_behaviour).

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([lookup_session_cookie/1]).

%% dynamic_page_behaviour callbacks
-export([handle/2]).


-define(SESSION_COOKIE_KEY, <<"SID">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec lookup_session_cookie(cowboy_req:req()) -> undefined | session:id().
lookup_session_cookie(Req) ->
    proplists:get_value(?SESSION_COOKIE_KEY, cowboy_req:parse_cookies(Req), undefined).


%%%===================================================================
%%% dynamic_page_behaviour callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"OPTIONS">>, Req) ->
    gui_cors:options_response(
        oneprovider:get_oz_url(),
        [<<"POST">>],
        [?HDR_X_AUTH_TOKEN, ?HDR_CONTENT_TYPE],
        Req
    );
handle(<<"POST">>, Req1) ->
    OzUrl = oneprovider:get_oz_url(),
    Req2 = gui_cors:allow_origin(OzUrl, Req1),
    Req3 = gui_cors:allow_frame_origin(OzUrl, Req2),

    AuthCtx = #http_auth_ctx{
        interface = rest,
        data_access_caveats_policy = allow_data_access_caveats
    },
    case http_auth:authenticate(Req1, AuthCtx) of
        {ok, ?USER(_Id, SessionId)} ->
            {ok, TokenCredentials} = session:get_credentials(SessionId),

            CookieOpts = maps_utils:remove_undefined(#{
                path => <<"/">>,
                max_age => auth_manager:infer_access_token_ttl(TokenCredentials),
                secure => true,
                http_only => true
            }),
            Req4 = cowboy_req:set_resp_cookie(?SESSION_COOKIE_KEY, SessionId, Req3, CookieOpts),
            cowboy_req:reply(?HTTP_204_NO_CONTENT, Req4);
        {ok, ?GUEST} ->
            http_req:send_error(?ERROR_UNAUTHORIZED, Req3);
        {error, _} = Error ->
            http_req:send_error(Error, Req3)
    end.

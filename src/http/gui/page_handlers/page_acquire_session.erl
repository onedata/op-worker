%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of session cookie.
%%% @end
%%%-------------------------------------------------------------------
-module(page_acquire_session).
-author("Bartosz Walkowicz").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([get_session_cookie/1]).

%% dynamic_page_behaviour callbacks
-export([handle/2]).


-define(SESSION_COOKIE_KEY, <<"SID">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_session_cookie(cowboy_req:req()) -> undefined | session:id().
get_session_cookie(Req) ->
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

    AuthCtx = #{
        interface => rest,
        data_access_caveats_policy => allow_data_access_caveats
    },
    case http_auth:authenticate(Req1, AuthCtx) of
        {ok, ?USER(_Id, SessionId)} ->
            Req4 = cowboy_req:set_resp_cookie(?SESSION_COOKIE_KEY, SessionId, Req3, #{
                path => <<"/">>,
%%                max_age => TTL,  todo no ttl??
                secure => true,
                http_only => true
            }),
            cowboy_req:reply(?HTTP_204_NO_CONTENT, Req4);
        {ok, _} ->
            http_req:send_error(?ERROR_UNAUTHORIZED, Req3);
        {error, _} = Error ->
            http_req:send_error(Error, Req3)
    end.
